#!/usr/bin/env python3
"""Build and install Wayfinder without starting or authorizing a map."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def run(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def atomic_copy(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as temporary:
        temporary_path = Path(temporary.name)
    try:
        shutil.copy2(source, temporary_path)
        os.replace(temporary_path, destination)
    finally:
        temporary_path.unlink(missing_ok=True)


def unit_quote(value, expand_environment=True):
    # systemd specifiers and ExecStart environment expansion are independent.
    if '\n' in str(value) or '\r' in str(value):
        raise ValueError('Installation paths cannot contain newlines.')
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"').replace('%', '%%').replace('$', '$$' if expand_environment else '$') + '"'


def main():
    if sys.platform != 'linux':
        raise RuntimeError('Linux is required.')
    source = Path(__file__).resolve().parent.parent
    for tool in ('cargo', 'herdr', 'systemctl'):
        if shutil.which(tool) is None:
            raise RuntimeError(f'Missing prerequisite: {tool}')
    if run('herdr', '--version', capture_output=True).stdout.strip() != 'herdr 0.9.3':
        raise RuntimeError('This release requires herdr 0.9.3; existing installation was not changed.')
    config = Path(os.environ.get('XDG_CONFIG_HOME', Path.home() / '.config')).resolve()
    state = Path(os.environ.get('XDG_STATE_HOME', Path.home() / '.local/state')).resolve() / 'wayfinder-herdr'
    install = Path(os.environ.get('WAYFINDER_INSTALL_DIR', Path.home() / '.local/lib/wayfinder-herdr')).resolve()
    # Refuse a downgrade before replacing any installed artifact.
    for path in sorted((state / 'maps').glob('*/state.json')):
        if json.loads(path.read_text()).get('format_version') != 1:
            raise RuntimeError(f'Unsupported state format: {path}. Use a compatible release; preserve this state.')
    active = run('systemctl', '--user', 'list-units', '--all', '--no-legend', '--plain',
                 'wayfinder-herdr@*.service', capture_output=True).stdout
    if any(line.split()[2] in ('active', 'activating', 'deactivating', 'reloading')
           for line in active.splitlines() if len(line.split()) >= 3):
        raise RuntimeError('Stop the active wayfinder-herdr@ instances before upgrading, then rerun this command. State will be preserved. Restart those instances after installation.')
    run('cargo', 'build', '--release', '--locked', cwd=source)
    # Respect Cargo target-dir overrides by asking Cargo where the artifact lives.
    metadata = json.loads(run('cargo', 'metadata', '--format-version', '1', '--no-deps', '--locked', cwd=source, capture_output=True).stdout)
    binary = Path(metadata['target_directory']) / 'release/wayfinder-herdr'
    atomic_copy(binary, install / 'bin/wayfinder-herdr')
    atomic_copy(source / 'herdr-plugin.toml', install / 'herdr-plugin.toml')
    unit = config / 'systemd/user/wayfinder-herdr@.service'
    unit.parent.mkdir(parents=True, exist_ok=True)
    text = '\n'.join([
        '[Unit]', 'Description=Wayfinder runtime for map %i',
        'StartLimitIntervalSec=60', 'StartLimitBurst=5', '',
        '[Service]', 'Type=simple',
        'Environment=' + unit_quote('XDG_CONFIG_HOME=' + str(config), False),
        'Environment=' + unit_quote('XDG_STATE_HOME=' + str(state.parent), False),
        'ExecStart=' + unit_quote(install / 'bin/wayfinder-herdr') + ' --state-dir ' + unit_quote(state) + ' serve --key %i',
        'Restart=on-failure', 'RestartSec=5', 'UMask=0077', '',
        '[Install]', 'WantedBy=default.target', '',
    ])
    with tempfile.NamedTemporaryFile(mode='w', dir=unit.parent, delete=False) as temporary:
        temporary.write(text)
        staged = Path(temporary.name)
    try:
        staged.chmod(0o644)
        os.replace(staged, unit)
    finally:
        staged.unlink(missing_ok=True)
    run('systemctl', '--user', 'daemon-reload')
    run('herdr', 'plugin', 'link', str(install), '--enabled')
    print(f'Installed {install}\nService template: {unit}\nNo map started. Attach a map and explicitly Start when ready.')


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f'Installation failed: {error}', file=sys.stderr)
        sys.exit(1)
