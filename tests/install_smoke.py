import os, pathlib, tempfile, subprocess, json, hashlib
source=pathlib.Path(__file__).resolve().parent.parent
root=pathlib.Path(tempfile.mkdtemp(prefix='wayfinder-install-'))
stubs=root/'tools';stubs.mkdir()
(stubs/'systemctl').write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$WAYFINDER_SYSTEMCTL_LOG"\nexit 0\n')
(stubs/'systemctl').chmod(0o755)
env={k:v for k,v in os.environ.items() if not k.startswith('HERDR_')}
env.update(HOME=str(root/'home'),XDG_CONFIG_HOME=str(root/'config'),XDG_STATE_HOME=str(root/'state'),HERDR_CONFIG_PATH=str(root/'config/herdr/config.toml'),WAYFINDER_INSTALL_DIR=str(root/'installed plugin'),WAYFINDER_SYSTEMCTL_LOG=str(root/'systemctl.log'),PATH=str(stubs)+':'+env['PATH'])
r=subprocess.run(['./scripts/install.sh'],cwd=source,env=env,capture_output=True,text=True)
print(r.stdout,r.stderr);assert r.returncode==0
binary=root/'installed plugin/bin/wayfinder-herdr'
assert binary.exists()
launcher=root/'installed plugin/bin/wayfinder'
assert launcher.exists() and os.access(launcher, os.X_OK)
public_command=root/'home/.local/bin/wayfinder'
assert public_command.is_symlink() and public_command.resolve()==launcher
assert subprocess.run([str(public_command),'--help'],capture_output=True,text=True).returncode==0
assert 'id = "feature-input"' in (root/'installed plugin/herdr-plugin.toml').read_text()
assert not (root/'state/wayfinder-herdr/maps').exists()
assert (root/'systemctl.log').read_text().splitlines()==['--user list-units --all --no-legend --plain wayfinder-herdr@*.service','--user daemon-reload']
unit=root/'config/systemd/user/wayfinder-herdr@.service'
v=subprocess.run(['systemd-analyze','--user','verify',str(unit)],capture_output=True,text=True)
print('systemd verification:',v.returncode,v.stdout,v.stderr);assert v.returncode==0
before=hashlib.sha256(binary.read_bytes()).hexdigest()
state=root/'state/wayfinder-herdr/maps/test/state.json';state.parent.mkdir(parents=True);state.write_text('{"format_version":999,"keep":"history"}')
r=subprocess.run(['./scripts/install.sh'],cwd=source,env=env,capture_output=True,text=True)
assert r.returncode==1 and 'Unsupported state format' in r.stderr
assert hashlib.sha256(binary.read_bytes()).hexdigest()==before
assert state.read_text()=='{"format_version":999,"keep":"history"}'
print('PASS: real build + isolated herdr registration; unit syntax; no map start; unsupported upgrade preserves binary and state')
print('ROOT',root)
