"""Command and PTY checks for the standalone feature input slice."""
import fcntl
import json
import os
import pathlib
import pty
import select
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import termios
import threading
import time
import unittest


SOURCE = pathlib.Path(__file__).resolve().parent.parent
COMMAND = SOURCE / "scripts/wayfinder.py"


class LaunchTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="wayfinder-launch-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.git("init", "-b", "develop", cwd=self.repo)
        self.git("-c", "user.name=Test", "-c", "user.email=test@example.test",
                 "commit", "--allow-empty", "-m", "initial", cwd=self.repo)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.socket_path = self.root / "herdr.sock"
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(self.socket_path))
        server.listen()
        server.settimeout(0.2)
        self.addCleanup(server.close)

        def serve_focus():
            while server.fileno() >= 0:
                try:
                    connection, _ = server.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                with connection:
                    request = json.loads(connection.recv(4096).decode())
                    connection.sendall((json.dumps({"id": request["id"], "result": {"ok": True}}) + "\n").encode())

        threading.Thread(target=serve_focus, daemon=True).start()
        fake = self.bin / "herdr"
        fake.write_text("""#!/usr/bin/env python3
import json, os, sys
with open(os.environ['HERDR_CALL_LOG'], 'a') as out:
    out.write(json.dumps(sys.argv[1:]) + '\\n')
if sys.argv[1:4] == ['pane','current','--current']:
    print(json.dumps({'result': {'pane': {'pane_id': 'w1:p1', 'workspace_id': 'w1', 'terminal_id': 'term-1'}}}))
elif sys.argv[1:4] == ['status','server','--json']:
    print(json.dumps({'running': True, 'compatible': True, 'endpoint_compatible': True,
                      'version': '0.9.3', 'protocol': 22, 'socket': os.environ['FAKE_HERDR_SOCKET']}))
elif sys.argv[1:3] == ['workspace','create']:
    print(json.dumps({'result': {'workspace': {'workspace_id': 'w2'},
                                 'root_pane': {'pane_id': 'w2:p1', 'terminal_id': 'term-2'}}}))
else:
    print(json.dumps({'result': {'ok': True}}))
""")
        fake.chmod(0o755)
        self.env = os.environ.copy()
        self.env.update(PATH=str(self.bin) + os.pathsep + self.env["PATH"],
                        XDG_STATE_HOME=str(self.root / "state"),
                        HERDR_CALL_LOG=str(self.root / "herdr.log"),
                        FAKE_HERDR_SOCKET=str(self.socket_path),
                        HERDR_ENV="1", HERDR_PANE_ID="w1:p1",
                        HERDR_WORKSPACE_ID="w1", HERDR_SOCKET_PATH=str(self.socket_path))

    def git(self, *args, cwd):
        result = subprocess.run(["git", *args], cwd=cwd, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def records(self):
        return [json.loads(path.read_text()) for path in (self.root / "state/wayfinder-herdr/launches").glob("*.json")]

    def test_checkout_binding_and_fresh_invocations(self):
        subdir = self.repo / "src/deep"
        subdir.mkdir(parents=True)
        linked = self.root / "linked"
        self.git("worktree", "add", "--detach", str(linked), cwd=self.repo)
        for cwd, expected in ((self.repo, self.repo), (subdir, self.repo), (linked, linked)):
            before = {record["launch_id"] for record in self.records()}
            result = subprocess.run([sys.executable, str(COMMAND)], cwd=cwd,
                                    env=self.env, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            records = [record for record in self.records() if record["launch_id"] not in before]
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["status"], "awaiting_input")
            self.assertEqual(records[0]["checkout"], str(expected))
            self.assertEqual(records[0]["source_terminal"], "term-1")
        ids = [record["launch_id"] for record in self.records()]
        self.assertEqual(len(ids), 3)
        self.assertEqual(len(set(ids)), 3)
        opens = [json.loads(line) for line in (self.root / "herdr.log").read_text().splitlines()
                 if 'feature-input' in line]
        self.assertEqual(len(opens), 3)
        for call in opens:
            self.assertIn("--cwd", call)
            self.assertIn("--env", call)
            self.assertIn("WAYFINDER_LAUNCH_ID=", call[call.index("--env") + 1])

    def test_outside_herdr_starts_client_and_coordinates_popup(self):
        env = {key: value for key, value in self.env.items() if not key.startswith("HERDR_")}
        env["HERDR_CALL_LOG"] = str(self.root / "herdr.log")
        result = subprocess.run([sys.executable, str(COMMAND)], cwd=self.repo,
                                env=env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            calls = (self.root / "herdr.log").read_text().splitlines()
            if any('feature-input' in call for call in calls):
                break
            time.sleep(0.05)
        self.assertTrue(any('feature-input' in call for call in calls))
        self.assertEqual(self.records()[0]["source_workspace"], "w2")
        self.assertEqual(self.records()[0]["source_pane"], "w2:p1")
        self.assertEqual(self.records()[0]["source_terminal"], "term-2")

    def run_popup(self, sequence):
        launch_id = os.urandom(16).hex()
        path = self.root / "state/wayfinder-herdr/launches" / (launch_id + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"launch_id": launch_id, "checkout": str(self.repo),
                                    "status": "awaiting_input"}))
        master, slave = pty.openpty()
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
        env = self.env.copy()
        env["TERM"] = "xterm"
        proc = subprocess.Popen([sys.executable, str(COMMAND), "popup", launch_id],
                                env=env, stdin=slave, stdout=slave, stderr=slave)
        os.close(slave)
        try:
            time.sleep(0.3)
            for part in sequence:
                os.write(master, part)
                time.sleep(0.1)
            deadline = time.monotonic() + 8
            while proc.poll() is None and time.monotonic() < deadline:
                if select.select([master], [], [], 0.2)[0]:
                    try:
                        os.read(master, 65536)
                    except OSError:
                        break
            self.assertEqual(proc.wait(timeout=2), 0)
            return json.loads(path.read_text())
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            os.close(master)

    def test_modified_enter_and_cancel(self):
        submitted = self.run_popup([b"First line", b"\x1b[13;2u", b"Second line", b"\r"])
        self.assertEqual(submitted["status"], "submitted")
        self.assertEqual(submitted["description"], "First line\nSecond line")
        recovered = subprocess.run(
            [sys.executable, str(COMMAND), "recover", submitted["launch_id"]],
            env=self.env, text=True, capture_output=True, check=True)
        self.assertIn("First line\nSecond line", recovered.stdout)
        self.assertIn("No feature effort was started", recovered.stdout)
        empty_cancelled = self.run_popup([b"\r", b"\x1b"])
        self.assertEqual(empty_cancelled["status"], "cancelled")
        self.assertNotIn("description", empty_cancelled)

    def test_host_close_before_enter_cancels_without_submission(self):
        launch_id = os.urandom(16).hex()
        path = self.root / "state/wayfinder-herdr/launches" / (launch_id + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"launch_id": launch_id, "checkout": str(self.repo),
                                    "status": "awaiting_input"}))
        master, slave = pty.openpty()
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
        env = self.env.copy()
        env["TERM"] = "xterm"
        proc = subprocess.Popen([sys.executable, str(COMMAND), "popup", launch_id],
                                env=env, stdin=slave, stdout=slave, stderr=slave)
        os.close(slave)
        try:
            time.sleep(0.3)
            proc.send_signal(signal.SIGTERM)
            self.assertEqual(proc.wait(timeout=2), 0)
            record = json.loads(path.read_text())
            self.assertEqual(record["status"], "cancelled")
            self.assertNotIn("description", record)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            os.close(master)


if __name__ == "__main__":
    unittest.main()
