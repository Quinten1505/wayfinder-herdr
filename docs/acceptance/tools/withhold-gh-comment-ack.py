#!/usr/bin/env python3
"""Inject one scoped lost-ack/read-after-write fault for issue16 acceptance.

This wrapper forwards every GitHub command to the real gh executable. When
explicitly armed for the private issue16 acceptance repository and one ticket,
it allows exactly one real comment creation, then reports a lost response and
hides that exact marker from one immediate comments read. It never creates or
edits a GitHub comment itself.
"""

import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path


EXPECTED_REPO = "Quinten1505/wayfinder-herdr-acceptance-20261001-issue16-7c4a"
REAL_GH = "/home/qbruinsma/.local/share/mise/installs/gh/latest/gh_2.101.0_linux_amd64/bin/gh"
repo = os.environ.get("WF16_FAULT_REPO")
issue = os.environ.get("WF16_FAULT_ISSUE")
state_path = Path(os.environ["WF16_FAULT_STATE"])
args = sys.argv[1:]

if repo != EXPECTED_REPO or not issue or not issue.isdecimal():
    sys.exit("fault wrapper requires the exact disposable repository and a numeric target issue")


def api_endpoint() -> str | None:
    for arg in args:
        if arg.startswith(f"repos/{repo}/issues/"):
            return arg.split("?", 1)[0]
    return None


def run_real(input_bytes: bytes | None):
    return subprocess.run(
        [REAL_GH, *args],
        input=input_bytes,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


input_bytes = sys.stdin.buffer.read() if "-" in args else None
endpoint = api_endpoint()
target_comments = f"repos/{repo}/issues/{issue}/comments"
is_post = "--method" in args and args[args.index("--method") + 1:args.index("--method") + 2] == ["POST"]

if endpoint == target_comments and is_post:
    state = json.loads(state_path.read_text())
    if state.get("armed") and not state.get("post_ack_withheld"):
        request = json.loads(input_bytes or b"{}")
        body = request.get("body", "")
        match = re.search(r"<!-- (wayfinder-operation:resolve:[^\s]+:resolution) -->", body)
        if not match:
            sys.exit("target POST did not contain the product-generated resolution marker")
        real = run_real(input_bytes)
        if real.returncode != 0:
            sys.stdout.buffer.write(real.stdout)
            sys.stderr.buffer.write(real.stderr)
            sys.exit(real.returncode)
        response = json.loads(real.stdout)
        state.update(
            {
                "armed": False,
                "post_ack_withheld": True,
                "remote_comment_created": True,
                "remote_comment_id": response.get("id"),
                "marker": match.group(1),
                "comment_body_sha256": hashlib.sha256(body.encode()).hexdigest(),
            }
        )
        state_path.write_text(json.dumps(state, indent=2) + "\n")
        sys.stderr.write("acceptance fault: real GitHub comment succeeded; POST acknowledgement deliberately withheld\n")
        sys.exit(75)
elif endpoint == target_comments and not is_post:
    state = json.loads(state_path.read_text())
    if state.get("post_ack_withheld") and not state.get("immediate_marker_read_withheld"):
        real = run_real(input_bytes)
        if real.returncode != 0:
            sys.stdout.buffer.write(real.stdout)
            sys.stderr.buffer.write(real.stderr)
            sys.exit(real.returncode)
        pages = json.loads(real.stdout)
        marker = state["marker"]
        hidden = 0
        for page in pages:
            for comment in page:
                if marker in comment.get("body", ""):
                    page.remove(comment)
                    hidden += 1
                    break
        if hidden != 1:
            sys.exit(f"expected exactly one real marker in immediate read, found {hidden}")
        state["immediate_marker_read_withheld"] = True
        state["hidden_read_comment_count"] = hidden
        state_path.write_text(json.dumps(state, indent=2) + "\n")
        sys.stdout.write(json.dumps(pages))
        sys.exit(0)

real = run_real(input_bytes)
sys.stdout.buffer.write(real.stdout)
sys.stderr.buffer.write(real.stderr)
sys.exit(real.returncode)
