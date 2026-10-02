#!/usr/bin/env python3
"""Start a feature from the invoking Git checkout and collect its description.

The popup receives an opaque launch ID. The checkout and source pane are bound
before Herdr is asked to open it; ambient popup focus never supplies identity.
"""

import argparse
import atexit
import curses
import json
import os
import re
import select
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def state_root():
    base = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state"))
    return base / "wayfinder-herdr" / "launches"


def record_path(launch_id):
    if not re.fullmatch(r"[0-9a-f]{32}", launch_id):
        raise ValueError("Invalid Wayfinder launch identity")
    return state_root() / (launch_id + ".json")


def save_record(path, record):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, staged = tempfile.mkstemp(prefix=".launch-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as output:
            os.fchmod(output.fileno(), 0o600)
            json.dump(record, output, ensure_ascii=False)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(staged, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(staged):
            os.unlink(staged)


def read_record(launch_id):
    path = record_path(launch_id)
    record = json.loads(path.read_text())
    if record.get("launch_id") != launch_id:
        raise ValueError("Wayfinder launch identity mismatch")
    return path, record


def herdr(*args):
    result = subprocess.run(["herdr", *args], text=True, capture_output=True, timeout=20)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "Herdr command failed")
    if not result.stdout.strip():
        return {}
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("Herdr returned an unexpected response") from error


def focus_source(socket_path, pane_id):
    request_id = "wayfinder-focus-" + secrets.token_hex(8)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(10)
        connection.connect(socket_path)
        connection.sendall((json.dumps({"id": request_id, "method": "pane.focus",
                                        "params": {"pane_id": pane_id}}) + "\n").encode())
        response = bytearray()
        while not response.endswith(b"\n"):
            chunk = connection.recv(4096)
            if not chunk or len(response) + len(chunk) > 1_048_576:
                raise RuntimeError("Herdr source focus returned an incomplete response")
            response.extend(chunk)
    result = json.loads(response)
    if result.get("id") != request_id or result.get("error"):
        raise RuntimeError("Herdr could not focus the verified source pane")


def checkout():
    def git(*args):
        result = subprocess.run(["git", *args], text=True, capture_output=True)
        if result.returncode:
            raise RuntimeError("Run wayfinder from a Git checkout: " + result.stderr.strip())
        return result.stdout.strip()

    root = Path(git("rev-parse", "--show-toplevel")).resolve(strict=True)
    common = Path(git("rev-parse", "--path-format=absolute", "--git-common-dir")).resolve(strict=True)
    return root, common


def launch():
    root, common = checkout()
    source = None
    if os.environ.get("HERDR_ENV") == "1":
        if not os.environ.get("HERDR_PANE_ID") or not os.environ.get("HERDR_SOCKET_PATH"):
            raise RuntimeError("Herdr pane identity is incomplete; reopen a shell pane and retry")
        source = herdr("pane", "current", "--current")
        source_pane = source.get("result", {}).get("pane", source.get("pane", {}))
        if not all(source_pane.get(key) for key in ("pane_id", "workspace_id", "terminal_id")):
            raise RuntimeError("Herdr could not verify this shell pane; retry from the intended shell")
    launch_id = secrets.token_hex(16)
    path = record_path(launch_id)
    while path.exists():
        launch_id = secrets.token_hex(16)
        path = record_path(launch_id)
    record = {
        "format_version": 1,
        "launch_id": launch_id,
        "checkout": str(root),
        "git_common_dir": str(common),
        "source_pane": source_pane["pane_id"] if source else None,
        "source_workspace": source_pane["workspace_id"] if source else None,
        "source_terminal": source_pane.get("terminal_id") if source else None,
        "source_socket": os.environ.get("HERDR_SOCKET_PATH") if source else None,
        "status": "awaiting_input",
    }
    save_record(path, record)
    if source:
        coordinate_or_record_error(launch_id)
        return
    # The normal Herdr entrypoint owns the terminal until the user detaches.
    # A separate coordinator opens the popup once its server is responsive.
    log = path.with_suffix(".log")
    with log.open("w") as output:
        subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "coordinate", launch_id],
                         stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                         start_new_session=True)
    os.execvp("herdr", ["herdr"])


def coordinate(launch_id):
    path, record = read_record(launch_id)
    root = Path(record["checkout"])
    if not root.is_dir() or not Path(record["git_common_dir"]).exists():
        raise RuntimeError("The invoking Git checkout disappeared; retry from its shell")
    deadline = time.monotonic() + 45
    while True:
        try:
            server = herdr("status", "server", "--json")
            if server.get("running") and server.get("compatible") and server.get("endpoint_compatible"):
                break
        except (RuntimeError, subprocess.TimeoutExpired):
            pass
        if time.monotonic() >= deadline:
            raise RuntimeError("Herdr did not become ready; retry from the invoking checkout")
        time.sleep(0.5)
    if server.get("version") != "0.9.3" or server.get("protocol") != 22:
        raise RuntimeError("Wayfinder requires Herdr 0.9.3, protocol 22")
    socket = server.get("socket")
    if not socket:
        raise RuntimeError("Herdr did not identify its session socket")
    if record["source_socket"] and record["source_socket"] != socket:
        raise RuntimeError("Herdr session socket changed; retry from the intended shell")
    record["source_socket"] = socket
    if record["source_pane"] is None:
        # A successful readiness read precedes exactly one workspace creation
        # request. An uncertain create is held for recovery, never retried here.
        workspace = herdr("workspace", "create", "--cwd", str(root),
                          "--label", root.name, "--focus")
        created = workspace.get("result", workspace)
        root_pane = created.get("root_pane", {})
        record["source_workspace"] = created.get("workspace", {}).get("workspace_id")
        record["source_pane"] = root_pane.get("pane_id")
        record["source_terminal"] = root_pane.get("terminal_id")
        if not record["source_workspace"] or not record["source_pane"] or not record["source_terminal"]:
            raise RuntimeError("Herdr created a workspace without verifiable source identities")
        save_record(path, record)
    else:
        save_record(path, record)
    focus_source(socket, record["source_pane"])
    # The popup's environment contains only the opaque identity. Its Herdr
    # context may reflect a different active pane and is never used as binding.
    herdr("plugin", "pane", "open", "--plugin", "wayfinder.herdr",
          "--entrypoint", "feature-input", "--cwd", str(root),
          "--env", "WAYFINDER_LAUNCH_ID=" + launch_id)


def coordinate_or_record_error(launch_id):
    try:
        coordinate(launch_id)
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        path, record = read_record(launch_id)
        record["status"] = "launch_error"
        record["error"] = str(error)
        save_record(path, record)
        try:
            herdr("notification", "show", "Wayfinder could not open feature input",
                  "--body", f"{error}. Retry `wayfinder` in the intended checkout.")
        except (OSError, RuntimeError, subprocess.TimeoutExpired):
            pass
        raise


def layout(value: str, width: int):
    rows = [""]
    positions = []
    row = col = 0
    for char in value:
        positions.append((row, col))
        if char == "\n":
            rows.append("")
            row += 1
            col = 0
        else:
            if col == width:
                rows.append("")
                row += 1
                col = 0
            rows[row] += char
            col += 1
    positions.append((row, col))
    return rows, positions


def put(window, y: int, x: int, value: str, style=0):
    height, width = window.getmaxyx()
    if y < 0 or y >= height or x < 0 or x >= width:
        return
    try:
        window.addstr(y, x, value[: max(0, width - x - 1)], style)
    except curses.error:
        pass


def move_vertical(cursor: int, positions, direction: int):
    current_row, current_col = positions[cursor]
    target_row = current_row + direction
    candidates = [index for index, (row, _) in enumerate(positions) if row == target_row]
    if not candidates:
        return cursor
    return min(candidates, key=lambda index: abs(positions[index][1] - current_col))


def draw(window, value: str, cursor: int, repo: str, message: str):
    window.erase()
    height, width = window.getmaxyx()
    if height < 16 or width < 58:
        put(window, 1, 2, "Wayfinder feature input needs at least 58 x 16 cells.")
        put(window, 3, 2, "Resize this terminal, or press Esc to cancel.")
        window.refresh()
        return

    box_width = min(width - 4, 74)
    left = (width - box_width) // 2
    top = max(1, (height - 13) // 2)
    field_width = box_width - 6
    rows, positions = layout(value, field_width)
    cursor_row, cursor_col = positions[cursor]
    top_line = max(0, cursor_row - 4)

    title = "Start a feature"
    put(window, top, left, "+" + "-" * (box_width - 2) + "+", curses.color_pair(2))
    for y in range(top + 1, top + 12):
        put(window, y, left, "|", curses.color_pair(2))
        put(window, y, left + box_width - 1, "|", curses.color_pair(2))
    put(window, top + 12, left, "+" + "-" * (box_width - 2) + "+", curses.color_pair(2))
    put(window, top + 1, left + 3, "WAYFINDER", curses.color_pair(3) | curses.A_BOLD)
    put(window, top + 2, left + 3, title, curses.A_BOLD)
    put(window, top + 3, left + 3, f"Repository: {repo}", curses.color_pair(2))
    put(window, top + 4, left + 3, "." + "-" * field_width + ".", curses.color_pair(2))
    for offset in range(5):
        y = top + 5 + offset
        put(window, y, left + 3, "|", curses.color_pair(2))
        put(window, y, left + 4, " " * field_width)
        put(window, y, left + 4 + field_width, "|", curses.color_pair(2))
        index = top_line + offset
        if index < len(rows):
            put(window, y, left + 4, rows[index])
    put(window, top + 10, left + 3, "'" + "-" * field_width + "'", curses.color_pair(2))
    hint = "Enter starts  Shift+Enter new line  Esc cancels"
    put(window, top + 11, left + 3, hint, curses.color_pair(2))
    if message:
        put(window, top + 13, left + 2, message, curses.color_pair(4))
    elif not value:
        put(window, top + 5, left + 4, "Describe the feature you want to build", curses.color_pair(2))

    screen_row = top + 5 + cursor_row - top_line
    screen_col = left + 4 + min(cursor_col, field_width - 1)
    if top + 5 <= screen_row <= top + 9:
        try:
            window.move(screen_row, screen_col)
        except curses.error:
            pass
    window.refresh()


def draw_progress(window, repo: str, detail: str):
    window.erase()
    height, width = window.getmaxyx()
    title = "Preparing your feature..."
    put(window, height // 2 - 1, max(1, (width - len(title)) // 2), title, curses.A_BOLD)
    put(window, height // 2 + 1, max(1, (width - len(detail)) // 2), detail, curses.color_pair(2))
    put(window, height // 2 + 3, max(1, (width - len(repo)) // 2), repo, curses.color_pair(3))
    window.refresh()


def draw_recovery(window, repo: str, launch_id: str):
    window.erase()
    height, width = window.getmaxyx()
    lines = [
        "Feature could not start. Your description was saved.",
        "Recovery ID: " + launch_id,
        "Run: wayfinder recover " + launch_id,
        "Open wayfinder again to retry when delivery is available.",
    ]
    top = max(0, (height - len(lines)) // 2)
    for offset, line in enumerate(lines):
        put(window, top + offset, max(1, (width - len(line)) // 2), line)
    put(window, min(height - 1, top + len(lines) + 1), 2, repo, curses.color_pair(3))
    window.refresh()


def read_key():
    # curses.get_wch() normalizes both CR and LF to '\n' even in raw mode.
    # Read the PTY bytes directly so Enter and Shift+Enter can differ.
    if not select.select([0], [], [], 0.25)[0]:
        return "idle", ""
    first = os.read(0, 1)
    if first == b"\x1b":
        tail = bytearray()
        deadline = time.monotonic() + 0.05
        while len(tail) < 32:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([0], [], [], remaining)[0]:
                break
            part = os.read(0, 1)
            tail.extend(part)
            if not tail.startswith(b"[") or (len(tail) > 1 and (part.isalpha() or part == b"~")):
                break
        sequence = bytes(tail)
        if not sequence:
            return "escape", ""
        if re.fullmatch(rb"\[(?:13|10);2(?::\d+)?u", sequence) or sequence in (b"[27;2;13~", b"\r"):
            return "newline", ""
        navigation = {b"[A": "up", b"[B": "down", b"[C": "right", b"[D": "left", b"[H": "home", b"[F": "end", b"[3~": "delete"}
        return navigation.get(sequence, "idle"), ""
    if first == b"\r":
        return "enter", ""
    if first in (b"\n", b"\x0e"):
        return "newline", ""
    if first in (b"\x7f", b"\x08"):
        return "backspace", ""
    if first == b"\x03":
        return "escape", ""
    if first[0] < 32:
        return "idle", ""
    if first[0] < 128:
        return "text", first.decode("ascii")

    expected = 2 if first[0] < 224 else 3 if first[0] < 240 else 4
    encoded = bytearray(first)
    while len(encoded) < expected and select.select([0], [], [], 0.05)[0]:
        encoded.extend(os.read(0, 1))
    try:
        return "text", encoded.decode("utf-8")
    except UnicodeDecodeError:
        return "idle", ""


def run_tui(window, repo: str, launch_id: str, on_submit):
    curses.set_escdelay(25)
    curses.raw()
    try:
        curses.curs_set(1)
    except curses.error:
        pass
    if curses.has_colors():
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(2, curses.COLOR_CYAN, -1)
        curses.init_pair(3, curses.COLOR_GREEN, -1)
        curses.init_pair(4, curses.COLOR_RED, -1)

    window.keypad(False)
    value = ""
    cursor = 0
    message = ""
    while True:
        draw(window, value, cursor, repo, message)
        kind, char = read_key()
        if kind == "escape":
            return "cancelled", ""
        if kind == "enter":
            if not value.strip():
                message = "Describe a feature before pressing Enter."
                continue
            draw_progress(window, repo, "Saving your description and preparing the chat")
            on_submit(value.strip())
            draw_recovery(window, repo, launch_id)
            time.sleep(6)
            return "submitted", value.strip()
        if kind == "newline":
            value = value[:cursor] + "\n" + value[cursor:]
            cursor += 1
        elif kind == "backspace":
            if cursor:
                value = value[: cursor - 1] + value[cursor:]
                cursor -= 1
        elif kind == "delete":
            value = value[:cursor] + value[cursor + 1 :]
        elif kind == "left":
            cursor = max(0, cursor - 1)
        elif kind == "right":
            cursor = min(len(value), cursor + 1)
        elif kind in ("up", "down"):
            _, positions = layout(value, max(1, min(window.getmaxyx()[1] - 10, 68)))
            cursor = move_vertical(cursor, positions, -1 if kind == "up" else 1)
        elif kind == "home":
            cursor = value.rfind("\n", 0, cursor) + 1
        elif kind == "end":
            next_break = value.find("\n", cursor)
            cursor = len(value) if next_break < 0 else next_break
        elif kind == "text" and len(value) < 4000:
            value = value[:cursor] + char + value[cursor:]
            cursor += 1
        message = ""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", nargs="?", choices=("launch", "popup", "coordinate", "recover"), default="launch")
    parser.add_argument("launch_id", nargs="?")
    args = parser.parse_args()
    if args.mode == "launch":
        launch()
        return
    launch_id = args.launch_id or os.environ.get("WAYFINDER_LAUNCH_ID")
    if not launch_id:
        raise ValueError("Missing Wayfinder launch identity")
    if args.mode == "recover":
        _, record = read_record(launch_id)
        if record["status"] != "submitted":
            raise RuntimeError("This launch has no submitted description to recover")
        print("Repository: " + record["checkout"])
        print("Description:\n" + record["description"])
        print("No feature effort was started. Run wayfinder again to retry.")
        return
    if args.mode == "coordinate":
        coordinate_or_record_error(launch_id)
        return
    path, record = read_record(launch_id)
    if record["status"] != "awaiting_input":
        raise RuntimeError("This Wayfinder input was already handled")

    def cancel_if_open():
        try:
            current_path, current = read_record(launch_id)
            if current["status"] == "awaiting_input":
                current["status"] = "cancelled"
                save_record(current_path, current)
        except (OSError, ValueError, KeyError, json.JSONDecodeError):
            pass

    atexit.register(cancel_if_open)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    signal.signal(signal.SIGHUP, lambda *_: sys.exit(0))

    def submit(value):
        record["description"] = value
        record["status"] = "submitted"
        save_record(path, record)

    status, _ = curses.wrapper(run_tui, record["checkout"], launch_id, submit)
    if status == "cancelled":
        record["status"] = "cancelled"
        save_record(path, record)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"Wayfinder: {error}", file=sys.stderr)
        sys.exit(1)
