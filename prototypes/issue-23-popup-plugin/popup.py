#!/usr/bin/env python3
"""THROWAWAY terminal popup prototype for Wayfinder issue 23.

Run: python3 prototypes/issue-23-popup-plugin/popup.py

This accepts input and simulates a short preparation step. It does not create a
map, start Herdr, access GitHub, or persist a submission unless --result-file
explicitly names a disposable prototype file.
"""

import argparse
import curses
import json
import os
import re
import select
import time
from pathlib import Path


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
    put(window, top + 1, left + 3, "WAYFINDER / INPUT PROTOTYPE", curses.color_pair(3) | curses.A_BOLD)
    put(window, top + 2, left + 3, title, curses.A_BOLD)
    put(window, top + 3, left + 3, f"Repository: {repo}  -  demo only", curses.color_pair(2))
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


def draw_progress(window, repo: str):
    window.erase()
    height, width = window.getmaxyx()
    title = "Preparing your feature..."
    detail = "Demo: a real launch would focus the orchestrator chat when ready."
    put(window, height // 2 - 1, max(1, (width - len(title)) // 2), title, curses.A_BOLD)
    put(window, height // 2 + 1, max(1, (width - len(detail)) // 2), detail, curses.color_pair(2))
    put(window, height // 2 + 3, max(1, (width - len(repo)) // 2), repo, curses.color_pair(3))
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


def run_tui(window, repo: str, delay: float):
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
            draw_progress(window, repo)
            time.sleep(delay)
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
    parser.add_argument("--repo-name", default=Path.cwd().name)
    parser.add_argument("--demo-delay", type=float, default=1.3)
    parser.add_argument("--result-file", type=Path, help="Disposable PROTOTYPE output file")
    args = parser.parse_args()
    status, value = curses.wrapper(run_tui, args.repo_name, args.demo_delay)
    result = {"prototype": True, "status": status, "description": value}
    if args.result_file:
        args.result_file.write_text(json.dumps(result, ensure_ascii=False) + "\n")
    print("Prototype submission saved for handoff." if status == "submitted" else "Cancelled. No feature effort started.")


if __name__ == "__main__":
    main()
