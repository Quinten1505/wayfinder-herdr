#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
cargo build --release --locked
build_target_dir=$(cargo metadata --format-version 1 --no-deps --locked | python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')
mkdir -p bin
install -m 755 "$build_target_dir/release/wayfinder-herdr" bin/wayfinder-herdr
