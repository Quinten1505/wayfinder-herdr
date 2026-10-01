# Wayfinder for herdr

A Rust plugin foundation for one orchestrating chat to coordinate a map, delegated agents, independent reviewers, and Git worktrees.

- [Decision map](https://github.com/Quinten1505/wayfinder-herdr/issues/1)
- [Specification](https://github.com/Quinten1505/wayfinder-herdr/issues/10)
- [Tracker conventions](docs/agents/issue-tracker.md)

The runtime foundation and GitHub map/ticket workflow are implemented. Worker dispatch, human decision routing, reviews, and feature integration are subsequent tickets. **No workers are launched by this release, including after Start.** Installation is not dispatch authorization.

## Build and install

Prerequisites: Linux, Rust/Cargo (see `rust-version` in Cargo.toml), a C linker, Python 3, GitHub CLI (`gh`) authenticated for the target repository, herdr **0.9.3**, and a running systemd user manager. Validated with Rust/Cargo 1.98.0; dependency metadata supports the declared Rust 1.85 minimum, which has not been exercised here. Cargo needs access to crates.io on the first build. Herdr's installed CLI and the bound server must both report 0.9.3; a newer release is not automatically assumed compatible.

From this source checkout:

```sh
./scripts/install.sh
```

This builds with `cargo build --release --locked`, copies the executable and manifest into `~/.local/lib/wayfinder-herdr`, links the plugin, installs `wayfinder-herdr@.service` in the user service directory, and reloads systemd. It does not attach, start, or enable a map runtime. `WAYFINDER_INSTALL_DIR` overrides the install directory. `XDG_CONFIG_HOME` and `XDG_STATE_HOME` are respected and recorded in the unit. The source checkout must remain available only for rebuilding, not for running the installed plugin.

Use the installer above to provision the plugin and supervision together. `sh scripts/build.sh` is a source build helper that creates a development binary in `bin/` without installing or linking it.

## Attach and explicitly Start

Run from the intended herdr session, supplying the repository checkout and canonical GitHub map identity:

```sh
~/.local/lib/wayfinder-herdr/bin/wayfinder-herdr attach \
  --map 'OWNER/REPO#NUMBER' --repository /absolute/path/to/repository
~/.local/lib/wayfinder-herdr/bin/wayfinder-herdr status --map 'OWNER/REPO#NUMBER'
~/.local/lib/wayfinder-herdr/bin/wayfinder-herdr start --map 'OWNER/REPO#NUMBER'
```

Attach binds the repository, herdr executable, and socket, initializes state as `awaiting_start`, and enables/starts that map's service. `--socket /absolute/socket` and `--herdr /absolute/herdr` make the endpoint explicit. `--no-service` initializes state without starting systemd. The printed map key identifies `wayfinder-herdr@KEY.service`; `journalctl --user -u wayfinder-herdr@KEY.service` shows failures.

The Start, Pause, Resume, and Status plugin actions select the unique attached map matching the invoking workspace and herdr socket. If multiple maps match, use the CLI with `--map`. Start/Pause/Resume return when a request has been durably queued; inspect status for its applied outcome. A first Resume cannot substitute for explicit Start. Pause prevents future dispatch authorization; it does not cancel work. Startup and lifecycle hooks only queue reconciliation requests.

The runtime checks host compatibility and plugin enablement, reads the map's ordered frontier, and stores a local frontier snapshot. It does not launch workers. A compatible host and a Start request alone cannot permit dispatch.

## GitHub map and ticket workflow

Attach a map before running ticket mutations so they share the map runtime's local state lock. New maps can be created while planning and start without an execution override:

```sh
wayfinder-herdr tracker create-map --repository OWNER/REPOSITORY \
  --title "Destination" --notes "Planning notes"
wayfinder-herdr tracker create-map --repository OWNER/REPOSITORY \
  --title "Destination" --notes "Selected scope" --execution-override
```

After attaching the returned `OWNER/REPOSITORY#NUMBER`, create decision or task tickets, add native dependencies, and inspect the frontier:

```sh
wayfinder-herdr tracker create-ticket --map OWNER/REPOSITORY#NUMBER \
  --title "Research question" --label wayfinder:research
wayfinder-herdr tracker block --map OWNER/REPOSITORY#NUMBER --ticket 24 --by 23
wayfinder-herdr tracker frontier --map OWNER/REPOSITORY#NUMBER
```

Planning operations, including ticket creation, dependencies, frontier reads, and decision resolution, do not require the execution override. Claims are GitHub assignments and require the map Notes to record `Execution override: ... selected`. Any existing assignment is a claim. Resolution comments use a stable operation marker, then close the ticket and update the map and spec indexes; retries inspect the marker before posting again. Body updates merge the latest fetched text and verify the result. GitHub does not support conditional issue-body writes, so a simultaneous external edit at the exact write boundary can still require a retry or manual reconciliation.

## State, restart, and upgrades

State lives in `${XDG_STATE_HOME:-~/.local/state}/wayfinder-herdr/maps/KEY`, outside the plugin checkout. A map key hashes normalized `owner/repository#number`, making the runtime exclusive per map within this machine's configured state root. Use one state root for all sessions; distinct overridden roots are separate installations and cannot arbitrate each other.

Each map has a lifetime kernel lock, a versioned `state.json`, and a durable request inbox. State updates use a synced temporary file, atomic rename, and directory sync. Applied request identities remain in history so crash replay cannot reapply them. The service restarts on failure; runtime authorization and queued requests survive process death. A 30-second default compatibility check (`attach --poll-seconds`) backs up hooks, with capped outage backoff. Missing/incompatible herdr or a disabled plugin suspends readiness.

For manual upgrades:

1. Note active instances with `systemctl --user list-units 'wayfinder-herdr@*.service'` and stop those instances. The installer refuses replacement while an instance is active.
2. Back up the state root after stopping the services, update this source checkout, and rerun `./scripts/install.sh`.
3. Start the previously running instances with `systemctl --user start wayfinder-herdr@KEY.service`. Do not issue a fresh Start to override an existing pause.

Upgrades preserve the state root. Unsupported formats or fields produce actionable errors; the runtime never resets history or silently migrates it. Use a compatible plugin release or restore a compatible backup while the services are stopped. Preserve the rejected files for recovery. A changed repository or endpoint binding also requires explicit repair; reattachment does not silently rebind an existing map.

## Checks

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
python3 tests/install_smoke.py
```

The installer smoke test builds and links in temporary XDG directories, substitutes a recording-only `systemctl`, and validates the generated unit with `systemd-analyze`; it does not start a live service.

Tests cover exclusive runtimes, durable requests and authorization across restart, replay, corrupt/future state preservation, host compatibility and disabled plugins. The isolated herdr/Codex walkthrough and complete workflow acceptance belong to [Prove installation and the complete workflow in an isolated live session](https://github.com/Quinten1505/wayfinder-herdr/issues/16).
