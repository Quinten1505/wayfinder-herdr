# Wayfinder for herdr

A Rust plugin foundation for one orchestrating chat to coordinate a map, delegated agents, independent reviewers, and Git worktrees.

- [Decision map](https://github.com/Quinten1505/wayfinder-herdr/issues/1)
- [Specification](https://github.com/Quinten1505/wayfinder-herdr/issues/10)
- [Tracker conventions](docs/agents/issue-tracker.md)

The runtime and GitHub map/ticket workflow dispatch ready research and task tickets to role-configured Herdr agents. Implementers and independent reviewers use separate detached-HEAD worktrees. Ticket results remain local evidence; review, ticket integration, and feature PR handoff are subsequent work. Installation is not dispatch authorization.

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

The runtime checks host compatibility and plugin enablement, reconciles the map's ordered frontier, and dispatches only after explicit Start and a map execution override. Researchers handle `wayfinder:research` and `wayfinder:prototype`; implementers handle `wayfinder:task`. Decision/grilling tickets remain for the orchestrator. Herdr worktree opening uses normal repository trust behavior and never changes trust automatically.

Configure shared defaults or a role override. Provider-specific argv is passed as separate arguments:

```sh
wayfinder-herdr configure-worker --map OWNER/REPOSITORY#NUMBER \
  --role implementer --kind codex --model MODEL --reasoning-effort high
wayfinder-herdr configure-worker --map OWNER/REPOSITORY#NUMBER \
  --role reviewer --kind codex --arg=--full-auto --concurrency 2
```

Roles are `orchestrator`, `researcher`, `implementer`, and `reviewer`; concurrency is shared across delegated roles and defaults to three. Reviews are selected before other queued work when capacity opens. The runtime writes a durable run intent before claiming a ticket or creating resources. It uses `git worktree add --detach`, then opens that exact checkout through the bound Herdr socket, with explicit returned workspace/tab/pane IDs. It never creates a ticket branch.

Workers are prompted to read the actual ticket, map, linked spec (when present), and accepted comments using repository-qualified GitHub identities. Their `.wayfinder-result.json` is correlated by run and ticket ID, durably copied under the map's private state directory, then removed from the source checkout. This makes the owned protocol file compatible with clean-tree validation while preserving evidence across restart and eventual worktree cleanup; any other dirty or untracked files still block implementation/reviewer acceptance. A settled Herdr status alone is never accepted as success. Running workers reconnect only when pane, terminal, observed Herdr agent-session, Linux boot ID, and foreground process PID/start time match the persisted identity. Implementations must identify a clean committed HEAD; reviewers must identify the exact pinned commit. Uncertain launch/stop state retains its capacity, claim, checkout, and output rather than triggering a duplicate worker. The runtime caps automatic retries at two after confirmed worker failures and permits three separate review/rework rounds.

Status displays run identities, worker questions and retained resources. The CLI exposes explicit controls for blocked/uncertain work:

```sh
wayfinder-herdr status --map OWNER/REPOSITORY#NUMBER
wayfinder-herdr answer-worker --map OWNER/REPOSITORY#NUMBER --run RUN_ID \
  --request-id HUMAN_REQUEST_ID --request-type worker_question \
  --response "the human's actual answer"
wayfinder-herdr stop-worker --map OWNER/REPOSITORY#NUMBER --run RUN_ID
wayfinder-herdr retry-worker --map OWNER/REPOSITORY#NUMBER --run RUN_ID --confirmed-absent-or-stopped
wayfinder-herdr abandon-worker --map OWNER/REPOSITORY#NUMBER --run RUN_ID
```

`answer-worker` requires the exact pending request ID and type shown by `status`. For a recorded worker question (`worker_question`), it submits the provided human response through Herdr's occupant-pinned `agent.prompt` API, after verifying worker identity. If Herdr reports that the worker has entered a recognized approval or question UI (`herdr_blocked_ui`), `answer-worker` retains the supplied answer and request evidence but does not send input. Open the named Herdr pane, inspect and answer that UI directly, then run `reconcile`; the worker keeps its capacity and retained evidence until its later state can be observed. A worker question that races into a blocked UI gets a fresh manual-interaction request, and the earlier answer is retained without replay. Ambiguous answer or stop outcomes are not resent automatically. Retrying an uncertain worker requires the caller to explicitly confirm that the prior worker is absent or stopped. Abandoning records the human decision but does not prove termination, release uncertain capacity, or delete retained work.

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
wayfinder-herdr tracker reconcile-claim --map OWNER/REPOSITORY#NUMBER --ticket 24
wayfinder-herdr retry-worker --map OWNER/REPOSITORY#NUMBER --run RUN_ID --confirmed-absent-or-stopped
```

Planning operations, including ticket creation, dependencies, frontier reads, and decision resolution, do not require the execution override. Claims are GitHub assignments and require an affirmative execution override in the map Notes. Any existing assignment is a claim. A reassignment or closure racing with a claim makes its durable local intent uncertain; the command fails and preserves all GitHub assignments. To recover, `tracker reconcile-claim` reads the current issue, map membership, assignees, and blockers; it clears the uncertain hold only when GitHub proves the ticket is still open, unblocked, and assigned exclusively to the requested user. It never assigns or reassigns. Foreign, empty, closed, blocked, or unreadable states remain uncertain for later inspection. After confirming that no previous worker is active, the human can use `retry-worker --confirmed-absent-or-stopped`; this records a separate retry intent and the runtime reuses the proven assignment. On resolution, the orchestrator posts a named decision pointer comment on the map and a proposed specification delta comment on the spec. Both say **body refresh pending for a human**; automation never patches existing map/spec bodies. Stable operation markers let retries find comments after ambiguous writes without duplicating them. This follows the human decision in [How should map updates handle non-atomic GitHub body writes?](https://github.com/Quinten1505/wayfinder-herdr/issues/18#issuecomment-5931910902).

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

Detached worktree and socket dispatch acceptance tests are included in `tests/dispatch.rs`; run them in an environment where isolated Unix sockets are permitted. The tracker issue body and spec remain human-owned and are never refreshed by worker automation; map/spec updates use append-only comments with body refresh pending for a human.
