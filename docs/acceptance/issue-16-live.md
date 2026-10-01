# Issue 16 acceptance evidence

Canonical ticket: [Prove installation and the complete workflow in an isolated live session](https://github.com/Quinten1505/wayfinder-herdr/issues/16)  
Specification: [Wayfinder herdr plugin specification](https://github.com/Quinten1505/wayfinder-herdr/issues/10)  
Acceptance map: [Acceptance: exact milli-unit quantity parsing](https://github.com/Quinten1505/wayfinder-herdr-acceptance-20261001-issue16-7c4a/issues/1)  
Implementation ticket: [Implement exact milli-unit parsing with a real human whitespace decision](https://github.com/Quinten1505/wayfinder-herdr-acceptance-20261001-issue16-7c4a/issues/2)  
Dependent documentation ticket: [Document milli-unit parser semantics and error handling](https://github.com/Quinten1505/wayfinder-herdr-acceptance-20261001-issue16-7c4a/issues/3)

This is a running acceptance record, not a claim that the complete live walkthrough has passed. The disposable repository and its logs may contain private data; this file records identifiers and outcomes only, not raw logs or credentials.

## Environment and pinned inputs

- Acceptance repository: private `Quinten1505/wayfinder-herdr-acceptance-20261001-issue16-7c4a`; initial `develop` commit `4155227aa9beb20bfb72a3ec953c8c3c4fcd09fe`.
- Wayfinder implementation checkout began at `ae0c0916ffb41d9cdf47e0f244586480aae53b94`.
- Herdr CLI/server: `0.9.3`, protocol `22`.
- Codex CLI: `0.157.1`.
- Rust/Cargo: `1.98.0` (`88d9e12ae`, `797e8a9bc`).
- Live map: `quinten1505/wayfinder-herdr-acceptance-20261001-issue16-7c4a#1`; map key `a17b3c907d0d945e87e2bb22ba9e1b3751b80d2885e472dfd6222e28405db1ab`.
- Named Herdr session: `wayfinder-acceptance-20261001-issue16`; isolated config/state roots are under `/tmp/wf16-acceptance`. Runtime binding persists source workspace `w2` and the exact session socket. The reusable `/tmp/wf16-acceptance/bin/wf16-env` clears inherited `HERDR_*`, sets owned XDG/Herdr paths, preserves `HOME` and `CODEX_HOME`, and always selects the named session.
- Worker provider stored on the live map: `codex`, model `gpt-6-luna`, reasoning `high`, args `--approve-for-me -c 'service_tier="priority"'`.

## Installer evidence (stubbed systemctl)

The installer smoke test `python3 tests/install_smoke.py` passed in temporary XDG roots. It built and installed into a temporary path with spaces, linked the plugin in an isolated Herdr instance, verified the unit syntax with `systemd-analyze`, checked that no map was started, and verified that an unsupported state format preserves both the prior binary and state. Its `systemctl` was a recording stub; this result does **not** establish real service supervision. Recording evidence is `/tmp/wayfinder-install-wwqhs9ub/systemctl.log`.

One installer attempt was rejected by automatic approval review. The exact stated reason was: “The wrapper does not set `WAYFINDER_INSTALL_DIR`, so this would reinstall into the default user-local plugin path rather than the authorized disposable directory and could overwrite the existing installation; use the explicit `/tmp/wf16-acceptance/install` target instead. Do not bypass this rejection through a workaround or indirect execution.” The retry set `WAYFINDER_INSTALL_DIR=/tmp/wf16-acceptance/install` and succeeded; subsequent installer commands kept that explicit target.

There was an earlier setup mistake before that rejection: one install invocation isolated XDG paths but omitted `WAYFINDER_INSTALL_DIR`, so `scripts/install.py` atomically copied the source-built executable and manifest into the default user-local plugin directory. The affected current paths and read-only metadata are:

| Path | Current size/mode | Current timestamp metadata (CEST) | Current SHA-256 |
| --- | --- | --- | --- |
| `/home/qbruinsma/.local/lib/wayfinder-herdr/bin/wayfinder-herdr` | 3,443,704 bytes, 0755 | mtime 2026-10-01 20:17:56.673744821; ctime/birth 20:21:22.495998441 | `9dcc3a875b84b493d11ef3961f7845b28acd8cf086a5eb6d35e528a727aba7d2` |
| `/home/qbruinsma/.local/lib/wayfinder-herdr/herdr-plugin.toml` | 1,307 bytes, 0644 | mtime 2026-10-01 20:14:47.306245202; ctime/birth 20:21:22.496908030 | `482e59c9bff5adeeeaea2dcde7cf73ced7d4391cd99557345d3df9149af6b1ca` |

The containing directory currently has birth/mtime 20:21:22.496908030 CEST. Those birth times are consistent with the directory and files being created by that invocation, but no pre-install listing/hash or durable installer log exists to prove whether destination files existed before it. The installer uses a temporary file plus `os.replace`, cleans up that temporary file, and does not create backups; scoped searches found no backup or copy. The disposable Herdr session linked that default path; no production Herdr session was started and no production Herdr configuration or unit file was written. Because the installer source issues `systemctl --user daemon-reload` unless stubbed, and the mistaken invocation left no recording log, whether it requested a production user-manager reload cannot be established; no production service was started or enabled. The old file contents and hashes are unknown. This is an unintended production-file overwrite, not an isolated install result. No restoration or cleanup was attempted.

## Real live runtime evidence

The real source-installed plugin was linked only in the named disposable Herdr session with `herdr plugin action invoke start --plugin wayfinder.herdr`. A real transient systemd **user** service, `wayfinder-acceptance-20261001-issue16-runtime.service`, supervised `/tmp/wf16-acceptance/install/bin/wayfinder-herdr --state-dir /tmp/wf16-acceptance/state/wayfinder-herdr serve --key a17b3c907d0d945e87e2bb22ba9e1b3751b80d2885e472dfd6222e28405db1ab`. Its observed invocation after restart was `3c358e017c33444f8e5be07f5ad1f887`, state `active/running`, PID `969017`. The user manager was reached via `systemctl --machine=qbruinsma@.host --user` because direct user-bus access failed in this environment. The service was observed active/running and restarted with a new invocation after the client fix. This is real process supervision, separate from the recording-stub installer smoke. The unit's explicit environment included isolated `XDG_CONFIG_HOME`, `XDG_STATE_HOME`, `XDG_CACHE_HOME`, `XDG_DATA_HOME`, `XDG_RUNTIME_DIR`, Herdr config/socket/session, and source workspace `w2`; the reusable helper preserved `HOME` and `CODEX_HOME` and removed inherited `HERDR_*` before setting owned values.

The first live attempt created an exact detached checkout but Herdr rejected `worktree.open` because Wayfinder had not bound the source repository workspace. The persisted binding now records `w2`; the runtime client sends that owning workspace ID explicitly so later focus changes do not select a different source workspace. The next attempt reached Herdr but was rejected because the request supplied both `workspace_id` and `cwd`, which Herdr 0.9.3 disallows. The request was corrected to pass `path` and `workspace_id` without `cwd`.

The explicit install-and-link command was `WAYFINDER_INSTALL_DIR=/tmp/wf16-acceptance/install PATH=/tmp/wf16-acceptance/stubbin:$PATH /tmp/wf16-acceptance/bin/wf16-env ./scripts/install.sh`. Run 3 now has a real detached checkout at `/tmp/wf16-acceptance/.wayfinder-1-2-3`, opened under `w3` / `w3:t1` / `w3:p1`. Herdr `pane process-info --pane w3:p1` observed the actual Codex process `977215` with argv `/tmp/wf16-acceptance/data/mise/installs/codex/latest/bin/codex --approve-for-me -c service_tier="priority" --model gpt-6-luna -c model_reasoning_effort=high`; Herdr identified it as `codex` agent `wf-2-run-00000000000000000003`. This verifies the requested priority/Fast tier and high reasoning reached the live worker process. Wayfinder correctly retained the attempt as uncertain with its capacity reserved when Herdr reported status `unknown`; it did not send the task prompt. Inspection of the exact owned pane found Codex's native “Trust this folder?” confirmation, naming repository root `/tmp/wf16-acceptance/repo`. The pane is intentionally untouched and waiting. The exact screen was preserved at `/tmp/wf16-acceptance/evidence/codex-trust-prompt.txt`; the pending trust decision is recorded at `/tmp/wf16-human-request.json`, including map/run/session/pane IDs and answer commands. No answer has been supplied. The worker's parser whitespace question has therefore **not** been requested yet; its request ID does not exist.

While that UI was pending, the isolated user service was restarted with `systemctl --machine=qbruinsma@.host --user restart wayfinder-acceptance-20261001-issue16-runtime.service`. Invocation `3c358e017c33444f8e5be07f5ad1f887` became `a4b8e036c88a4149a4cc2f3d9dba5a26`; `systemctl ... show` reported `active/running` at PID `978899`. A subsequent map status still reported run 3 `uncertain`, the same `w3` / `w3:t1` / `w3:p1` identity, one of two slots reserved, and the same pending initial-prompt state. Re-reading `w3:p1` showed the same unanswered folder-trust prompt. No input was sent and the detached checkout was retained.

## Automated versus live coverage

- `cargo fmt --check`: passed.
- `cargo clippy --locked --all-targets -- -D warnings`: passed with `CARGO_HOME=/tmp/wayfinder-12-review-cargo-home`.
- `cargo test --locked`: passed (19 unit, 53 dispatch, 10 runtime, 11 tracker tests; 93 total). Dispatch fixtures require local Unix sockets; a restricted run failed with EPERM, then the same suite passed with the authorized escalation.
- Live so far: install/link in isolated Herdr; explicit Start; execution override and ticket claim; real detached checkout; source workspace persistence; real service active/restart; provider args persisted; recovery from known prelaunch failures.
- Still outstanding live: human folder trust and the actual worker question/answer; worker implementation and independent exact-commit review/rework; ticket integration; ready feature PR; lost/duplicate hook reconciliation; cold Herdr restart; pause/stop and uncertain-work preservation under a real worker; full reproduction bundle and teardown. Automated fixtures are not evidence of these live behaviors.

No live acceptance completion is asserted while the actual trust gate is pending. This checkpoint is **blocked on real human trust**, not DONE or accepted issue16. Preserve the pending UI and its exact identity until the human responds. Remaining live steps include the actual parser question and answer, worker launch and provider-argument verification, implementation, exact-commit independent review/rework, issue integration under root approval, ready feature PR, lost/duplicate hook reconciliation, Herdr cold restart, and real-worker pause/stop/uncertain-work preservation.
