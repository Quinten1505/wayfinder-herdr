# Wayfinder orchestration in herdr

Label: wayfinder:map
Status: open

## Destination

Deliver a working Rust herdr plugin implementing the wayfinder workflow, with one orchestrating chat inside herdr delegating to agents in worktrees and panes/tabs, including implementation instances, independent reviewers, and herdr hooks.

## Notes

- Execution override: this effort includes implementation and review, as explicitly selected by the user on 2026-10-01. Charting remains one session; subsequent sessions work one non-research ticket at a time.
- Dispatch ready work and run implementation/review cycles automatically after human decisions are settled. Merges require the human's decision.
- Human-in-the-loop decisions return to the orchestrating chat; workers cannot supply the human's answers.
- Consult wayfinder, grilling, and domain-modeling when resolving decisions; research for primary-source investigations.
- Tracker: local Markdown, using the conventions in tracker.md. No tracker was configured; `/setup-matt-pocock-skills` can configure a different tracker later.
- Installed target observed at charting: herdr 0.9.3. Verify plugin compatibility against its actual interfaces.
- “Everything herdr supports” calls for a capability inventory and an explicit coverage decision; it is not evidence that every capability belongs in this plugin.

## Decisions so far

## Not yet specified

- Detailed scheduling, resource limits, cancellation, and recovery behavior depend on herdr's lifecycle guarantees and the selected execution model.
- Packaging, installation, upgrade behavior, and end-to-end acceptance scenarios depend on the plugin contract.
- How to expose capabilities beyond the core worktree/agent/review flow depends on the capability inventory and coverage decision.
- Implementation sequence will emerge after the runtime, state ownership, and review decisions.

## Out of scope

- Autonomous merges: the user selected human-controlled merges.
