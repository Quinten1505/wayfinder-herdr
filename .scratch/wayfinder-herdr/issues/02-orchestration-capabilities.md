# What orchestration capabilities and lifecycle guarantees does herdr provide?

Parent: ../map.md
Label: wayfinder:research
Type: research
Mode: AFK
Status: resolved
Assignee: orchestration-research
Blocked by: none

## Question

Inventory agent providers, start/prompt/read/wait, lifecycle events, workspaces, worktrees, tabs, panes, sessions, notifications, integrations, remote machines, and socket/schema access. Identify concrete APIs and guarantees needed for isolated implementation and review, human input, restart recovery, completion detection, and resource cleanup. Distinguish supported behavior from gaps and race conditions.

## Research context

Branch: `research/orchestration-capabilities`
Worktree: `/tmp/wayfinder-herdr-orchestration-research`
Findings: `docs/research/02-orchestration-capabilities.md` on that branch.

## Answer

Resolution comment, 2026-10-01.

Herdr 0.9.3 supplies agent, worktree, layout, event, session, notification, and remote-machine primitives. Occupant-pinned prompt/wait and settled lifecycle states do not certify a specific task result. Events are non-durable and have no common snapshot boundary; cold restart replaces processes. Wayfinder must own claims, run/result identity, review commit identity, human decisions, resource ownership, and recovery. The research establishes interfaces, not live integration correctness.

[Primary-source findings](/tmp/wayfinder-herdr-orchestration-research/docs/research/02-orchestration-capabilities.md) are preserved on `research/orchestration-capabilities` at commit `b0fbb08cc14e2414fb961d6cb8186f21b5a0e7f7`, path `docs/research/02-orchestration-capabilities.md`. Retrieve independently of the temporary worktree with `git show b0fbb08cc14e2414fb961d6cb8186f21b5a0e7f7:docs/research/02-orchestration-capabilities.md`.
