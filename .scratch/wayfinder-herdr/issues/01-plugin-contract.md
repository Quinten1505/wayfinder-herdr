# How can a Rust plugin integrate with herdr?

Parent: ../map.md
Label: wayfinder:research
Type: research
Mode: AFK
Status: resolved
Assignee: plugin-research
Blocked by: none

## Question

What plugin manifest, executable or runtime contract, actions, hook/event interfaces, configuration, permissions, installation, and compatibility guarantees does herdr 0.9.3 actually provide? Determine a supported Rust integration path using primary sources, distinguish installed behavior from upstream changes, and document unsupported assumptions.

## Research context

Branch: `research/plugin-contract`
Worktree: `/tmp/wayfinder-herdr-plugin-research`
Findings: `docs/research/01-plugin-contract.md` on that branch.

## Answer

Resolution comment, 2026-10-01.

Herdr 0.9.3 supports a standalone Rust executable declared in herdr-plugin.toml, including actions, event hooks, startup commands, and panes. Startup is one-shot; hooks are non-durable, have a restricted allowlist, and share a 32-command concurrency limit. The plugin must own workflow durability and human merge authorization. Primary-source research is complete; live plugin validation remains future work.

[Primary-source findings](/tmp/wayfinder-herdr-plugin-research/docs/research/01-plugin-contract.md) are preserved on `research/plugin-contract` at commit `f7b5d7a`, path `docs/research/01-plugin-contract.md`. Retrieve independently of the temporary worktree with `git show f7b5d7a:docs/research/01-plugin-contract.md`.
