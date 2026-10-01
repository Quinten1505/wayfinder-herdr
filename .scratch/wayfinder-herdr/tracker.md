# Local tracker operations

The map is map.md; each file in issues/ is a child issue. Its filename is its identity. Metadata records Parent, Label, Type, Mode, Status, Assignee, and Blocked by.

Status is open, claimed, or resolved. Open, unassigned issues whose blockers are all resolved form the frontier, ordered by filename. Blocked by contains child filename stems, or none. This file-based tracker has no native dependency relationship.

Claim before work by setting Assignee and Status: claimed. Resolve by appending a resolution comment under ## Answer, setting Status: resolved, and appending a named link and one-line gist to the map's Decisions so far. Link findings and artifacts from the ticket. Keep detailed answers in tickets, not the map.

Create tickets before wiring blockers. Serialize shared map updates through the orchestrator when research agents run concurrently. Research findings live on research branches/worktrees; the canonical ticket links their branch, commit, and artifact path.
