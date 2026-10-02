# Issue tracker: GitHub

Issues, the wayfinder map, and the evolving specification live in GitHub Issues for `Quinten1505/wayfinder-herdr`. Use `gh` with `--repo Quinten1505/wayfinder-herdr` for issue commands and explicit repository paths for API calls.

- [Wayfinder orchestration in herdr](https://github.com/Quinten1505/wayfinder-herdr/issues/1) is the canonical map.
- [Wayfinder herdr plugin specification](https://github.com/Quinten1505/wayfinder-herdr/issues/10) is the canonical draft spec. Update it as decisions resolve; preserve links to the decision tickets.
- `.scratch/wayfinder-herdr/` contains redirects retained for old context pointers. Update GitHub, not those files. Git history preserves the original local tracker.
- Research assets are versioned files on the published `research/plugin-contract` and `research/orchestration-capabilities` branches, linked by immutable commit from resolution comments.

## Wayfinding operations

The map is labelled `wayfinder:map`. Its tickets are native GitHub sub-issues, each labelled `wayfinder:research`, `wayfinder:prototype`, `wayfinder:grilling`, or `wayfinder:task`. The separate specification is labelled `wayfinder:spec`; it is not a decision ticket.

Read an issue with `gh issue view NUMBER --repo Quinten1505/wayfinder-herdr --comments`. Refer to issues in human-facing text by linked title.

Create a child with `gh issue create --repo Quinten1505/wayfinder-herdr --title TITLE --label LABEL --body-file FILE`. Then fetch its numeric database ID with `gh api repos/Quinten1505/wayfinder-herdr/issues/CHILD --jq .id` and attach it using `gh api --method POST repos/Quinten1505/wayfinder-herdr/issues/MAP/sub_issues -F sub_issue_id=DATABASE_ID`.

Create all tickets before wiring blockers. Add native dependencies with `gh api --method POST repos/Quinten1505/wayfinder-herdr/issues/CHILD/dependencies/blocked_by -F issue_id=BLOCKER_DATABASE_ID`. Issue numbers and GraphQL node IDs are not database IDs. Native relationships are canonical; do not maintain a second dependency list in issue bodies.

Find the frontier by listing the map's sub-issues with `gh api --paginate repos/Quinten1505/wayfinder-herdr/issues/MAP/sub_issues`. Preserve map order and select open, unassigned children with no open blockers. Inspect `issue_dependencies_summary.blocked_by`, or fetch `issues/CHILD/dependencies/blocked_by` and check blocker states when the summary is absent.

Claim before work by assigning the ticket to the driving developer using `gh issue edit NUMBER --repo Quinten1505/wayfinder-herdr --add-assignee @me`. Re-read before dispatch. GitHub assignment is the visible claim convention, not an atomic scheduling lock; parallel sessions belonging to one developer must coordinate through the orchestrator.

Resolve by posting the answer as a comment using `gh issue comment NUMBER --repo Quinten1505/wayfinder-herdr --body-file FILE` and closing the issue. Automated index updates go in append-only map/spec comments under the accepted policy below. Link artifacts from the resolution. Open work is found through sub-issue queries, not a duplicate list in the map body.

Use temporary body files for multiline creates and comments. Preserve unrelated changes during any human-authorized body refresh; serialize shared index updates through the orchestrator.

## Automated map and spec updates

The human resolved [How should map updates handle non-atomic GitHub body writes?](https://github.com/Quinten1505/wayfinder-herdr/issues/18#issuecomment-5931910902) in favor of append-only comments. Automated resolution/index/spec updates must post the named map decision pointer and proposed spec delta as comments, explicitly marking **body refresh pending for a human**. Do not PATCH existing map or spec bodies for these updates; initial issue creation may set its body.

Future workers must read relevant issue comments for accepted policy or specification deltas that have not yet been refreshed into the issue body. Reconcile uncertain comment submissions by searching for their durable operation marker before retrying, so a repeated request does not duplicate a comment.

## Scope

The human-selected execution override, automatic dispatch/review, and human-controlled merges are recorded in the map and spec. This repository's GitHub tracker choice does not decide the delivered plugin's supported tracker adapters.

PRs as a triage request surface: no.
