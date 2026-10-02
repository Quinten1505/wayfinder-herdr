# Repository preparation for the one-command launcher

Research for [How can the launcher infer its repository and prepare an isolated feature?](https://github.com/Quinten1505/wayfinder-herdr/issues/21), within [Start a feature with one wayfinder command](https://github.com/Quinten1505/wayfinder-herdr/issues/19). Investigated 2026-10-02 against plugin commit `83f6976fb85ec7cd98de82ee899eebb52cd3f562` and official documentation. Recommendations below are proposals, not accepted decisions.

## Result

Git can discover the current checkout without a path argument; GitHub can verify repository identity and discover its default branch. Fork targets and delivery-base policy still require decisions. The current runtime hardcodes `origin/develop`, binds one exact checkout/workspace, and recovers the same map for identical inputs. A launcher wrapper alone cannot deliver the requested workflow reliably.

Accepted user choices: always prompt for a new feature; automatically create/select its branch; ask about local changes in orchestrator chat; automatically prepare missing labels/configuration; isolate concurrent features in separate worktrees/workspaces.

## Discovery and preparation facts

**Checkout:** use `git -C <invocation-cwd> rev-parse --is-inside-work-tree`, then `rev-parse --path-format=absolute --show-toplevel`. This handles subdirectories; a linked checkout has a `.git` file, so directory scanning is insufficient. Record canonical `--git-common-dir` separately to recognize concurrent efforts across linked worktrees. It identifies shared Git storage, not the orchestrator working directory. Bare/non-repository invocation needs an actionable error. [Git rev-parse](https://git-scm.com/docs/git-rev-parse)

**Remotes:** enumerate fetch and push URLs using `git remote get-url --all <remote>` and `--push --all`. Git expands `insteadOf`/`pushInsteadOf`, and destinations can differ. Do not silently rewrite remotes. Parse ordinary GitHub HTTPS, `ssh://git@github.com/OWNER/REPO.git`, and `git@github.com:OWNER/REPO.git` forms, stripping terminal `.git`/trailing slash. SSH aliases and non-GitHub URLs need explicit handling; do not infer github.com from an arbitrary hostname or echo embedded credentials. [Git remote](https://git-scm.com/docs/git-remote), [Git URL syntax](https://git-scm.com/docs/git-fetch#_git_urls)

**GitHub target:** unqualified gh inference is insufficient: gh has its own default repository, and `gh repo clone` selects a fork's parent by default. `GH_REPO` can also redirect commands. Verify an explicitly inferred identity with `gh repo view OWNER/REPO --json nameWithOwner,url,isFork,parent,defaultBranchRef,hasIssuesEnabled,isArchived,viewerPermission`. Preserve the user's gh defaults. Enterprise support needs host-aware changes beyond today's owner/repository map identity. [gh default](https://cli.github.com/manual/gh_repo_set-default), [fork clone behavior](https://cli.github.com/manual/gh_repo_clone), [gh environment](https://cli.github.com/manual/gh_help_environment), [repository fields](https://cli.github.com/manual/gh_repo_view)

**Access:** `gh auth status --active --hostname github.com` checks the intended account; JSON mode exits zero despite authentication issues, so parse fields if used. Never use `--show-token`. A private-resource 404 can mean missing authentication/access, not nonexistence. Metadata read permission does not prove label creation, Git push, or PR access. Verify API and Git transport independently and report the actual failing operation. [gh authentication](https://cli.github.com/manual/gh_auth_status), [GitHub 404 behavior](https://docs.github.com/en/rest/using-the-rest-api/troubleshooting-the-rest-api#404-not-found-for-an-existing-resource)

**Base:** `defaultBranchRef` and `git ls-remote --symref <remote> HEAD` supply candidates; neither decides the desired integration branch. Cached remote HEAD is optional/stale. Once policy selects the base, fetch it explicitly and pin its SHA. Save repository, remote, base name/ref, and feature branch so initial checkout, reviews, synchronization, pushes, and PRs agree. Do not silently fall back to current HEAD. [gh repository fields](https://cli.github.com/manual/gh_repo_view), [Git ls-remote](https://git-scm.com/docs/git-ls-remote), [Git remote HEAD](https://git-scm.com/docs/git-remote)

**New effort:** retain `feature/` for current compatibility; use a readable slug plus unique launch suffix. Validate with `git check-ref-format --branch`, and check local/remote collisions. Create with `git worktree add -b <branch> <path> <explicit-base-SHA>` for isolation. Git refuses existing branches and branches checked out elsewhere; preserve those safeguards, avoiding `-B`/force. `worktree list --porcelain -z` gives stable machine output. [Git branch validation](https://git-scm.com/docs/git-check-ref-format), [Git worktree](https://git-scm.com/docs/git-worktree)

**Local changes:** inspect `git status --porcelain=v1 -z --untracked-files=all`. A separate clean worktree leaves original changes untouched, but does not decide whether they belong to this feature. Ask in chat before carrying changes, stashing, committing, or otherwise changing their disposition. Clean status omits ignored files; avoid assuming an in-place branch switch cannot overwrite them. [Git status](https://git-scm.com/docs/git-status)

**Preparation:** create missing `wayfinder:map`, `research`, `prototype`, `grilling`, `task`, and `spec` labels (all with the `wayfinder:` prefix). Preserve existing metadata; avoid `gh label create --force`, which updates existing labels. Re-read after races/uncertain results. REST creation requires Issues or Pull requests write permission. Automatically generated local config can live outside tracked files; enabling GitHub Issues or changing remotes/defaults is a separate policy. [gh labels](https://cli.github.com/manual/gh_label_create), [label permissions](https://docs.github.com/en/rest/issues/labels#create-a-label)

## Current code constraints

The following source links are immutable:

- [CLI attach](https://github.com/Quinten1505/wayfinder-herdr/blob/83f6976fb85ec7cd98de82ee899eebb52cd3f562/src/main.rs#L318) canonicalizes a supplied directory without discovering its Git root. [Binding/reattach](https://github.com/Quinten1505/wayfinder-herdr/blob/83f6976fb85ec7cd98de82ee899eebb52cd3f562/src/store.rs#L826) rejects changed checkout/endpoint bindings. Establish the final feature checkout/workspace before attach.
- [Chat action context](https://github.com/Quinten1505/wayfinder-herdr/blob/83f6976fb85ec7cd98de82ee899eebb52cd3f562/src/orchestration.rs#L659) requires verified action context and exact workspace cwd. [Worker worktrees](https://github.com/Quinten1505/wayfinder-herdr/blob/83f6976fb85ec7cd98de82ee899eebb52cd3f562/src/herdr.rs#L443) open within the owning workspace; they do not establish a new feature workspace.
- [Delivery](https://github.com/Quinten1505/wayfinder-herdr/blob/83f6976fb85ec7cd98de82ee899eebb52cd3f562/src/delivery.rs) discovers `feature/` from the bound checkout. `origin/develop` is hardcoded in final scope, readiness, base updates, PR selection/creation, and publishing. Main-only repositories and upstream PR/fork publishing require consistent runtime changes.
- [Map creation](https://github.com/Quinten1505/wayfinder-herdr/blob/83f6976fb85ec7cd98de82ee899eebb52cd3f562/src/tracker.rs#L312) hashes repo/title/notes/override for retry recovery. Deliberately new launches with identical inputs return the same map. Introduce per-launch identity while preserving same-launch retries.
- [Installer](https://github.com/Quinten1505/wayfinder-herdr/blob/83f6976fb85ec7cd98de82ee899eebb52cd3f562/scripts/install.py) prepares installed artifacts/unit, not repository labels. No mandatory new tracked repo configuration exists today.

## Follow-up human decisions

1. Fork/multiple remotes: choose origin, upstream, or a named selection in chat? Recommend asking only for ambiguity and persisting the choice; do not let gh's implicit upstream selection decide.
2. Base: retain develop with a fallback policy, or consistently use repository default with a saved override? Recommend default plus override, preserving existing efforts' base semantics.
3. First effort: allow in-place branch selection for a clean checkout, or isolate every effort? Concurrent isolation is already decided. Recommend considering isolation for every effort; define chat choices for including local work without automatic transfer.

These fit the existing lifecycle/setup decision. Check-suite/provider expansion remains outside this research.

## Validation boundary

This was source/documentation research. An isolated research worktree was created successfully; the original checkout's `CONTEXT.md` edit was preserved. No target-repository/GitHub setup or Herdr launch was attempted. Installation/PATH and exact host-launch APIs are companion research concerns.
