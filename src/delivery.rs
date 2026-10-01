//! Exact-commit review evidence and serialized ticket integration.
//!
//! Delivery state is kept beside the map runtime state so integration retries survive
//! process restarts without changing the worker identity/recovery records.
use crate::{
    store::{self, Lock, State, WorkerStatus},
    tracker::{ChildTicket, MapRef},
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    hash::{Hash, Hasher},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DeliveryState {
    #[serde(default)]
    pub repository: Option<String>,
    #[serde(default)]
    pub feature_branch: Option<String>,
    #[serde(default)]
    pub tickets: BTreeMap<String, TicketDelivery>,
    /// Complete latest direct-child snapshot. Closed task tickets remain visible
    /// so external closure cannot stand in for integration evidence.
    #[serde(default)]
    pub map_children: Option<Vec<ChildTicket>>,
    /// Compatibility snapshot written by the first issue-15 implementation.
    #[serde(default)]
    pub open_children: Vec<ChildTicket>,
    #[serde(default)]
    pub final_review: Option<ReviewSummary>,
    #[serde(default)]
    pub final_checks: Vec<CheckEvidence>,
    #[serde(default)]
    pub feature_checks: Vec<CheckEvidence>,
    #[serde(default)]
    pub draft_pr: Option<PullRequest>,
    #[serde(default)]
    pub ready_commit: Option<String>,
    /// A readiness mutation that has not yet been confirmed at GitHub.
    #[serde(default)]
    pub readiness_intent: Option<ReadinessIntent>,
    #[serde(default)]
    pub merged_commit: Option<String>,
    #[serde(default)]
    pub pr_base_commit: Option<String>,
    #[serde(default)]
    pub final_review_commit: Option<String>,
    #[serde(default)]
    pub handoff_error: Option<String>,
    #[serde(default)]
    pub ready_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TicketDelivery {
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub url: String,
    pub reviewed_commit: Option<String>,
    pub candidate_commit: Option<String>,
    #[serde(default)]
    pub candidate_base: Option<String>,
    pub integrated_commit: Option<String>,
    pub integration_worktree: Option<PathBuf>,
    pub issue: u64,
    pub last_error: Option<String>,
    #[serde(default)]
    pub conflict_base: Option<String>,
    /// Exact candidate whose required checks failed; review and diagnostics
    /// remain attached to this record through the bounded repair lifecycle.
    #[serde(default)]
    pub check_failure_commit: Option<String>,
    #[serde(default)]
    pub superseded_by: Option<String>,
    #[serde(default)]
    pub superseded_reason: Option<String>,
    #[serde(default)]
    pub review: Option<ReviewSummary>,
    #[serde(default)]
    pub checks: Vec<CheckEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct ReviewSummary {
    pub commit: String,
    pub summary: String,
    pub unresolved_findings: Vec<String>,
    pub known_limitations: Vec<String>,
}

/// Correlation fields required for a complete-feature review to authorize PR
/// readiness. Ticket reviewers may omit this block; final-feature reviewers may not.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FinalFeatureReviewScope {
    pub scope: String,
    pub map: String,
    pub base_ref: String,
    pub base_commit: String,
    pub reviewed_commit: String,
    /// Canonical linked spec URL, or `none-linked` when the map has no spec.
    pub spec: String,
    pub accepted_decisions_reviewed: bool,
}

impl FinalFeatureReviewScope {
    pub fn matches(&self, expected_map: &str, expected_base: &str, expected_commit: &str) -> bool {
        self.scope == "complete_feature"
            && self.map == expected_map
            && self.base_ref == "origin/develop"
            && self.base_commit == expected_base
            && self.reviewed_commit == expected_commit
            && (self.spec == "none-linked" || self.spec.starts_with("https://github.com/"))
            && self.accepted_decisions_reviewed
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CheckEvidence {
    pub commit: String,
    pub command: String,
    pub result: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReadinessIntent {
    pub pr_number: u64,
    pub commit: String,
    pub base: String,
    pub action: ReadinessAction,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessAction {
    Ready,
    Draft,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadinessResolution {
    Ready,
    Draft,
}

struct ObservedPullRequest {
    pull_request: PullRequest,
    head_oid: String,
    base_oid: String,
    merged: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchivedReview {
    format_version: u32,
    run_id: String,
    ticket: u64,
    role: String,
    status: String,
    summary: String,
    reviewed_commit: String,
    verdict: String,
    unresolved_findings: Option<Vec<String>>,
    known_limitations: Option<Vec<String>>,
    #[serde(default)]
    final_feature_review: Option<FinalFeatureReviewScope>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullRequest {
    pub number: u64,
    pub url: String,
    pub head: String,
    pub base: String,
    pub draft: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Nothing,
    Integrated {
        run_id: String,
        commit: String,
    },
    ReviewRenewal {
        run_id: String,
        commit: String,
    },
    FinalReviewNeeded {
        run_id: String,
        commit: String,
    },
    FeatureReady {
        run_id: String,
        commit: String,
    },
    Conflict {
        run_id: String,
        base_commit: String,
        detail: String,
    },
    CheckFailure {
        run_id: String,
        commit: String,
        detail: String,
    },
    Held {
        run_id: String,
        detail: String,
    },
}

pub fn read(dir: &Path) -> Result<DeliveryState> {
    let path = dir.join("delivery.json");
    if !path.exists() {
        return Ok(DeliveryState::default());
    }
    let bytes = fs::read(&path)?;
    serde_json::from_slice(&bytes).context("decode delivery state")
}

/// Retain the exact failed-check ancestry once its bounded implementer repair
/// has been durably queued. The reviewed commit and approval remain archived.
pub fn record_check_failure_rework(
    dir: &Path,
    source_run: &str,
    rework_run: &str,
    commit: &str,
) -> Result<()> {
    let _lock = Lock::acquire(&dir.join("integration.lock"))?;
    let mut delivery = read(dir)?;
    let entry = delivery
        .tickets
        .get_mut(source_run)
        .context("required-check failure delivery record disappeared")?;
    ensure!(
        entry.check_failure_commit.as_deref() == Some(commit),
        "required-check rework does not match the retained failed candidate"
    );
    if entry.superseded_by.is_none() {
        entry.superseded_by = Some(rework_run.to_owned());
        entry.superseded_reason = Some(format!(
            "required checks failed on exact candidate {commit}; bounded rework {} was queued",
            rework_run
        ));
    }
    save(dir, &delivery)
}

fn archived_review(run: &crate::store::WorkerRun, expected_commit: &str) -> Result<ArchivedReview> {
    ensure!(
        run.role == "reviewer" && run.status == WorkerStatus::Completed,
        "review gate requires a completed reviewer run"
    );
    let path = run
        .result_evidence
        .as_deref()
        .context("review has no retained evidence")?;
    let review: ArchivedReview =
        serde_json::from_slice(&fs::read(path)?).context("decode retained reviewer evidence")?;
    ensure!(
        review.format_version == 1
            && review.run_id == run.id
            && review.ticket == run.ticket
            && review.role == "reviewer"
            && review.status == "completed"
            && !review.summary.trim().is_empty(),
        "review evidence identity or required summary does not match the completed reviewer run"
    );
    ensure!(
        review.reviewed_commit == expected_commit
            && run.base_commit.as_deref() == Some(expected_commit),
        "review evidence is stale for the expected exact commit"
    );
    ensure!(
        review.unresolved_findings.is_some() && review.known_limitations.is_some(),
        "review evidence omitted required unresolved_findings or known_limitations"
    );
    Ok(review)
}

pub fn final_feature_review_scope_matches(
    run: &crate::store::WorkerRun,
    expected_map: &str,
    expected_base: &str,
    expected_commit: &str,
) -> bool {
    if !run.is_final_feature_review()
        || run.role != "reviewer"
        || run.status != WorkerStatus::Completed
        || run.base_commit.as_deref() != Some(expected_commit)
    {
        return false;
    }
    let Ok(review) = archived_review(run, expected_commit) else {
        return false;
    };
    let Some(scope) = review.final_feature_review else {
        return false;
    };
    let observed_base = git(&run.worktree, &["rev-parse", "refs/remotes/origin/develop"])
        .ok()
        .map(|value| value.trim().to_owned());
    scope.matches(expected_map, expected_base, expected_commit)
        && observed_base.as_deref() == Some(expected_base)
}

fn approved_review(run: &crate::store::WorkerRun, expected_commit: &str) -> Result<ReviewSummary> {
    let review = archived_review(run, expected_commit)?;
    ensure!(
        review.verdict == "approved",
        "review verdict is not approved"
    );
    let findings = review.unresolved_findings.unwrap_or_default();
    ensure!(
        findings.is_empty(),
        "review approved while unresolved required findings remain"
    );
    Ok(ReviewSummary {
        commit: expected_commit.to_owned(),
        summary: review.summary,
        unresolved_findings: findings,
        known_limitations: review.known_limitations.unwrap_or_default(),
    })
}

/// Human-readable, read-only ticket and handoff milestones for the orchestrating chat.
/// Scheduler decisions are supplied separately as typed notices so they have one
/// actionable message instead of a duplicate summary here.
pub fn chat_milestones(dir: &Path) -> Result<Vec<String>> {
    let state = read(dir)?;
    let mut milestones = Vec::new();
    for ticket in state.tickets.values() {
        let child = state
            .open_children
            .iter()
            .find(|child| child.number == ticket.issue);
        let title = if !ticket.title.is_empty() {
            ticket.title.clone()
        } else if let Some(child) = child {
            child.title.clone()
        } else {
            "Wayfinder ticket with missing title metadata".to_owned()
        };
        let link = if !ticket.url.is_empty() {
            ticket.url.clone()
        } else if let Some(child) = child {
            child.url.clone()
        } else {
            format!(
                "https://github.com/{}/issues/{}",
                state.repository.as_deref().unwrap_or_default(),
                ticket.issue
            )
        };
        if let Some(commit) = ticket.integrated_commit.as_deref() {
            milestones.push(format!(
                "Integrated [{title}]({link}) at commit {}.",
                short_commit(commit)
            ));
        } else if let Some(commit) = ticket.candidate_commit.as_deref() {
            milestones.push(format!(
                "[{title}]({link}) is waiting for renewed review of {}.",
                short_commit(commit)
            ));
        } else if let Some(error) = ticket.last_error.as_deref() {
            milestones.push(format!("Delivery for [{title}]({link}) is held: {}", error));
        }
    }
    let children = state
        .map_children
        .as_deref()
        .unwrap_or(&state.open_children);
    for child in children.iter().filter(|child| child.state == "open") {
        let link = format!("[{}]({})", child.title, child.url);
        let is_task = child.labels.iter().any(|label| label == "wayfinder:task");
        let integrated = is_task
            && state
                .tickets
                .values()
                .any(|entry| entry.issue == child.number && entry.integrated_commit.is_some());
        if integrated {
            milestones.push(format!(
                "{link} is integrated and awaiting orchestrator closure."
            ));
        } else {
            let kind = if is_task {
                "implementation"
            } else {
                "map decision or research"
            };
            milestones.push(format!("Open {kind} work remains: {link}."));
        }
    }
    if dir.join("state.json").exists() {
        let runtime = store::read_state(dir)?;
        for run in
            runtime.workers.runs.iter().filter(|run| {
                run.status == WorkerStatus::NeedsHuman && run.human_request_id.is_some()
            })
        {
            let title_url = children
                .iter()
                .find(|child| child.number == run.ticket)
                .map(|child| (child.title.as_str(), child.url.as_str()))
                .or_else(|| {
                    state.tickets.values().find_map(|ticket| {
                        (ticket.issue == run.ticket && !ticket.title.is_empty())
                            .then_some((ticket.title.as_str(), ticket.url.as_str()))
                    })
                });
            if let Some((title, url)) = title_url {
                milestones.push(format!("A human decision is pending for [{title}]({url})."));
            }
        }
    }
    if let Some(pr) = state.draft_pr {
        if let Some(intent) = state.readiness_intent.as_ref() {
            match intent.action {
                ReadinessAction::Ready => milestones.push(format!(
                    "Feature PR #{} readiness request for commit {} is pending remote verification and is not locally considered ready: {} ({})",
                    intent.pr_number, intent.commit, intent.reason, pr.url
                )),
                ReadinessAction::Draft => milestones.push(format!(
                    "Feature PR #{} may still be ready; draft invalidation for reviewed commit {} is pending remote verification: {} ({})",
                    intent.pr_number, intent.commit, intent.reason, pr.url
                )),
            }
        } else {
            milestones.push(format!(
                "Feature PR #{} is {}: {}",
                pr.number,
                if pr.draft {
                    "draft"
                } else {
                    "ready for human review"
                },
                pr.url
            ));
        }
    }
    if let Some(commit) = state.ready_commit {
        milestones.push(format!(
            "Feature review approved commit {}; PR may be ready for human review.",
            commit
        ));
    }
    if let Some(commit) = state.merged_commit {
        milestones.push(format!("Human feature merge observed at {}; eligible clean preserved worktrees are being removed.", commit));
    }
    if let Some(error) = state.handoff_error {
        milestones.push(format!("Feature PR handoff is pending: {error}"));
    }
    if let Some(error) = state.ready_error {
        if state.readiness_intent.is_some() {
            milestones.push(format!(
                "Feature PR readiness is not locally verified; human action may be needed: {error}"
            ));
        } else {
            milestones.push(format!("Feature PR remains draft: {error}"));
        }
    }
    Ok(milestones)
}

pub fn record_handoff_error(dir: &Path, error: &str) -> Result<()> {
    let mut state = read(dir)?;
    state.handoff_error = Some(error.to_owned());
    save(dir, &state)
}

pub fn record_ready_error(dir: &Path, error: &str) -> Result<()> {
    let mut state = read(dir)?;
    state.ready_error = Some(error.to_owned());
    save(dir, &state)
}

fn observe_pr(gh: &Path, repository: &str, number: u64) -> Result<ObservedPullRequest> {
    let expected_number = number;
    let number = number.to_string();
    let viewed = gh_json(
        gh,
        &[
            "pr",
            "view",
            &number,
            "--repo",
            repository,
            "--json",
            "isDraft,headRefOid,baseRefOid,url,number,headRefName,baseRefName,mergedAt",
        ],
    )?;
    let pull_request = parse_pr(&viewed)?;
    ensure!(
        pull_request.number == expected_number,
        "GitHub PR view returned a different pull request"
    );
    Ok(ObservedPullRequest {
        pull_request,
        head_oid: viewed["headRefOid"]
            .as_str()
            .context("GitHub PR view omitted head commit")?
            .to_owned(),
        base_oid: viewed["baseRefOid"]
            .as_str()
            .context("GitHub PR view omitted base commit")?
            .to_owned(),
        merged: viewed["mergedAt"].as_str().is_some(),
    })
}

fn save_pending_error(dir: &Path, delivery: &mut DeliveryState, detail: &str) -> Result<()> {
    delivery.ready_commit = None;
    delivery.ready_error = Some(format!(
        "PR readiness is not locally verified; remote transition is pending reconciliation: {detail}"
    ));
    save(dir, delivery)
}

fn persist_draft_intent(
    dir: &Path,
    delivery: &mut DeliveryState,
    pr_number: u64,
    commit: &str,
    base: &str,
    reason: &str,
) -> Result<()> {
    delivery.readiness_intent = Some(ReadinessIntent {
        pr_number,
        commit: commit.to_owned(),
        base: base.to_owned(),
        action: ReadinessAction::Draft,
        reason: reason.to_owned(),
    });
    delivery.ready_commit = None;
    delivery.final_review_commit = None;
    delivery.ready_error = Some(format!(
        "PR may still be ready; draft invalidation is pending remote verification: {reason}"
    ));
    save(dir, delivery)
}

fn confirm_draft(
    dir: &Path,
    delivery: &mut DeliveryState,
    intent: &ReadinessIntent,
    observed: ObservedPullRequest,
) -> Result<ReadinessResolution> {
    ensure!(
        observed.pull_request.draft && !observed.merged,
        "cannot confirm PR draft state from this GitHub response"
    );
    delivery.draft_pr = Some(observed.pull_request);
    delivery.ready_commit = None;
    delivery.final_review_commit = None;
    delivery.readiness_intent = None;
    delivery.ready_error = Some(format!(
        "PR is confirmed draft; renewed final review is required: {}",
        intent.reason
    ));
    save(dir, delivery)?;
    Ok(ReadinessResolution::Draft)
}

fn confirm_ready(
    dir: &Path,
    delivery: &mut DeliveryState,
    intent: &ReadinessIntent,
    observed: ObservedPullRequest,
) -> Result<ReadinessResolution> {
    ensure!(
        !observed.pull_request.draft
            && !observed.merged
            && observed.head_oid == intent.commit
            && observed.base_oid == intent.base,
        "GitHub PR state does not match the persisted readiness intent"
    );
    delivery.draft_pr = Some(observed.pull_request);
    delivery.ready_commit = Some(intent.commit.clone());
    delivery.final_review_commit = Some(intent.commit.clone());
    delivery.pr_base_commit = Some(intent.base.clone());
    delivery.readiness_intent = None;
    delivery.ready_error = None;
    delivery.handoff_error = None;
    save(dir, delivery)?;
    Ok(ReadinessResolution::Ready)
}

/// Reconcile an already-persisted readiness effect. A local ready marker is
/// written only after GitHub confirms the exact commit, base, and draft state.
fn reconcile_readiness_intent(
    dir: &Path,
    delivery: &mut DeliveryState,
    gh: &Path,
    repository: &str,
) -> Result<Option<ReadinessResolution>> {
    let Some(mut intent) = delivery.readiness_intent.clone() else {
        return Ok(None);
    };
    loop {
        let observed = match observe_pr(gh, repository, intent.pr_number) {
            Ok(observed) => observed,
            Err(error) => {
                if intent.action == ReadinessAction::Ready {
                    let reason = format!(
                        "PR state could not be read while a readiness effect may have taken place: {error:#}"
                    );
                    persist_draft_intent(
                        dir,
                        delivery,
                        intent.pr_number,
                        &intent.commit,
                        &intent.base,
                        &reason,
                    )?;
                    intent = delivery.readiness_intent.clone().unwrap();
                    continue;
                }
                save_pending_error(dir, delivery, &format!("could not inspect PR: {error:#}"))?;
                bail!("PR readiness transition remains pending: {error:#}");
            }
        };
        match intent.action {
            ReadinessAction::Draft => {
                if observed.merged {
                    let detail = format!(
                        "PR {} was merged before draft invalidation could be confirmed; human reconciliation is required",
                        intent.pr_number
                    );
                    save_pending_error(dir, delivery, &detail)?;
                    bail!("{detail}");
                }
                if observed.pull_request.draft {
                    return confirm_draft(dir, delivery, &intent, observed).map(Some);
                }
                let number = intent.pr_number.to_string();
                let mutation = gh_text(
                    gh,
                    &["pr", "ready", &number, "--undo", "--repo", repository],
                );
                let after = match observe_pr(gh, repository, intent.pr_number) {
                    Ok(observed) => observed,
                    Err(error) => {
                        save_pending_error(
                            dir,
                            delivery,
                            &format!(
                                "draft mutation outcome is unverified ({}); follow-up PR read failed: {error:#}",
                                mutation
                                    .as_ref()
                                    .map(|_| "request succeeded")
                                    .unwrap_or("request failed")
                            ),
                        )?;
                        bail!("PR draft invalidation remains pending: {error:#}");
                    }
                };
                if after.pull_request.draft && !after.merged {
                    return confirm_draft(dir, delivery, &intent, after).map(Some);
                }
                let mutation_error = mutation
                    .err()
                    .map(|error| format!("draft mutation reported failure: {error:#}; "))
                    .unwrap_or_default();
                let detail = format!(
                    "{mutation_error}GitHub still reports PR {} as ready; draft invalidation remains pending",
                    intent.pr_number
                );
                save_pending_error(dir, delivery, &detail)?;
                bail!("{detail}");
            }
            ReadinessAction::Ready => {
                if observed.merged {
                    let reason = "PR was merged while readiness was being reconciled; human reconciliation is required";
                    persist_draft_intent(
                        dir,
                        delivery,
                        intent.pr_number,
                        &intent.commit,
                        &intent.base,
                        reason,
                    )?;
                    intent = delivery.readiness_intent.clone().unwrap();
                    continue;
                }
                let exact = observed.head_oid == intent.commit && observed.base_oid == intent.base;
                if !observed.pull_request.draft && exact {
                    return confirm_ready(dir, delivery, &intent, observed).map(Some);
                }
                if !observed.pull_request.draft && !exact {
                    let reason = format!(
                        "GitHub reports ready at head {} and base {}, not reviewed head {} and base {}",
                        observed.head_oid, observed.base_oid, intent.commit, intent.base
                    );
                    persist_draft_intent(
                        dir,
                        delivery,
                        intent.pr_number,
                        &intent.commit,
                        &intent.base,
                        &reason,
                    )?;
                    intent = delivery.readiness_intent.clone().unwrap();
                    continue;
                }
                if !exact {
                    let reason = format!(
                        "PR remained draft but its head/base moved from reviewed {}/{} to {}/{}",
                        intent.commit, intent.base, observed.head_oid, observed.base_oid
                    );
                    return confirm_draft(
                        dir,
                        delivery,
                        &ReadinessIntent { reason, ..intent },
                        observed,
                    )
                    .map(Some);
                }

                let number = intent.pr_number.to_string();
                let mutation = gh_text(gh, &["pr", "ready", &number, "--repo", repository]);
                let after = match observe_pr(gh, repository, intent.pr_number) {
                    Ok(observed) => observed,
                    Err(error) => {
                        let reason = format!(
                            "post-ready GitHub state could not be read: {error:#}; original mutation result: {}",
                            mutation.as_ref().map(|_| "success").unwrap_or("failure")
                        );
                        persist_draft_intent(
                            dir,
                            delivery,
                            intent.pr_number,
                            &intent.commit,
                            &intent.base,
                            &reason,
                        )?;
                        intent = delivery.readiness_intent.clone().unwrap();
                        continue;
                    }
                };
                if !after.merged
                    && !after.pull_request.draft
                    && after.head_oid == intent.commit
                    && after.base_oid == intent.base
                {
                    return confirm_ready(dir, delivery, &intent, after).map(Some);
                }
                if after.merged || !after.pull_request.draft {
                    let reason = if after.merged {
                        "PR was merged during readiness verification; human reconciliation is required".to_owned()
                    } else {
                        format!(
                            "post-ready verification observed head/base {}/{} instead of reviewed {}/{}",
                            after.head_oid, after.base_oid, intent.commit, intent.base
                        )
                    };
                    persist_draft_intent(
                        dir,
                        delivery,
                        intent.pr_number,
                        &intent.commit,
                        &intent.base,
                        &reason,
                    )?;
                    intent = delivery.readiness_intent.clone().unwrap();
                    continue;
                }
                let reason = mutation
                    .err()
                    .map(|error| format!("GitHub did not ready the PR: {error:#}"))
                    .unwrap_or_else(|| {
                        "GitHub left the PR in draft after the readiness request".into()
                    });
                return confirm_draft(dir, delivery, &ReadinessIntent { reason, ..intent }, after)
                    .map(Some);
            }
        }
    }
}

fn request_draft_invalidation(
    dir: &Path,
    delivery: &mut DeliveryState,
    gh: &Path,
    repository: &str,
    mut intent: ReadinessIntent,
) -> Result<()> {
    intent.action = ReadinessAction::Draft;
    persist_draft_intent(
        dir,
        delivery,
        intent.pr_number,
        &intent.commit,
        &intent.base,
        &intent.reason,
    )?;
    match reconcile_readiness_intent(dir, delivery, gh, repository)? {
        Some(ReadinessResolution::Draft) => Ok(()),
        Some(ReadinessResolution::Ready) => {
            bail!("draft invalidation unexpectedly resolved as ready")
        }
        None => bail!("draft invalidation intent disappeared before verification"),
    }
}

pub fn mark_ready(
    dir: &Path,
    repository: &Path,
    map: &str,
    workers: &State,
    review_run: &str,
    commit: &str,
) -> Result<()> {
    let gh = std::env::var_os("GH_BIN_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("gh"));
    mark_ready_with(
        dir,
        repository,
        map,
        workers,
        review_run,
        commit,
        required_checks,
        &gh,
    )
}

#[allow(clippy::too_many_arguments)]
fn mark_ready_with(
    dir: &Path,
    repository: &Path,
    map: &str,
    workers: &State,
    review_run: &str,
    commit: &str,
    checks: impl FnOnce(&Path) -> Result<Vec<CheckEvidence>>,
    gh: &Path,
) -> Result<()> {
    let mut delivery = read(dir)?;
    if delivery.readiness_intent.is_some() {
        let map_ref = MapRef::parse(map)?;
        let repository_name = format!("{}/{}", map_ref.owner, map_ref.repository);
        match reconcile_readiness_intent(dir, &mut delivery, gh, &repository_name)? {
            Some(ReadinessResolution::Ready) => return Ok(()),
            Some(ReadinessResolution::Draft) => {
                bail!(
                    "a prior PR readiness transition was reconciled to draft; obtain renewed final review before retrying readiness"
                )
            }
            None => bail!("persisted PR readiness intent disappeared during reconciliation"),
        }
    }
    let children = delivery
        .map_children
        .as_deref()
        .context("PR readiness requires a current complete map-child snapshot")?;
    ensure!(
        all_children_resolved(children, &delivery),
        "feature PR cannot be ready while a map child is unresolved or any implementation child lacks verified integration evidence"
    );
    ensure!(
        delivery
            .tickets
            .values()
            .all(|ticket| ticket.superseded_by.is_some() || ticket.integrated_commit.is_some()),
        "feature PR cannot be ready while ticket integrations remain"
    );
    ensure!(
        !delivery.tickets.is_empty(),
        "feature PR cannot be ready without integrated ticket evidence"
    );
    ensure!(
        workers
            .scheduler_decisions
            .iter()
            .all(|decision| !decision.awaits_human_action()),
        "feature PR cannot be ready while a scheduler decision awaits an explicit human action"
    );
    let reviewer = workers
        .workers
        .runs
        .iter()
        .find(|run| run.id == review_run)
        .context("final reviewer run disappeared")?;
    let review_source = crate::store::implementation_source(&workers.workers.runs, reviewer)
        .context("final reviewer source run disappeared")?;
    ensure!(
        reviewer.role == "reviewer"
            && review_source.role == "implementer"
            && review_source.ticket == reviewer.ticket
            && reviewer.is_final_feature_review(),
        "PR readiness requires the independent final feature reviewer"
    );
    ensure!(
        reviewer.base_commit.as_deref() == Some(commit)
            && reviewer.status == WorkerStatus::Completed,
        "final reviewer did not complete against the current feature commit"
    );
    let review_base = delivery
        .pr_base_commit
        .as_deref()
        .context("final feature review has no recorded develop base")?;
    ensure!(
        final_feature_review_scope_matches(reviewer, map, review_base, commit),
        "final review evidence does not correlate a complete-feature scope, map, accepted decisions, origin/develop base, and pinned commit"
    );
    let review_summary = approved_review(reviewer, commit)?;
    ensure!(
        git(&reviewer.worktree, &["rev-parse", "HEAD"])?.trim() == commit,
        "final reviewer checkout moved away from reviewed feature commit"
    );
    ensure!(
        git(&reviewer.worktree, &["status", "--porcelain"])?
            .trim()
            .is_empty(),
        "final reviewer checkout contains edits"
    );
    let branch = delivery
        .feature_branch
        .as_deref()
        .context("feature branch is not recorded")?;
    let reference = format!("refs/heads/{branch}");
    ensure!(
        git(repository, &["rev-parse", &reference])?.trim() == commit,
        "feature branch changed after final review"
    );
    let final_checks = checks(&reviewer.worktree)?;
    run(repository, "git", &["fetch", "origin", "develop"])?;
    let base = git(repository, &["rev-parse", "refs/remotes/origin/develop"])?
        .trim()
        .to_owned();
    ensure!(
        is_ancestor(repository, &base, commit)?,
        "feature branch must include the current develop target before PR readiness"
    );
    if let Some(previous) = delivery.pr_base_commit.as_deref() {
        ensure!(
            previous == base,
            "develop changed since the draft PR base was recorded; update the feature branch and obtain a renewed final review before marking ready"
        );
    }
    let pr = delivery
        .draft_pr
        .as_ref()
        .context("draft feature PR has not been created")?;
    let map_ref = MapRef::parse(map)?;
    let repo = format!("{}/{}", map_ref.owner, map_ref.repository);
    let intent = ReadinessIntent {
        pr_number: pr.number,
        commit: commit.to_owned(),
        base: base.clone(),
        action: ReadinessAction::Ready,
        reason: "final feature review and checks passed for this exact head and base".into(),
    };
    delivery.readiness_intent = Some(intent);
    delivery.ready_commit = None;
    delivery.final_review_commit = Some(commit.to_owned());
    delivery.final_review = Some(review_summary);
    delivery.final_checks = final_checks.clone();
    delivery.feature_checks = final_checks;
    delivery.pr_base_commit = Some(base);
    delivery.ready_error = Some(
        "PR readiness mutation is pending remote verification; it is not yet locally considered ready".into(),
    );
    save(dir, &delivery)?;
    match reconcile_readiness_intent(dir, &mut delivery, gh, &repo)? {
        Some(ReadinessResolution::Ready) => Ok(()),
        Some(ReadinessResolution::Draft) => {
            bail!(
                "GitHub did not confirm readiness for the reviewed commit; the PR is confirmed draft"
            )
        }
        None => bail!("readiness intent disappeared before GitHub verification"),
    }
}

pub fn reconcile(dir: &Path, state: &State, children: &[ChildTicket]) -> Result<Outcome> {
    let _lock = Lock::acquire(&dir.join("integration.lock"))?;
    let mut delivery = read(dir)?;
    delivery.map_children = Some(children.to_vec());
    delivery.open_children = children
        .iter()
        .filter(|child| child.state == "open")
        .cloned()
        .collect();
    save(dir, &delivery)?;
    let delivery_snapshot = read(dir)?;
    let Some(run) = state.workers.runs.iter().rev().find(|run| {
        if run.role != "implementer" || run.status != WorkerStatus::Reviewed {
            return false;
        }
        let Some(record) = delivery_snapshot.tickets.get(&run.id) else {
            return true;
        };
        if record.superseded_by.is_some() {
            return false;
        }
        record
            .check_failure_commit
            .as_deref()
            .is_none_or(|failed_commit| {
                !state.workers.runs.iter().any(|child| {
                    child.role == "implementer"
                        && child.ticket == run.ticket
                        && child.source_run.as_deref() == Some(&run.id)
                        && child.base_commit.as_deref() == Some(failed_commit)
                })
            })
    }) else {
        return Ok(Outcome::Nothing);
    };
    let Some(reviewed_commit) = run.result_commit.as_deref() else {
        return Ok(Outcome::Held {
            run_id: run.id.clone(),
            detail: "implementation result has no commit".into(),
        });
    };
    let mut delivery = read(dir)?;
    if delivery.repository.is_none() {
        delivery.repository = Some(state.map.split('#').next().unwrap_or_default().to_owned());
    }
    supersede_delivery_ancestors(state, &mut delivery, run);
    let entry = delivery
        .tickets
        .entry(run.id.clone())
        .or_insert_with(|| TicketDelivery {
            reviewed_commit: Some(reviewed_commit.to_owned()),
            issue: run.ticket,
            title: ticket_title(state, run.ticket),
            url: format!(
                "https://github.com/{}/issues/{}",
                state.map.split('#').next().unwrap_or_default(),
                run.ticket
            ),
            ..TicketDelivery::default()
        });
    ensure!(
        entry.reviewed_commit.as_deref() == Some(reviewed_commit),
        "reviewed commit changed after delivery was recorded"
    );
    if entry.integrated_commit.is_some() {
        return feature_review_outcome(state, &delivery, run, children);
    }
    let expected_review = entry.candidate_commit.as_deref().unwrap_or(reviewed_commit);
    let reviewer = state.workers.runs.iter().rev().find(|candidate| {
        candidate.role == "reviewer"
            && candidate.ticket == run.ticket
            && crate::store::implementation_source(&state.workers.runs, candidate)
                .is_some_and(|implementation| implementation.id == run.id)
            && candidate.status == WorkerStatus::Completed
            && candidate.base_commit.as_deref() == Some(expected_review)
    });
    let Some(reviewer) = reviewer else {
        return Ok(Outcome::Held {
            run_id: run.id.clone(),
            detail: format!("independent approval for {expected_review} is missing"),
        });
    };
    let review = archived_review(reviewer, expected_review)?;
    let findings = review.unresolved_findings.as_deref().unwrap_or_default();
    if review.verdict != "approved" || !findings.is_empty() {
        let findings_text = if findings.is_empty() {
            "none recorded".to_owned()
        } else {
            findings.join("; ")
        };
        return Ok(Outcome::Held {
            run_id: run.id.clone(),
            detail: format!(
                "independent review {} did not approve candidate {expected_review}; unresolved findings: {}",
                review.verdict, findings_text
            ),
        });
    }
    entry.review = Some(approved_review(reviewer, expected_review)?);
    let repository =
        fs::canonicalize(&state.binding.repository).context("resolve bound repository")?;
    let branch = match &delivery.feature_branch {
        Some(branch) => branch.clone(),
        None => {
            let current = git(&repository, &["branch", "--show-current"])?;
            let branch = current.trim().to_owned();
            if branch.is_empty() || !branch.starts_with("feature/") {
                let detail =
                    "bound checkout must be on the map feature branch before ticket integration";
                entry.last_error = Some(detail.into());
                save(dir, &delivery)?;
                return Ok(Outcome::Held {
                    run_id: run.id.clone(),
                    detail: detail.into(),
                });
            }
            delivery.feature_branch = Some(branch.clone());
            branch
        }
    };
    validate_branch(&branch)?;
    let reference = format!("refs/heads/{branch}");
    let target = git(&repository, &["rev-parse", &reference])?
        .trim()
        .to_owned();
    if entry.conflict_base.as_deref() == Some(&target) {
        return Ok(Outcome::Conflict {
            run_id: run.id.clone(),
            base_commit: target,
            detail: entry
                .last_error
                .clone()
                .unwrap_or_else(|| "previous rebase conflict remains unresolved".into()),
        });
    }
    let base = if let Some(base) = run.base_commit.as_deref() {
        base.to_owned()
    } else {
        match git(&repository, &["merge-base", &target, reviewed_commit]) {
            Ok(base) if !base.trim().is_empty() => {
                let base = base.trim().to_owned();
                entry.candidate_base = Some(base.clone());
                base
            }
            Ok(_) => {
                let detail = format!(
                    "implementation run omitted its base and feature target {target} has no common ancestor with reviewed candidate {reviewed_commit}"
                );
                entry.last_error = Some(detail.clone());
                save(dir, &delivery)?;
                return Ok(Outcome::Held {
                    run_id: run.id.clone(),
                    detail,
                });
            }
            Err(error) => {
                let detail = format!(
                    "could not derive the omitted implementation base from the feature target and reviewed candidate: {error:#}"
                );
                entry.last_error = Some(detail.clone());
                save(dir, &delivery)?;
                return Ok(Outcome::Held {
                    run_id: run.id.clone(),
                    detail,
                });
            }
        }
    };
    let mut candidate = entry.candidate_commit.clone();
    let mut check_path = run.worktree.clone();
    let needs_rebase = match candidate.as_deref() {
        Some(commit) => !is_ancestor(&repository, &target, commit)?,
        None => !is_ancestor(&repository, &target, reviewed_commit)?,
    };
    if needs_rebase {
        let path = dir.join("integration-worktrees").join(run.id.as_str());
        if path.exists() {
            ensure!(
                validate_worktree(&repository, &path).is_ok(),
                "integration worktree path exists but is not the recorded owned checkout; preserving it"
            );
        } else {
            fs::create_dir_all(
                path.parent()
                    .context("integration worktree has no parent")?,
            )?;
            git(
                &repository,
                &[
                    "worktree",
                    "add",
                    "--detach",
                    path.to_str()
                        .context("non-UTF8 integration worktree path")?,
                    reviewed_commit,
                ],
            )?;
        }
        entry.integration_worktree = Some(path.clone());
        let old_base = entry.candidate_base.as_deref().unwrap_or(&base);
        let old_commit = entry.candidate_commit.as_deref().unwrap_or(reviewed_commit);
        let checkout = if git(&path, &["rev-parse", "HEAD"]).is_ok() {
            git_result(&path, &["checkout", "--detach", old_commit])
        } else {
            bail!("integration worktree is unavailable")
        };
        if let Err(error) = checkout {
            entry.last_error = Some(format!("could not select owned candidate: {error:#}"));
            save(dir, &delivery)?;
            return Ok(Outcome::Held {
                run_id: run.id.clone(),
                detail: format!("integration checkout could not be reconciled: {error:#}"),
            });
        }
        let rebase = git_result(&path, &["rebase", "--onto", &target, old_base]);
        match rebase {
            Ok(_) => {
                let rebased = git(&path, &["rev-parse", "HEAD"])?.trim().to_owned();
                ensure!(
                    is_ancestor(&repository, &target, &rebased)?,
                    "rebased candidate is not a descendant of the current feature head"
                );
                entry.candidate_commit = Some(rebased.clone());
                entry.candidate_base = Some(target.clone());
                entry.last_error = None;
                save(dir, &delivery)?;
                return Ok(Outcome::ReviewRenewal {
                    run_id: run.id.clone(),
                    commit: rebased,
                });
            }
            Err(error) => {
                entry.last_error = Some(format!("{error:#}"));
                entry.conflict_base = Some(target.clone());
                save(dir, &delivery)?;
                return Ok(Outcome::Conflict {
                    run_id: run.id.clone(),
                    base_commit: target,
                    detail: format!("rebase conflict retained at {}: {error:#}", path.display()),
                });
            }
        }
    }
    if let Some(candidate_commit) = candidate.as_ref() {
        let latest_review = state.workers.runs.iter().rev().find(|candidate_run| {
            candidate_run.role == "reviewer"
                && crate::store::implementation_source(&state.workers.runs, candidate_run)
                    .is_some_and(|implementation| implementation.id == run.id)
                && candidate_run.status == WorkerStatus::Completed
                && candidate_run.base_commit.as_deref() == Some(candidate_commit)
        });
        if latest_review.is_none() {
            return Ok(Outcome::Held {
                run_id: run.id.clone(),
                detail: format!(
                    "rebased commit {candidate_commit} requires a renewed independent review"
                ),
            });
        }
        let path = entry
            .integration_worktree
            .as_ref()
            .context("rebased integration worktree was not retained")?;
        check_path = path.clone();
        if target != git(&repository, &["rev-parse", &reference])?.trim() {
            return Ok(Outcome::Held {
                run_id: run.id.clone(),
                detail: "feature target moved while renewed review was pending; reconcile again"
                    .into(),
            });
        }
    } else {
        candidate = Some(reviewed_commit.to_owned());
    }
    let candidate = candidate.context("integration candidate missing")?;
    ensure!(
        git(&check_path, &["rev-parse", "HEAD"])?.trim() == candidate,
        "integration checkout no longer matches reviewed candidate"
    );
    entry.checks.clear();
    let checks = match required_checks(&check_path) {
        Ok(checks) => checks,
        Err(error)
            if matches!(
                error.downcast_ref::<RequiredCheckError>(),
                Some(RequiredCheckError::Launch { .. })
            ) =>
        {
            entry.last_error = Some(format!("required check could not be launched: {error}"));
            save(dir, &delivery)?;
            return Ok(Outcome::Held {
                run_id: run.id.clone(),
                detail: format!(
                    "required-check environment could not start the command; candidate and evidence retained: {error}"
                ),
            });
        }
        Err(error)
            if matches!(
                error.downcast_ref::<RequiredCheckError>(),
                Some(RequiredCheckError::Failed { .. })
            ) =>
        {
            entry.last_error = Some(format!("required checks failed: {error:#}"));
            entry.check_failure_commit = Some(candidate.clone());
            save(dir, &delivery)?;
            return Ok(Outcome::CheckFailure {
                run_id: run.id.clone(),
                commit: candidate,
                detail: format!("required checks failed: {error:#}"),
            });
        }
        Err(error) => return Err(error),
    };
    entry.checks = checks;
    ensure!(
        git(&check_path, &["status", "--porcelain"])?
            .trim()
            .is_empty(),
        "integration checkout became dirty during required checks; preserving it"
    );
    let latest = git(&repository, &["rev-parse", &reference])?
        .trim()
        .to_owned();
    ensure!(
        latest == target,
        "feature branch target changed during integration; retry against the new target"
    );
    ensure!(
        is_ancestor(&repository, &target, &candidate)?,
        "candidate cannot fast-forward the feature branch"
    );
    advance_feature_branch(&repository, &reference, &candidate, &target)?;
    entry.integrated_commit = Some(candidate.clone());
    entry.candidate_commit = Some(candidate.clone());
    entry.last_error = None;
    save(dir, &delivery)?;
    Ok(Outcome::Integrated {
        run_id: run.id.clone(),
        commit: candidate,
    })
}

fn ticket_integrated(delivery: &DeliveryState, issue: u64) -> bool {
    delivery.tickets.values().any(|entry| {
        entry.issue == issue && entry.integrated_commit.is_some() && entry.superseded_by.is_none()
    })
}

fn all_children_resolved(children: &[ChildTicket], delivery: &DeliveryState) -> bool {
    children.iter().all(|child| {
        if child.labels.iter().any(|label| label == "wayfinder:task") {
            ticket_integrated(delivery, child.number)
        } else {
            child.state != "open"
        }
    })
}

fn supersedable_ancestors(
    state: &State,
    delivery: &DeliveryState,
    run: &crate::store::WorkerRun,
) -> Vec<(String, String)> {
    let mut supersedable = Vec::new();
    let mut cursor = run.source_run.as_deref();
    let mut visited = std::collections::BTreeSet::new();
    while let Some(source_id) = cursor {
        if !visited.insert(source_id.to_owned()) {
            break;
        }
        let Some(ancestor) = state
            .workers
            .runs
            .iter()
            .find(|candidate| candidate.id == source_id)
        else {
            break;
        };
        if ancestor.role == "implementer" {
            if let Some(record) = delivery.tickets.get(&ancestor.id) {
                if record.integrated_commit.is_none() {
                    if record.conflict_base.as_deref() == run.base_commit.as_deref()
                        && record.conflict_base.is_some()
                    {
                        supersedable.push((
                            ancestor.id.clone(),
                            "integration conflict was resolved in this reviewed repair run".into(),
                        ));
                    } else if record.candidate_commit.as_deref() == run.base_commit.as_deref() {
                        if let Some(reviewer) = state.workers.runs.iter().find(|candidate| {
                            candidate.role == "reviewer"
                                && crate::store::implementation_source(
                                    &state.workers.runs,
                                    candidate,
                                )
                                .is_some_and(|implementation| implementation.id == ancestor.id)
                                && candidate.base_commit.as_deref() == run.base_commit.as_deref()
                                && candidate.status == WorkerStatus::Completed
                        }) {
                            if archived_review(
                                reviewer,
                                run.base_commit.as_deref().unwrap_or_default(),
                            )
                            .is_ok_and(|review| review.verdict == "changes_requested")
                            {
                                supersedable.push((
                                    ancestor.id.clone(),
                                    "reviewer requested changes to this exact candidate; reviewed repair run supersedes it".into(),
                                ));
                            }
                        }
                    }
                }
            }
        }
        cursor = ancestor.source_run.as_deref();
    }
    supersedable
}

fn supersede_delivery_ancestors(
    state: &State,
    delivery: &mut DeliveryState,
    run: &crate::store::WorkerRun,
) {
    for (prior_run, reason) in supersedable_ancestors(state, delivery, run) {
        if let Some(prior) = delivery.tickets.get_mut(&prior_run) {
            if prior.integrated_commit.is_none() && prior.superseded_by.is_none() {
                prior.superseded_by = Some(run.id.clone());
                prior.superseded_reason = Some(reason);
            }
        }
    }
}

fn feature_review_outcome(
    state: &State,
    delivery: &DeliveryState,
    source: &crate::store::WorkerRun,
    children: &[ChildTicket],
) -> Result<Outcome> {
    let final_review_in_flight = state
        .workers
        .runs
        .iter()
        .any(|run| run.is_final_feature_review() && run.reserves_capacity());
    let pending_implementation_or_human_work = state
        .workers
        .runs
        .iter()
        .any(|run| run.reserves_capacity() && !run.is_final_feature_review());
    let pending_scheduler_decision = state
        .scheduler_decisions
        .iter()
        .any(|decision| decision.awaits_human_action());
    let unresolved_children = children.iter().any(|child| {
        if child.labels.iter().any(|label| label == "wayfinder:task") {
            !ticket_integrated(delivery, child.number)
        } else {
            child.state == "open"
        }
    });
    if unresolved_children
        || delivery.tickets.is_empty()
        || delivery
            .tickets
            .values()
            .any(|ticket| ticket.superseded_by.is_none() && ticket.integrated_commit.is_none())
        || pending_implementation_or_human_work
        || pending_scheduler_decision
    {
        return Ok(Outcome::Nothing);
    }
    // The final reviewer is not an implementation loop. Its own queued/running
    // request blocks duplicate scheduling until that exact review completes.
    if final_review_in_flight {
        return Ok(Outcome::Nothing);
    }
    let Some(branch) = delivery.feature_branch.as_deref() else {
        return Ok(Outcome::Nothing);
    };
    let repository = fs::canonicalize(&state.binding.repository)?;
    let head = git(&repository, &["rev-parse", &format!("refs/heads/{branch}")])?
        .trim()
        .to_owned();
    let review_base = delivery.pr_base_commit.as_deref();
    let final_review = state.workers.runs.iter().rev().find(|candidate| {
        candidate.role == "reviewer"
            && candidate.ticket == source.ticket
            && crate::store::implementation_source(&state.workers.runs, candidate)
                .is_some_and(|implementation| implementation.id == source.id)
            && candidate.is_final_feature_review()
            && candidate.base_commit.as_deref() == Some(&head)
            && candidate.status == WorkerStatus::Completed
            && review_base.is_some_and(|base| {
                final_feature_review_scope_matches(candidate, &state.map, base, &head)
            })
    });
    let Some(final_review) = final_review else {
        return Ok(Outcome::FinalReviewNeeded {
            run_id: source.id.clone(),
            commit: head,
        });
    };
    let report = archived_review(final_review, &head)?;
    let findings = report.unresolved_findings.as_deref().unwrap_or_default();
    if report.verdict != "approved" || !findings.is_empty() {
        return Ok(Outcome::Held {
            run_id: final_review.id.clone(),
            detail: format!(
                "final independent review {} did not approve feature commit {head}; unresolved findings: {}",
                report.verdict,
                if findings.is_empty() {
                    "none recorded".to_owned()
                } else {
                    findings.join("; ")
                }
            ),
        });
    }
    ensure!(
        git(&final_review.worktree, &["rev-parse", "HEAD"])?.trim() == head,
        "final reviewer worktree moved away from the reviewed feature head"
    );
    ensure!(
        git(&final_review.worktree, &["status", "--porcelain"])?
            .trim()
            .is_empty(),
        "final reviewer worktree contains edits"
    );
    required_checks(&final_review.worktree)?;
    Ok(Outcome::FeatureReady {
        run_id: final_review.id.clone(),
        commit: head,
    })
}

fn ticket_title(state: &State, ticket: u64) -> String {
    state
        .workers
        .runs
        .iter()
        .find_map(|run| {
            if run.ticket != ticket {
                return None;
            }
            run.context.as_deref()?.lines().find_map(|line| {
                line.strip_prefix("GitHub ticket title: ")
                    .map(str::to_owned)
            })
        })
        .unwrap_or_else(|| format!("Wayfinder issue #{ticket}"))
}

/// Create or refresh the map's draft feature PR once at least one reviewed ticket
/// has been integrated. Repeated reconciliation reads before writing and uses the
/// stable head/base pair to recover an ambiguous create response.
pub fn ensure_draft_pr(
    dir: &Path,
    repository: &Path,
    map: &str,
    workers: &State,
) -> Result<Option<PullRequest>> {
    let gh = std::env::var_os("GH_BIN_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("gh"));
    ensure_draft_pr_with_gh(dir, repository, map, workers, &gh)
}

fn ensure_draft_pr_with_gh(
    dir: &Path,
    repository: &Path,
    map: &str,
    workers: &State,
    gh: &Path,
) -> Result<Option<PullRequest>> {
    let mut delivery = read(dir)?;
    if delivery.readiness_intent.is_some() {
        let map_ref = MapRef::parse(map)?;
        let repository_name = format!("{}/{}", map_ref.owner, map_ref.repository);
        reconcile_readiness_intent(dir, &mut delivery, gh, &repository_name)?;
    }
    if !delivery
        .tickets
        .values()
        .any(|ticket| ticket.integrated_commit.is_some())
    {
        return Ok(None);
    }
    ensure!(
        delivery
            .tickets
            .values()
            .filter(|ticket| ticket.integrated_commit.is_some())
            .all(|ticket| {
                ticket.review.as_ref().is_some_and(|review| {
                    review.commit == ticket.integrated_commit.as_deref().unwrap_or_default()
                        && !review.summary.trim().is_empty()
                        && review.unresolved_findings.is_empty()
                }) && !ticket.checks.is_empty()
                    && ticket.checks.iter().all(|check| {
                        check.commit == ticket.integrated_commit.as_deref().unwrap_or_default()
                            && !check.command.trim().is_empty()
                            && !check.result.trim().is_empty()
                    })
            }),
        "draft PR handoff requires exact-commit ticket review and required-check evidence for every integrated ticket"
    );
    let branch = delivery
        .feature_branch
        .clone()
        .context("integrated ticket has no recorded feature branch")?;
    validate_branch(&branch)?;
    let map_ref = MapRef::parse(map)?;
    let repo = format!("{}/{}", map_ref.owner, map_ref.repository);
    let listed = gh_json(
        gh,
        &[
            "pr",
            "list",
            "--repo",
            &repo,
            "--head",
            &branch,
            "--base",
            "develop",
            "--state",
            "all",
            "--json",
            "number,url,isDraft,headRefName,baseRefName,mergedAt",
        ],
    )?;
    let prs = listed
        .as_array()
        .context("gh pr list response was not an array")?;
    ensure!(
        prs.len() <= 1,
        "multiple feature PRs use the same map branch and base"
    );
    let mut forced_draft = false;
    if let Some(existing) = prs.first() {
        let existing_pr = parse_pr(existing)?;
        if existing["mergedAt"].as_str().is_some() {
            run(repository, "git", &["fetch", "origin", "develop"])?;
            delivery.merged_commit = Some(
                git(repository, &["rev-parse", "refs/remotes/origin/develop"])?
                    .trim()
                    .to_owned(),
            );
            delivery.draft_pr = Some(existing_pr.clone());
            save(dir, &delivery)?;
            cleanup_merged_worktrees(repository, workers, &delivery)?;
            return Ok(Some(existing_pr));
        }
        run(repository, "git", &["fetch", "origin", "develop"])?;
        let observed_base = git(repository, &["rev-parse", "refs/remotes/origin/develop"])?
            .trim()
            .to_owned();
        let observed_head = git(repository, &["rev-parse", &format!("refs/heads/{branch}")])?
            .trim()
            .to_owned();
        if existing_pr.draft
            && let Some(ready_commit) = delivery.ready_commit.clone()
        {
            let ready_base = delivery
                .pr_base_commit
                .clone()
                .unwrap_or_else(|| observed_base.clone());
            request_draft_invalidation(
                dir,
                &mut delivery,
                gh,
                &repo,
                ReadinessIntent {
                    pr_number: existing_pr.number,
                    commit: ready_commit,
                    base: ready_base,
                    action: ReadinessAction::Draft,
                    reason: "GitHub reports the PR is already draft; the previous readiness is no longer active".into(),
                },
            )?;
        }
        let complete_review_present =
            delivery
                .ready_commit
                .as_deref()
                .is_some_and(|ready_commit| {
                    workers.workers.runs.iter().any(|run| {
                        crate::store::implementation_source(&workers.workers.runs, run).is_some()
                            && final_feature_review_scope_matches(
                                run,
                                map,
                                delivery
                                    .pr_base_commit
                                    .as_deref()
                                    .unwrap_or(observed_base.as_str()),
                                ready_commit,
                            )
                    })
                });
        let ready_missing_scope =
            !existing_pr.draft && delivery.ready_commit.is_some() && !complete_review_present;
        let ready_invalid = !existing_pr.draft
            && (delivery.ready_commit.as_deref() != Some(observed_head.as_str())
                || delivery.pr_base_commit.as_deref() != Some(observed_base.as_str())
                || !is_ancestor(repository, &observed_base, &observed_head)?
                || ready_missing_scope);
        let ready_without_verified_evidence = !existing_pr.draft && delivery.ready_commit.is_none();
        let readiness_commit = delivery
            .ready_commit
            .clone()
            .unwrap_or_else(|| observed_head.clone());
        let readiness_base = delivery
            .pr_base_commit
            .clone()
            .unwrap_or_else(|| observed_base.clone());
        if ready_invalid || ready_without_verified_evidence {
            let reason = if ready_without_verified_evidence {
                "GitHub reports a ready PR without a locally verified readiness record".to_owned()
            } else if ready_missing_scope {
                "feature head or develop target changed, or the recorded final review lacks correlated complete-feature scope evidence; renew the review before readiness".to_owned()
            } else {
                "feature head or develop target changed after final review".to_owned()
            };
            request_draft_invalidation(
                dir,
                &mut delivery,
                gh,
                &repo,
                ReadinessIntent {
                    pr_number: existing_pr.number,
                    commit: readiness_commit.clone(),
                    base: readiness_base.clone(),
                    action: ReadinessAction::Draft,
                    reason,
                },
            )?;
            forced_draft = true;
        }
        let (base, target_updated) = sync_develop_target(dir, repository, &branch, &mut delivery)?;
        if target_updated && !existing_pr.draft && !forced_draft {
            request_draft_invalidation(
                dir,
                &mut delivery,
                gh,
                &repo,
                ReadinessIntent {
                    pr_number: existing_pr.number,
                    commit: readiness_commit.clone(),
                    base: readiness_base.clone(),
                    action: ReadinessAction::Draft,
                    reason: "develop advanced after final review".into(),
                },
            )?;
            forced_draft = true;
        }
        delivery.pr_base_commit = Some(base);
        save(dir, &delivery)?;
    } else {
        let (base, _) = sync_develop_target(dir, repository, &branch, &mut delivery)?;
        delivery.pr_base_commit = Some(base);
        save(dir, &delivery)?;
    }
    if !forced_draft
        && let Some(existing) = prs
            .first()
            .filter(|pr| pr["isDraft"].as_bool() == Some(false))
    {
        let expected_commit = delivery
            .ready_commit
            .clone()
            .context("GitHub reports a ready PR without a locally verified commit")?;
        let expected_base = delivery
            .pr_base_commit
            .clone()
            .context("GitHub reports a ready PR without a locally verified base")?;
        let remote_head = remote_ref(repository, "refs/heads", &branch);
        let remote_base = git(repository, &["rev-parse", "refs/remotes/origin/develop"]);
        let target_changed = remote_base
            .as_ref()
            .is_ok_and(|base| base.trim() != expected_base)
            || remote_head
                .as_ref()
                .is_ok_and(|head| head.as_deref() != Some(expected_commit.as_str()));
        if target_changed || remote_head.is_err() || remote_base.is_err() {
            request_draft_invalidation(
                dir,
                &mut delivery,
                gh,
                &repo,
                ReadinessIntent {
                    pr_number: existing["number"].as_u64().context("ready PR omitted number")?,
                    commit: expected_commit,
                    base: expected_base,
                    action: ReadinessAction::Draft,
                    reason: "remote feature head or develop target changed or could not be verified before publishing the branch".into(),
                },
            )?;
            forced_draft = true;
        }
    }
    if let Err(push_error) = run(
        repository,
        "git",
        &["push", "--set-upstream", "origin", &branch],
    ) {
        if let Some(existing) = prs
            .first()
            .filter(|pr| pr["isDraft"].as_bool() == Some(false))
        {
            let fallback_commit = git(repository, &["rev-parse", &format!("refs/heads/{branch}")])
                .unwrap_or_default()
                .trim()
                .to_owned();
            let fallback_base = git(repository, &["rev-parse", "refs/remotes/origin/develop"])
                .unwrap_or_default()
                .trim()
                .to_owned();
            let readiness_commit = delivery.ready_commit.clone().unwrap_or(fallback_commit);
            let readiness_base = delivery.pr_base_commit.clone().unwrap_or(fallback_base);
            request_draft_invalidation(
                dir,
                &mut delivery,
                gh,
                &repo,
                ReadinessIntent {
                    pr_number: existing["number"].as_u64().context("ready PR omitted number")?,
                    commit: readiness_commit,
                    base: readiness_base,
                    action: ReadinessAction::Draft,
                    reason: format!("feature branch push failed while the PR was ready; readiness requires reconciliation: {push_error:#}"),
                },
            )
            .context("could not confirm the ready PR was returned to draft after branch push failure")?;
        }
        return Err(push_error);
    }
    let mut pr = if let Some(pr) = prs.first() {
        let mut parsed = parse_pr(pr)?;
        if forced_draft {
            parsed.draft = true;
        }
        ensure!(
            parsed.head == branch && parsed.base == "develop",
            "existing feature PR target does not match the delivery contract"
        );
        parsed
    } else {
        let body = pr_body(&delivery);
        let body_file = write_pr_body_file(&body)?;
        let output = gh_text(
            gh,
            &[
                "pr",
                "create",
                "--repo",
                &repo,
                "--draft",
                "--base",
                "develop",
                "--head",
                &branch,
                "--title",
                &format!("Wayfinder delivery: {}", map_ref.number),
                "--body-file",
                body_file.path().to_str().context("non-UTF8 PR body path")?,
            ],
        );
        match output {
            Ok(url) => {
                let number = url
                    .trim()
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .and_then(|n| n.parse::<u64>().ok())
                    .context("gh pr create returned no pull request URL")?;
                PullRequest {
                    number,
                    url: url.trim().to_owned(),
                    head: branch.to_owned(),
                    base: "develop".into(),
                    draft: true,
                }
            }
            Err(error) => {
                let after = gh_json(
                    gh,
                    &[
                        "pr",
                        "list",
                        "--repo",
                        &repo,
                        "--head",
                        &branch,
                        "--base",
                        "develop",
                        "--state",
                        "all",
                        "--json",
                        "number,url,isDraft,headRefName,baseRefName",
                    ],
                );
                if let Ok(after) = after {
                    let rows = after
                        .as_array()
                        .context("gh pr list response was not an array")?;
                    if rows.len() == 1 {
                        parse_pr(&rows[0])?
                    } else {
                        return Err(error).context("draft PR creation was ambiguous; retry after listing the feature head/base pair");
                    }
                } else {
                    return Err(error).context(
                        "draft PR creation was ambiguous and the follow-up listing failed",
                    );
                }
            }
        }
    };
    if !pr.draft {
        let base_result = run(repository, "git", &["fetch", "origin", "develop"])
            .and_then(|()| git(repository, &["rev-parse", "refs/remotes/origin/develop"]))
            .map(|base| base.trim().to_owned());
        let head_result = remote_ref(repository, "refs/heads", &branch);
        let target_changed = base_result
            .as_ref()
            .is_ok_and(|base| delivery.pr_base_commit.as_deref() != Some(base.as_str()));
        let head_changed = head_result
            .as_ref()
            .is_ok_and(|head| delivery.ready_commit.as_deref() != head.as_deref());
        if target_changed
            || head_changed
            || delivery.ready_commit.is_none()
            || base_result.is_err()
            || head_result.is_err()
        {
            let reason = match (base_result.as_ref(), head_result.as_ref()) {
                (Err(error), _) => format!(
                    "develop target could not be checked after readiness: {error:#}"
                ),
                (_, Err(error)) => format!(
                    "remote feature head could not be checked after readiness: {error:#}"
                ),
                (Ok(_), Ok(_)) if target_changed => {
                    "develop moved after final review; update the feature branch, rerun checks, and obtain a renewed final review".into()
                }
                _ => "feature branch changed or lacks verified readiness evidence; obtain a renewed final review".into(),
            };
            let readiness_commit = delivery.ready_commit.clone().unwrap_or_else(|| {
                head_result
                    .as_ref()
                    .ok()
                    .and_then(|head| head.clone())
                    .unwrap_or_default()
            });
            let readiness_base = delivery
                .pr_base_commit
                .clone()
                .or_else(|| base_result.as_ref().ok().cloned())
                .unwrap_or_default();
            request_draft_invalidation(
                dir,
                &mut delivery,
                gh,
                &repo,
                ReadinessIntent {
                    pr_number: pr.number,
                    commit: readiness_commit,
                    base: readiness_base,
                    action: ReadinessAction::Draft,
                    reason,
                },
            )?;
            pr.draft = true;
            if let Ok(current_base) = base_result {
                delivery.pr_base_commit = Some(current_base);
            }
        }
    }
    let body = pr_body(&delivery);
    let body_file = write_pr_body_file(&body)?;
    let _ = gh_text(
        gh,
        &[
            "pr",
            "edit",
            &pr.number.to_string(),
            "--repo",
            &repo,
            "--body-file",
            body_file.path().to_str().context("non-UTF8 PR body path")?,
        ],
    )?;
    delivery.draft_pr = Some(pr.clone());
    delivery.handoff_error = None;
    if delivery.pr_base_commit.is_none() {
        delivery.pr_base_commit = remote_ref(repository, "refs/heads", "develop")?;
    }
    save(dir, &delivery)?;
    Ok(Some(pr))
}

fn write_pr_body_file(body: &str) -> Result<tempfile::NamedTempFile> {
    let mut file = tempfile::NamedTempFile::new()?;
    file.write_all(body.as_bytes())?;
    file.as_file().sync_all()?;
    Ok(file)
}

fn remote_ref(repository: &Path, namespace: &str, branch: &str) -> Result<Option<String>> {
    let reference = format!("{namespace}/{branch}");
    let output = Command::new("git")
        .args(["-C"])
        .arg(repository)
        .args(["ls-remote", "origin", &reference])
        .output()
        .context("read remote feature head")?;
    ensure!(
        output.status.success(),
        "git ls-remote failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(String::from_utf8(output.stdout)?
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().next())
        .map(str::to_owned))
}

fn sync_develop_target(
    dir: &Path,
    repository: &Path,
    feature_branch: &str,
    delivery: &mut DeliveryState,
) -> Result<(String, bool)> {
    sync_develop_target_with_checks(dir, repository, feature_branch, delivery, |path| {
        required_checks(path).map(|_| ())
    })
}

fn sync_develop_target_with_checks(
    dir: &Path,
    repository: &Path,
    feature_branch: &str,
    delivery: &mut DeliveryState,
    checks: impl Fn(&Path) -> Result<()>,
) -> Result<(String, bool)> {
    run(repository, "git", &["fetch", "origin", "develop"])?;
    let base = git(repository, &["rev-parse", "refs/remotes/origin/develop"])?
        .trim()
        .to_owned();
    let reference = format!("refs/heads/{feature_branch}");
    let old_head = git(repository, &["rev-parse", &reference])?
        .trim()
        .to_owned();
    let target_updated = delivery
        .pr_base_commit
        .as_deref()
        .is_some_and(|previous| previous != base)
        || !is_ancestor(repository, &base, &old_head)?;
    if !is_ancestor(repository, &base, &old_head)? {
        let suffix = short_commit(&base);
        let path = dir
            .join("integration-worktrees")
            .join(format!("develop-update-{suffix}"));
        if path.exists() {
            bail!(
                "a retained develop update worktree already exists at {}; inspect it before retrying",
                path.display()
            );
        }
        fs::create_dir_all(
            path.parent()
                .context("develop update worktree has no parent")?,
        )?;
        git(
            repository,
            &[
                "worktree",
                "add",
                "--detach",
                path.to_str().context("non-UTF8 develop update path")?,
                &old_head,
            ],
        )?;
        let merge = git_result(&path, &["merge", "--no-edit", &base]);
        if let Err(error) = merge {
            delivery.handoff_error = Some(format!(
                "feature target update conflicted; worktree retained at {}: {error:#}",
                path.display()
            ));
            save(dir, delivery)?;
            bail!(
                "{}",
                delivery
                    .handoff_error
                    .as_deref()
                    .unwrap_or("feature target update conflict")
            );
        }
        let updated = git(&path, &["rev-parse", "HEAD"])?.trim().to_owned();
        ensure!(
            is_ancestor(repository, &base, &updated)?,
            "updated feature head does not contain current develop"
        );
        if let Err(error) = checks(&path) {
            delivery.handoff_error = Some(format!(
                "required checks failed after updating feature against develop; checkout retained at {}: {error:#}",
                path.display()
            ));
            save(dir, delivery)?;
            bail!(
                "{}",
                delivery
                    .handoff_error
                    .as_deref()
                    .unwrap_or("target update checks failed")
            );
        }
        ensure!(
            git(&path, &["status", "--porcelain"])?.trim().is_empty(),
            "develop update worktree became dirty; preserving it"
        );
        update_ref(repository, &reference, &updated, &old_head)?;
        git(
            repository,
            &[
                "worktree",
                "remove",
                path.to_str().context("non-UTF8 develop update path")?,
            ],
        )?;
        delivery.ready_commit = None;
        delivery.final_review_commit = None;
        delivery.ready_error = Some("develop advanced; feature branch updated and final independent review is required again".into());
    }
    delivery.pr_base_commit = Some(base.clone());
    Ok((base, target_updated))
}

fn pr_body(delivery: &DeliveryState) -> String {
    let mut body = String::from(
        "## Summary\n\nThis feature branch contains independently reviewed Wayfinder ticket work. The human decides whether the feature PR is merged into `develop`.\n\n## Evidence\n\n",
    );
    for ticket in delivery.tickets.values() {
        if let Some(commit) = &ticket.integrated_commit {
            let title = if ticket.title.is_empty() {
                format!("Issue #{}", ticket.issue)
            } else {
                ticket.title.clone()
            };
            let url = if ticket.url.is_empty() {
                format!(
                    "https://github.com/{}/issues/{}",
                    delivery.repository.as_deref().unwrap_or_default(),
                    ticket.issue
                )
            } else {
                ticket.url.clone()
            };
            body.push_str(&format!(
                "### [{title}]({url})\n\n- Reviewed commit: `{}`\n- Integrated commit: `{commit}`\n",
                ticket
                    .review
                    .as_ref()
                    .map(|review| review.commit.as_str())
                    .or(ticket.reviewed_commit.as_deref())
                    .unwrap_or("unknown")
            ));
            if let Some(review) = &ticket.review {
                body.push_str(&format!("- Independent review: {}\n", review.summary));
                append_report_list(
                    &mut body,
                    "Unresolved findings",
                    &review.unresolved_findings,
                );
                append_report_list(&mut body, "Known limitations", &review.known_limitations);
            } else {
                body.push_str(
                    "- Independent review report is missing from local delivery evidence.\n",
                );
            }
            body.push_str("- Required checks:\n");
            append_checks(&mut body, &ticket.checks);
            body.push('\n');
        }
    }
    if let Some(review) = &delivery.final_review {
        body.push_str(&format!(
            "### Final feature review at `{}`\n\n{}\n\n",
            review.commit, review.summary
        ));
        append_report_list(
            &mut body,
            "Unresolved findings",
            &review.unresolved_findings,
        );
        append_report_list(&mut body, "Known limitations", &review.known_limitations);
    }
    if !delivery.final_checks.is_empty() {
        body.push_str("### Final required checks\n\n");
        append_checks(&mut body, &delivery.final_checks);
        body.push('\n');
    }
    body.push_str(
        "## Merge Danger\n\n**Door:** Two-way\n\n**Blast Radius:** delivery\n\nThe human controls the feature merge into `develop`; automation does not merge this PR.\n",
    );
    body
}

fn append_report_list(body: &mut String, title: &str, entries: &[String]) {
    body.push_str(&format!("- {title}:\n"));
    if entries.is_empty() {
        body.push_str("  - None reported.\n");
    } else {
        for entry in entries {
            body.push_str(&format!("  - {entry}\n"));
        }
    }
}

fn append_checks(body: &mut String, checks: &[CheckEvidence]) {
    if checks.is_empty() {
        body.push_str("  - No check evidence recorded.\n");
        return;
    }
    for check in checks {
        body.push_str(&format!(
            "  - `{}` at `{}`: {}\n",
            check.command, check.commit, check.result
        ));
    }
}

fn short_commit(commit: &str) -> &str {
    commit.get(..commit.len().min(12)).unwrap_or(commit)
}

fn parse_pr(value: &serde_json::Value) -> Result<PullRequest> {
    Ok(PullRequest {
        number: value["number"].as_u64().context("PR omitted number")?,
        url: value["url"].as_str().context("PR omitted URL")?.to_owned(),
        head: value["headRefName"]
            .as_str()
            .context("PR omitted head branch")?
            .to_owned(),
        base: value["baseRefName"]
            .as_str()
            .context("PR omitted base branch")?
            .to_owned(),
        draft: value["isDraft"]
            .as_bool()
            .context("PR omitted draft state")?,
    })
}

fn gh_json(executable: &Path, args: &[&str]) -> Result<serde_json::Value> {
    let output = Command::new(executable)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("launch GitHub CLI")?;
    ensure!(
        output.status.success(),
        "gh {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    serde_json::from_slice(&output.stdout).context("decode gh JSON response")
}
fn gh_text(executable: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new(executable)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("launch GitHub CLI")?;
    ensure!(
        output.status.success(),
        "gh {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8(output.stdout).context("gh returned non-UTF8 output")
}

fn cleanup_merged_worktrees(
    repository: &Path,
    workers: &State,
    delivery: &DeliveryState,
) -> Result<()> {
    let merged = delivery
        .merged_commit
        .as_deref()
        .context("merge cleanup requires a confirmed merge commit")?;
    for (run_id, ticket) in &delivery.tickets {
        let Some(path) = ticket.integration_worktree.as_ref() else {
            continue;
        };
        if !path.exists() || !validate_worktree(repository, path).is_ok() {
            continue;
        }
        let clean =
            git(path, &["status", "--porcelain"]).is_ok_and(|status| status.trim().is_empty());
        let Some(commit) = ticket.integrated_commit.as_deref() else {
            continue;
        };
        if clean && is_ancestor(repository, commit, merged)? {
            git(
                repository,
                &[
                    "worktree",
                    "remove",
                    path.to_str()
                        .context("non-UTF8 integration worktree path")?,
                ],
            )?;
        }
        let _ = run_id;
    }
    for worker in &workers.workers.runs {
        if !matches!(
            worker.status,
            WorkerStatus::Completed | WorkerStatus::Reviewed
        ) {
            continue;
        }
        let preserved = if worker.role == "implementer" {
            delivery
                .tickets
                .get(&worker.id)
                .and_then(|ticket| ticket.integrated_commit.as_deref())
        } else if worker.role == "reviewer" {
            worker.base_commit.as_deref()
        } else {
            None
        };
        let Some(preserved) = preserved else { continue };
        let path = &worker.worktree;
        if !path.exists() || validate_worktree(repository, path).is_err() {
            continue;
        }
        let clean =
            git(path, &["status", "--porcelain"]).is_ok_and(|status| status.trim().is_empty());
        if clean && is_ancestor(repository, preserved, merged)? {
            git(
                repository,
                &[
                    "worktree",
                    "remove",
                    path.to_str().context("non-UTF8 worker worktree path")?,
                ],
            )?;
        }
    }
    Ok(())
}

#[derive(Debug)]
enum RequiredCheckError {
    Launch {
        command: String,
        source: std::io::Error,
    },
    Failed {
        command: String,
        detail: String,
    },
}

impl std::fmt::Display for RequiredCheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Launch { command, source } => {
                write!(f, "could not launch {command}: {source}")
            }
            Self::Failed { command, detail } => write!(f, "{command} failed: {detail}"),
        }
    }
}

impl std::error::Error for RequiredCheckError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Launch { source, .. } => Some(source),
            Self::Failed { .. } => None,
        }
    }
}

fn required_checks(path: &Path) -> Result<Vec<CheckEvidence>> {
    let commit = git(path, &["rev-parse", "HEAD"])?.trim().to_owned();
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .map(Ok)
        .unwrap_or_else(|| check_target_dir(path, &commit))?;
    fs::create_dir_all(&target_dir).context("create isolated required-check target directory")?;
    let commands: [(&str, &[&str]); 3] = [
        ("cargo", &["fmt", "--all", "--", "--check"]),
        (
            "cargo",
            &[
                "clippy",
                "--locked",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ],
        ),
        ("cargo", &["test", "--locked", "--all-targets"]),
    ];
    commands
        .into_iter()
        .map(|(program, args)| {
            let command = format!("{program} {}", args.join(" "));
            let output = Command::new(program)
                .args(args)
                .current_dir(path)
                .env("CARGO_TARGET_DIR", &target_dir)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .map_err(|source| {
                    anyhow::Error::new(RequiredCheckError::Launch {
                        command: command.clone(),
                        source,
                    })
                })?;
            if !output.status.success() {
                return Err(anyhow::Error::new(RequiredCheckError::Failed {
                    command,
                    detail: format!(
                        "exit {}\nstdout:\n{}\nstderr:\n{}",
                        output.status,
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    ),
                }));
            }
            Ok(CheckEvidence {
                commit: commit.clone(),
                command,
                result: format!("passed (exit {})", output.status),
            })
        })
        .collect()
}

fn check_target_dir(path: &Path, commit: &str) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path).context("resolve required-check checkout")?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    canonical.hash(&mut hasher);
    Ok(std::env::temp_dir()
        .join("wayfinder-herdr-required-check-targets")
        .join(format!("{:016x}", hasher.finish()))
        .join(commit))
}

fn run(path: &Path, program: &str, args: &[&str]) -> Result<()> {
    let output = Command::new(program)
        .args(args)
        .current_dir(path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("launch {program}"))?;
    ensure!(
        output.status.success(),
        "{program} {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(())
}
fn git(path: &Path, args: &[&str]) -> Result<String> {
    let output = git_result(path, args)?;
    String::from_utf8(output).context("git returned non-UTF8 output")
}
fn git_result(path: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .args(["-C"])
        .arg(path)
        .args(args)
        .output()
        .context("run git delivery operation")?;
    ensure!(
        output.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(output.stdout)
}
fn validate_branch(branch: &str) -> Result<()> {
    ensure!(
        !branch.is_empty()
            && !branch.starts_with('-')
            && !branch.contains("..")
            && !branch.contains(' '),
        "invalid feature branch name"
    );
    Ok(())
}
fn is_ancestor(repository: &Path, ancestor: &str, descendant: &str) -> Result<bool> {
    let output = Command::new("git")
        .args(["-C"])
        .arg(repository)
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .output()?;
    match output.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => bail!(
            "git merge-base failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    }
}
fn update_ref(repository: &Path, reference: &str, new: &str, expected_old: &str) -> Result<()> {
    git(repository, &["update-ref", reference, new, expected_old])
        .context("feature branch changed before serialized fast-forward")?;
    Ok(())
}

/// Advance an integration target and its checkout together. A bound repository
/// commonly has the feature branch checked out; updating only its ref would
/// leave the index and files at the prior commit while reporting integration.
fn advance_feature_branch(
    repository: &Path,
    reference: &str,
    new: &str,
    expected_old: &str,
) -> Result<()> {
    let current = git(repository, &["branch", "--show-current"])?;
    let current_ref = format!("refs/heads/{}", current.trim());
    if current_ref == reference {
        ensure!(
            git(repository, &["rev-parse", "HEAD"])?.trim() == expected_old,
            "checked-out feature branch moved before integration"
        );
        ensure!(
            git(repository, &["rev-parse", reference])?.trim() == expected_old,
            "feature ref changed before checked-out fast-forward"
        );
        let output = Command::new("git")
            .args(["-C"])
            .arg(repository)
            // Git otherwise replaces ignored files when the incoming commit
            // starts tracking their path. Those files are invisible to
            // status/porcelain, so require an explicit refusal before moving
            // either the branch or the checked-out files.
            .args([
                "merge",
                "--ff-only",
                "--no-edit",
                "--no-overwrite-ignore",
                new,
            ])
            .output()
            .context("fast-forward checked-out feature branch")?;
        ensure!(
            output.status.success(),
            "checked-out feature branch fast-forward failed safely: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
        ensure!(
            git(repository, &["rev-parse", "HEAD"])?.trim() == new
                && git(repository, &["rev-parse", reference])?.trim() == new,
            "checked-out feature branch did not reach the reviewed integration commit"
        );
    } else {
        update_ref(repository, reference, new, expected_old)?;
    }
    Ok(())
}
fn validate_worktree(repository: &Path, path: &Path) -> Result<()> {
    let expected = fs::canonicalize(path)?;
    let root = git(path, &["rev-parse", "--show-toplevel"])?;
    ensure!(
        Path::new(root.trim()) == expected,
        "integration worktree root mismatch"
    );
    let list = git(repository, &["worktree", "list", "--porcelain"])?;
    ensure!(
        list.lines().any(|line| line
            .strip_prefix("worktree ")
            .is_some_and(|value| Path::new(value) == expected)),
        "integration path is not a registered worktree"
    );
    Ok(())
}
fn save(dir: &Path, state: &DeliveryState) -> Result<()> {
    store::atomic_json(&dir.join("delivery.json"), state)
}

pub fn map_identity(state: &State) -> Result<MapRef> {
    MapRef::parse(&state.map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{fs, os::unix::fs::PermissionsExt, process::Command};
    use tempfile::TempDir;

    fn command(path: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .args(["-C"])
            .arg(path)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    struct Fixture {
        _temp: TempDir,
        dir: PathBuf,
        repo: PathBuf,
        implementation: String,
        state: State,
    }

    impl Fixture {
        fn new(conflicting_target: bool, stale_evidence: bool) -> Self {
            Self::build(conflicting_target, stale_evidence, true, false)
        }

        fn missing_lockfile() -> Self {
            Self::build(false, false, false, true)
        }

        fn check_ready() -> Self {
            let mut fixture = Self::build(false, false, false, false);
            let worktree = &fixture.state.workers.runs[0].worktree;
            fs::create_dir_all(worktree.join("src")).unwrap();
            fs::write(
                worktree.join("Cargo.toml"),
                "[package]\nname = \"locked-check-fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
            )
            .unwrap();
            fs::write(
                worktree.join("src/lib.rs"),
                "pub fn value() -> u8 {\n    1\n}\n",
            )
            .unwrap();
            fs::write(
                worktree.join("Cargo.lock"),
                "version = 4\n\n[[package]]\nname = \"locked-check-fixture\"\nversion = \"0.1.0\"\n",
            )
            .unwrap();
            command(worktree, &["add", "Cargo.toml", "Cargo.lock", "src"]);
            command(
                worktree,
                &["commit", "--quiet", "-m", "locked check fixture"],
            );
            let candidate = command(worktree, &["rev-parse", "HEAD"]);
            let reviewer = &fixture.state.workers.runs[1].worktree;
            command(reviewer, &["checkout", "--detach", &candidate]);
            fixture.state.workers.runs[0].result_commit = Some(candidate.clone());
            fixture.state.workers.runs[1].base_commit = Some(candidate.clone());
            let review_path = fixture.state.workers.runs[1]
                .result_evidence
                .as_ref()
                .unwrap();
            let mut evidence: serde_json::Value =
                serde_json::from_slice(&fs::read(review_path).unwrap()).unwrap();
            evidence["reviewed_commit"] = json!(candidate);
            fs::write(review_path, serde_json::to_vec(&evidence).unwrap()).unwrap();
            fixture.implementation = candidate;
            fixture
        }

        fn build(
            conflicting_target: bool,
            stale_evidence: bool,
            advance_target: bool,
            missing_lockfile: bool,
        ) -> Self {
            let temp = tempfile::tempdir().unwrap();
            let repo = temp.path().join("repo");
            fs::create_dir(&repo).unwrap();
            command(&repo, &["init", "--quiet"]);
            command(&repo, &["config", "user.name", "Delivery Fixture"]);
            command(&repo, &["config", "user.email", "delivery@example.invalid"]);
            fs::write(repo.join("shared.txt"), "base\n").unwrap();
            command(&repo, &["add", "shared.txt"]);
            command(&repo, &["commit", "--quiet", "-m", "base"]);
            command(&repo, &["branch", "develop"]);
            command(&repo, &["branch", "-M", "feature/delivery-test"]);
            let base = command(&repo, &["rev-parse", "HEAD"]);

            let implementation_path = temp.path().join("implementer");
            command(
                &repo,
                &[
                    "worktree",
                    "add",
                    "--detach",
                    implementation_path.to_str().unwrap(),
                    &base,
                ],
            );
            fs::write(implementation_path.join("shared.txt"), "implementation\n").unwrap();
            command(&implementation_path, &["add", "shared.txt"]);
            if missing_lockfile {
                fs::create_dir_all(implementation_path.join("src")).unwrap();
                fs::create_dir_all(implementation_path.join("tests")).unwrap();
                fs::write(
                    implementation_path.join("Cargo.toml"),
                    "[package]\nname = \"check-failure-fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
                )
                .unwrap();
                fs::write(
                    implementation_path.join("src/lib.rs"),
                    "pub fn value() -> u8 {\n    1\n}\n",
                )
                .unwrap();
                fs::write(
                    implementation_path.join("tests/check.rs"),
                    "#[test]\nfn check() {\n    assert_eq!(check_failure_fixture::value(), 1);\n}\n",
                )
                .unwrap();
                command(&implementation_path, &["add", "Cargo.toml", "src", "tests"]);
            }
            command(
                &implementation_path,
                &["commit", "--quiet", "-m", "implementation"],
            );
            let implementation = command(&implementation_path, &["rev-parse", "HEAD"]);

            let reviewer_path = temp.path().join("reviewer");
            command(
                &repo,
                &[
                    "worktree",
                    "add",
                    "--detach",
                    reviewer_path.to_str().unwrap(),
                    &implementation,
                ],
            );
            let evidence = temp.path().join("review.json");
            fs::write(&evidence, serde_json::to_vec(&json!({
                "format_version":1,"run_id":"run-00000000000000000002","ticket":15,"role":"reviewer",
                "status":"completed","summary":"review approved","reviewed_commit":(if stale_evidence { &base } else { &implementation }),
                "verdict":"approved","unresolved_findings":[],"known_limitations":["fixture-only local repository"]
            })).unwrap()).unwrap();

            let target_path = temp.path().join("target");
            command(
                &repo,
                &[
                    "worktree",
                    "add",
                    "--detach",
                    target_path.to_str().unwrap(),
                    &base,
                ],
            );
            if conflicting_target {
                fs::write(target_path.join("shared.txt"), "target\n").unwrap();
            } else {
                fs::write(target_path.join("target.txt"), "target\n").unwrap();
                command(&target_path, &["add", "target.txt"]);
            }
            command(&target_path, &["add", "shared.txt"]);
            command(
                &target_path,
                &["commit", "--quiet", "-m", "advance feature target"],
            );
            if advance_target {
                command(
                    &repo,
                    &[
                        "update-ref",
                        "refs/heads/feature/delivery-test",
                        &command(&target_path, &["rev-parse", "HEAD"]),
                        &base,
                    ],
                );
            }

            let implementer = json!({"id":"run-00000000000000000001","ticket":15,"role":"implementer","attempt":1,"rework_round":0,"status":"reviewed","worktree":implementation_path,"workspace_id":null,"tab_id":null,"pane_id":null,"base_commit":base,"result_commit":implementation,"summary":"implemented","question":null,"source_run":null,"claim_login":"fixture","context":null,"last_activity_ms":null,"terminal_id":null,"agent_provider":null,"agent_session":null,"foreground_process":null,"result_evidence":null});
            let reviewer = json!({"id":"run-00000000000000000002","ticket":15,"role":"reviewer","attempt":1,"rework_round":0,"status":"completed","worktree":reviewer_path,"workspace_id":null,"tab_id":null,"pane_id":null,"base_commit":implementation,"result_commit":null,"summary":"approved","question":null,"source_run":"run-00000000000000000001","claim_login":"fixture","context":null,"last_activity_ms":null,"terminal_id":null,"agent_provider":null,"agent_session":null,"foreground_process":null,"result_evidence":evidence});
            let state: serde_json::Value = json!({
                "format_version":1,
                "map":"example/project#42",
                "binding":{"repository":repo,"herdr_binary":"/bin/true","socket":"/tmp/test.sock","herdr_config":null},
                "authorization":"started","poll_seconds":30,"concurrency":3,"reconciled":true,
                "suspension":"","history":[],"workers":{"next_run":2,"runs":[implementer,reviewer],"providers":{}}
            });
            let state: State = serde_json::from_value(state).unwrap();
            let dir = temp.path().join("state");
            fs::create_dir_all(&dir).unwrap();
            Self {
                _temp: temp,
                dir,
                repo,
                implementation,
                state,
            }
        }
    }

    #[test]
    fn stale_review_evidence_cannot_authorize_integration() {
        let f = Fixture::new(false, true);
        assert!(
            reconcile(&f.dir, &f.state, &[])
                .unwrap_err()
                .to_string()
                .contains("stale for the expected exact commit")
        );
        assert!(read(&f.dir).unwrap().tickets.is_empty());
    }

    #[test]
    fn required_check_failure_retains_approval_and_exact_candidate_for_rework() {
        let f = Fixture::missing_lockfile();
        let outcome = reconcile(&f.dir, &f.state, &[]).unwrap();
        let Outcome::CheckFailure {
            run_id,
            commit,
            detail,
        } = outcome
        else {
            panic!("expected a confirmed required-check failure, got {outcome:?}")
        };
        assert_eq!(run_id, "run-00000000000000000001");
        assert_eq!(commit, f.implementation);
        assert!(
            detail.contains("cargo clippy --locked --all-targets"),
            "{detail}"
        );
        assert!(detail.contains("Cargo.lock"), "{detail}");
        let delivery = read(&f.dir).unwrap();
        let ticket = delivery.tickets.get(&run_id).unwrap();
        assert_eq!(
            ticket.check_failure_commit.as_deref(),
            Some(commit.as_str())
        );
        assert_eq!(ticket.review.as_ref().unwrap().commit, commit);
        assert!(ticket.last_error.as_deref().unwrap().contains(&detail));
        assert!(ticket.integrated_commit.is_none());
        assert!(
            !f.state.workers.runs[0].worktree.join("target").exists(),
            "Cargo output stays outside the worker checkout even on check failure"
        );
        assert_eq!(
            git(&f.repo, &["rev-parse", "refs/heads/feature/delivery-test"])
                .unwrap()
                .trim(),
            f.state
                .workers
                .runs
                .iter()
                .find(|run| run.role == "implementer")
                .unwrap()
                .base_commit
                .as_deref()
                .unwrap()
        );
    }

    #[test]
    fn required_checks_use_owned_external_target_and_integrate_clean_candidate() {
        let f = Fixture::check_ready();
        let outcome = reconcile(&f.dir, &f.state, &[]).unwrap();
        assert!(
            matches!(&outcome, Outcome::Integrated { commit, .. } if commit == &f.implementation),
            "locked checks should integrate the exact approved candidate: {outcome:?}"
        );
        assert!(
            !f.state.workers.runs[0].worktree.join("target").exists(),
            "required checks must not dirty the owned implementation checkout"
        );
        let worktree = &f.state.workers.runs[0].worktree;
        let check_target = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .map(Ok)
            .unwrap_or_else(|| check_target_dir(worktree, &f.implementation))
            .unwrap();
        assert!(check_target.exists());
        assert!(
            !check_target.starts_with(worktree),
            "required-check build outputs must stay outside the owned checkout"
        );
        let delivery = read(&f.dir).unwrap();
        let ticket = delivery.tickets.values().next().unwrap();
        assert_eq!(
            ticket.integrated_commit.as_deref(),
            Some(f.implementation.as_str())
        );
        assert_eq!(ticket.checks.len(), 3);
        assert!(ticket.checks.iter().all(|check| {
            check.commit == f.implementation && check.result.starts_with("passed")
        }));
        assert_eq!(
            git(&f.repo, &["rev-parse", "refs/heads/feature/delivery-test"])
                .unwrap()
                .trim(),
            f.implementation
        );
        assert_eq!(
            git(&f.repo, &["rev-parse", "HEAD"]).unwrap().trim(),
            f.implementation,
            "checked-out feature branch HEAD must follow the integrated ref"
        );
        assert!(git(&f.repo, &["status", "--porcelain"]).unwrap().is_empty());
        assert_eq!(
            git(&f.repo, &["write-tree"]).unwrap().trim(),
            command(&f.repo, &["rev-parse", "HEAD^{tree}"]),
            "the checked-out index must match the integrated commit tree"
        );
        assert_eq!(
            fs::read(f.repo.join("Cargo.lock")).unwrap(),
            git(&f.repo, &["show", "HEAD:Cargo.lock"])
                .unwrap()
                .into_bytes()
        );
    }

    #[test]
    fn approval_from_retried_reviewer_resolves_through_original_implementation() {
        let mut f = Fixture::check_ready();
        let prior_reviewer = &mut f.state.workers.runs[1];
        prior_reviewer.status = WorkerStatus::Stopped;
        let reviewer_path = f._temp.path().join("reviewer-retry");
        command(
            &f.repo,
            &[
                "worktree",
                "add",
                "--detach",
                reviewer_path.to_str().unwrap(),
                &f.implementation,
            ],
        );
        let report = f._temp.path().join("reviewer-retry-result.json");
        fs::write(
            &report,
            serde_json::to_vec(&json!({
                "format_version":1,
                "run_id":"run-review-retry",
                "ticket":15,
                "role":"reviewer",
                "status":"completed",
                "summary":"approved the same exact candidate after restart recovery",
                "reviewed_commit":f.implementation,
                "verdict":"approved",
                "unresolved_findings":[],
                "known_limitations":[]
            }))
            .unwrap(),
        )
        .unwrap();
        let mut retry = f.state.workers.runs[1].clone();
        retry.id = "run-review-retry".into();
        retry.attempt = 2;
        retry.status = WorkerStatus::Completed;
        retry.worktree = reviewer_path;
        retry.source_run = Some("run-00000000000000000002".into());
        retry.result_evidence = Some(report);
        f.state.workers.runs.push(retry);

        let outcome = reconcile(&f.dir, &f.state, &[]).unwrap();
        assert!(
            matches!(&outcome, Outcome::Integrated { commit, .. } if commit == &f.implementation),
            "a retried reviewer must approve the implementation ancestor, not be orphaned: {outcome:?}"
        );
        let delivery = read(&f.dir).unwrap();
        assert_eq!(
            delivery
                .tickets
                .values()
                .next()
                .and_then(|ticket| ticket.integrated_commit.as_deref()),
            Some(f.implementation.as_str())
        );
    }

    #[test]
    fn checked_out_fast_forward_preserves_unrelated_untracked_user_file() {
        let f = Fixture::check_ready();
        let note = f.repo.join("human-notes.txt");
        fs::write(&note, "retain unrelated user content\n").unwrap();
        let outcome = reconcile(&f.dir, &f.state, &[]).unwrap();
        assert!(matches!(outcome, Outcome::Integrated { .. }));
        assert_eq!(
            fs::read_to_string(&note).unwrap(),
            "retain unrelated user content\n"
        );
        assert_eq!(
            git(&f.repo, &["rev-parse", "HEAD"]).unwrap().trim(),
            f.implementation
        );
        assert!(
            git(&f.repo, &["status", "--porcelain"])
                .unwrap()
                .contains("?? human-notes.txt")
        );
    }

    #[test]
    fn checked_out_fast_forward_preserves_ignored_user_file_at_new_candidate_path() {
        let f = Fixture::check_ready();
        let ignored = f.repo.join("Cargo.lock");
        fs::write(f.repo.join(".git/info/exclude"), "Cargo.lock\n").unwrap();
        fs::write(&ignored, "human's local ignored content\n").unwrap();
        assert!(command(&f.repo, &["check-ignore", "Cargo.lock"]).contains("Cargo.lock"));

        let error = reconcile(&f.dir, &f.state, &[]).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("checked-out feature branch fast-forward failed safely"),
            "integration should retain a visible refusal: {error:#}"
        );
        assert_eq!(
            fs::read_to_string(&ignored).unwrap(),
            "human's local ignored content\n",
            "a reviewed candidate must not overwrite ignored local data"
        );
        assert_ne!(
            git(&f.repo, &["rev-parse", "HEAD"]).unwrap().trim(),
            f.implementation,
            "the checked-out ref must remain at its old commit when Git refuses"
        );
        assert_ne!(
            git(&f.repo, &["rev-parse", "refs/heads/feature/delivery-test"])
                .unwrap()
                .trim(),
            f.implementation,
            "the feature ref must not advance when checkout preservation fails"
        );
    }

    #[test]
    fn checked_out_fast_forward_refuses_a_stale_expected_target() {
        let f = Fixture::check_ready();
        let error = advance_feature_branch(
            &f.repo,
            "refs/heads/feature/delivery-test",
            &f.implementation,
            "0000000000000000000000000000000000000000",
        )
        .unwrap_err();
        assert!(error.to_string().contains("moved before integration"));
        assert_eq!(
            git(&f.repo, &["rev-parse", "HEAD"]).unwrap().trim(),
            f.state.workers.runs[0].base_commit.as_deref().unwrap()
        );
        assert!(git(&f.repo, &["status", "--porcelain"]).unwrap().is_empty());
    }

    #[test]
    fn legacy_reviewed_run_without_base_uses_exact_common_ancestor_for_checking() {
        let mut f = Fixture::missing_lockfile();
        f.state.workers.runs[0].base_commit = None;
        let outcome = reconcile(&f.dir, &f.state, &[]).unwrap();
        assert!(
            matches!(&outcome, Outcome::CheckFailure { commit, .. } if commit == &f.implementation),
            "legacy missing-base candidate should reach the required checks: {outcome:?}"
        );
        let delivery = read(&f.dir).unwrap();
        let ticket = delivery.tickets.values().next().unwrap();
        assert_eq!(
            ticket.candidate_base.as_deref(),
            Some(
                git(&f.repo, &["rev-parse", "refs/heads/feature/delivery-test"])
                    .unwrap()
                    .trim()
            )
        );
    }

    #[test]
    fn changed_feature_target_rebases_and_requires_review_of_new_commit() {
        let f = Fixture::new(false, false);
        let outcome = reconcile(&f.dir, &f.state, &[]).unwrap();
        let Outcome::ReviewRenewal { run_id, commit } = outcome else {
            panic!("expected renewed review, got {outcome:?}")
        };
        assert_eq!(run_id, "run-00000000000000000001");
        assert_ne!(commit, f.implementation);
        assert!(
            is_ancestor(
                &f.repo,
                &command(&f.repo, &["rev-parse", "refs/heads/feature/delivery-test"]),
                &commit
            )
            .unwrap()
        );
        let second = reconcile(&f.dir, &f.state, &[]).unwrap();
        assert!(
            matches!(second, Outcome::Held { .. }),
            "a repeated hook must wait for renewed review"
        );
        let delivery = read(&f.dir).unwrap();
        assert_eq!(
            delivery
                .tickets
                .values()
                .next()
                .unwrap()
                .candidate_commit
                .as_deref(),
            Some(commit.as_str())
        );

        let mut renewed_reviewer = f.state.workers.runs[1].clone();
        renewed_reviewer.id = "run-renewed-review".into();
        renewed_reviewer.base_commit = Some(commit.clone());
        let evidence = f._temp.path().join("renewed-review.json");
        fs::write(
            &evidence,
            serde_json::to_vec(&json!({
                "format_version":1,"run_id":"run-renewed-review","ticket":15,
                "role":"reviewer","status":"completed","summary":"The rebased candidate still has a required defect.",
                "reviewed_commit":commit,"verdict":"changes_requested",
                "unresolved_findings":["The conflict repair changes required behavior."],
                "known_limitations":[]
            }))
            .unwrap(),
        )
        .unwrap();
        renewed_reviewer.result_evidence = Some(evidence);
        let mut renewed_state = f.state.clone();
        renewed_state.workers.runs.push(renewed_reviewer);
        let held = reconcile(&f.dir, &renewed_state, &[]).unwrap();
        assert!(matches!(held, Outcome::Held { .. }));
        assert_eq!(
            command(&f.repo, &["rev-parse", "refs/heads/feature/delivery-test"]),
            delivery
                .tickets
                .values()
                .next()
                .unwrap()
                .candidate_base
                .clone()
                .unwrap()
        );
        assert!(
            read(&f.dir)
                .unwrap()
                .tickets
                .values()
                .next()
                .unwrap()
                .integrated_commit
                .is_none()
        );
    }

    #[test]
    fn wrong_bound_branch_holds_delivery_without_aborting_runtime_reconciliation() {
        let f = Fixture::new(false, false);
        command(&f.repo, &["checkout", "develop"]);
        let outcome = reconcile(&f.dir, &f.state, &[]).unwrap();
        assert!(
            matches!(outcome, Outcome::Held { detail, .. } if detail.contains("feature branch"))
        );
        let ticket = read(&f.dir).unwrap().tickets;
        assert!(ticket.values().any(|entry| {
            entry
                .last_error
                .as_deref()
                .is_some_and(|detail| detail.contains("feature branch"))
        }));
        assert_eq!(
            git(&f.repo, &["branch", "--show-current"]).unwrap().trim(),
            "develop"
        );
    }

    #[test]
    fn rebase_conflict_is_retained_and_repeated_hook_does_not_retry_it() {
        let f = Fixture::new(true, false);
        let first = reconcile(&f.dir, &f.state, &[]).unwrap();
        let Outcome::Conflict { base_commit, .. } = first else {
            panic!("expected conflict, got {first:?}")
        };
        let delivery = read(&f.dir).unwrap();
        let entry = delivery.tickets.values().next().unwrap();
        let retained = entry.integration_worktree.as_ref().unwrap();
        assert!(retained.exists());
        assert_eq!(entry.conflict_base.as_deref(), Some(base_commit.as_str()));
        let again = reconcile(&f.dir, &f.state, &[]).unwrap();
        assert!(matches!(again, Outcome::Conflict { .. }));
        assert!(retained.exists());
    }

    #[test]
    fn every_open_task_must_be_integrated_before_final_review_is_dispatched() {
        let f = Fixture::new(false, false);
        let mut delivery = DeliveryState {
            feature_branch: Some("feature/delivery-test".into()),
            map_children: Some(vec![open_child(15, "Integrate exact-commit reviews", true)]),
            ..DeliveryState::default()
        };
        delivery.tickets.insert(
            "run-00000000000000000001".into(),
            TicketDelivery {
                reviewed_commit: Some(f.implementation.clone()),
                integrated_commit: Some(f.implementation.clone()),
                issue: 15,
                title: "Implement exact-commit review".into(),
                url: "https://github.com/example/project/issues/15".into(),
                ..TicketDelivery::default()
            },
        );
        let source = f
            .state
            .workers
            .runs
            .iter()
            .find(|run| run.role == "implementer")
            .unwrap();
        let mut children = vec![open_child(15, "Implement exact-commit review", true)];
        children.push(open_child(16, "Second implementation ticket", true));
        let blocked = feature_review_outcome(&f.state, &delivery, source, &children).unwrap();
        assert_eq!(blocked, Outcome::Nothing);
        children.pop();
        let mut externally_closed = open_child(16, "Externally closed implementation", true);
        externally_closed.state = "closed".into();
        children.push(externally_closed);
        assert_eq!(
            feature_review_outcome(&f.state, &delivery, source, &children).unwrap(),
            Outcome::Nothing,
            "external closure without verified integration evidence must block final review"
        );
        children.pop();
        let ready_while_open =
            feature_review_outcome(&f.state, &delivery, source, &children).unwrap();
        assert!(matches!(
            ready_while_open,
            Outcome::FinalReviewNeeded { .. }
        ));
        children[0].state = "closed".into();
        let ready_after_natural_closure =
            feature_review_outcome(&f.state, &delivery, source, &children).unwrap();
        assert!(
            matches!(
                ready_after_natural_closure,
                Outcome::FinalReviewNeeded { .. }
            ),
            "a naturally closed task with verified integration evidence must allow final review"
        );
    }

    #[test]
    fn conflict_repair_supersedes_old_blocker_without_erasing_conflict_evidence() {
        let f = Fixture::new(true, false);
        let Outcome::Conflict { base_commit, .. } = reconcile(&f.dir, &f.state, &[]).unwrap()
        else {
            panic!("expected initial integration conflict")
        };
        let mut delivery = read(&f.dir).unwrap();
        let original = delivery.tickets.values().next().unwrap().clone();
        assert!(original.last_error.is_some());
        assert_eq!(
            original.conflict_base.as_deref(),
            Some(base_commit.as_str())
        );

        let original_run = f
            .state
            .workers
            .runs
            .iter()
            .find(|run| run.role == "implementer")
            .unwrap();
        let mut repair = original_run.clone();
        repair.id = "run-conflict-repair".into();
        repair.source_run = Some(original_run.id.clone());
        repair.base_commit = Some(base_commit.clone());
        repair.status = WorkerStatus::Reviewed;
        let mut repair_state = f.state.clone();
        repair_state.workers.runs.push(repair.clone());
        supersede_delivery_ancestors(&repair_state, &mut delivery, &repair);

        let superseded = delivery.tickets.get(&original_run.id).unwrap();
        assert_eq!(
            superseded.superseded_by.as_deref(),
            Some(repair.id.as_str())
        );
        assert_eq!(
            superseded.conflict_base.as_deref(),
            Some(base_commit.as_str())
        );
        assert!(superseded.last_error.is_some());
        delivery.tickets.insert(
            repair.id.clone(),
            TicketDelivery {
                issue: repair.ticket,
                title: "Implement exact-commit review".into(),
                url: "https://github.com/example/project/issues/15".into(),
                reviewed_commit: repair.result_commit.clone(),
                integrated_commit: Some(base_commit),
                ..TicketDelivery::default()
            },
        );
        delivery.feature_branch = Some("feature/delivery-test".into());
        let children = [open_child(15, "Implement exact-commit review", true)];
        assert!(matches!(
            feature_review_outcome(&repair_state, &delivery, &repair, &children).unwrap(),
            Outcome::FinalReviewNeeded { .. }
        ));
    }

    #[test]
    fn unresolved_non_task_map_child_gates_final_review_and_is_named_in_chat() {
        let f = Fixture::new(false, false);
        let mut delivery = DeliveryState {
            feature_branch: Some("feature/delivery-test".into()),
            ..DeliveryState::default()
        };
        delivery.tickets.insert(
            "run-00000000000000000001".into(),
            TicketDelivery {
                reviewed_commit: Some(f.implementation.clone()),
                integrated_commit: Some(f.implementation.clone()),
                issue: 15,
                title: "Implement exact-commit review".into(),
                url: "https://github.com/example/project/issues/15".into(),
                ..TicketDelivery::default()
            },
        );
        let source = f
            .state
            .workers
            .runs
            .iter()
            .find(|run| run.role == "implementer")
            .unwrap();
        let research = open_child(17, "Resolve accepted research question", false);
        assert_eq!(
            feature_review_outcome(&f.state, &delivery, source, std::slice::from_ref(&research))
                .unwrap(),
            Outcome::Nothing,
            "an open non-task child must keep feature readiness blocked"
        );
        delivery.open_children.push(research);
        save(&f.dir, &delivery).unwrap();
        let milestones = chat_milestones(&f.dir).unwrap();
        assert!(milestones.iter().any(|line| {
            line.contains("Open map decision or research work remains")
                && line.contains(
                    "[Resolve accepted research question](https://github.com/example/project/issues/17)"
                )
        }));
    }

    #[test]
    fn pending_human_answer_blocks_readiness_but_final_review_does_not_count_as_ticket_work() {
        let f = Fixture::new(false, false);
        let mut delivery = DeliveryState {
            feature_branch: Some("feature/delivery-test".into()),
            ..DeliveryState::default()
        };
        delivery.tickets.insert(
            "run-00000000000000000001".into(),
            TicketDelivery {
                issue: 15,
                title: "Implement exact-commit review".into(),
                url: "https://github.com/example/project/issues/15".into(),
                integrated_commit: Some(f.implementation.clone()),
                ..TicketDelivery::default()
            },
        );
        let source = f
            .state
            .workers
            .runs
            .iter()
            .find(|run| run.role == "implementer")
            .unwrap();
        let mut state = f.state.clone();
        let mut human_request = state.workers.runs[0].clone();
        human_request.id = "run-human-question".into();
        human_request.role = "implementer".into();
        human_request.status = WorkerStatus::NeedsHuman;
        human_request.human_request_id = Some("request-genuine-human-answer".into());
        state.workers.runs.push(human_request);
        assert_eq!(
            feature_review_outcome(&state, &delivery, source, &[]).unwrap(),
            Outcome::Nothing,
            "a pending worker question must gate the final feature review"
        );
        let chat_root = f._temp.path().join("chat-state");
        let (_, key) = store::map_identity(&state.map).unwrap();
        let chat_dir = store::map_dir(&chat_root, &key).unwrap();
        fs::create_dir_all(&chat_dir).unwrap();
        store::atomic_json(&chat_dir.join("state.json"), &state).unwrap();
        save(&chat_dir, &delivery).unwrap();
        assert!(chat_milestones(&chat_dir).unwrap().iter().any(|line| {
            line.contains("A human decision is pending for")
                && line.contains(
                    "[Implement exact-commit review](https://github.com/example/project/issues/15)",
                )
        }));

        state.workers.runs.pop();
        let request_id = store::create_scheduler_decision(
            &mut state,
            store::NewSchedulerDecision {
                ticket: 15,
                run_id: "run-completed-worker",
                source_run_id: "run-completed-worker",
                kind: store::SchedulerDecisionKind::ConflictExhaustion,
                blocked_status: WorkerStatus::Completed,
                base_commit: Some("feature-target"),
                question: "Choose whether to accept the documented limitation.",
            },
        )
        .unwrap();
        assert_eq!(
            feature_review_outcome(&state, &delivery, source, &[]).unwrap(),
            Outcome::Nothing,
            "an unresolved scheduler decision must gate final review"
        );
        let abandoned = state.scheduler_decisions.last_mut().unwrap();
        abandoned.response = Some("Leave this ticket incomplete".into());
        abandoned.disposition = Some(store::SchedulerDecisionDisposition::Abandon);
        abandoned.application = store::SchedulerDecisionApplication::Abandoned;
        delivery.tickets.insert(
            "run-abandoned-but-unintegrated".into(),
            TicketDelivery {
                issue: 16,
                title: "Abandoned implementation remains incomplete".into(),
                url: "https://github.com/example/project/issues/16".into(),
                ..TicketDelivery::default()
            },
        );
        assert_eq!(
            feature_review_outcome(&state, &delivery, source, &[]).unwrap(),
            Outcome::Nothing,
            "abandon releases the known-finished worker but cannot satisfy integration evidence"
        );
        delivery.tickets.remove("run-abandoned-but-unintegrated");
        state.scheduler_decisions.pop();
        assert!(!request_id.is_empty());
        let branch_head = git(&f.repo, &["rev-parse", "refs/heads/feature/delivery-test"])
            .unwrap()
            .trim()
            .to_owned();
        let mut final_review = state.workers.runs[1].clone();
        final_review.id = "run-final-review".into();
        final_review.role = "reviewer".into();
        final_review.context = Some("wayfinder-final-feature-review".into());
        final_review.base_commit = Some(branch_head);
        final_review.status = WorkerStatus::Running;
        state.workers.runs.push(final_review);
        assert_eq!(
            feature_review_outcome(&state, &delivery, source, &[]).unwrap(),
            Outcome::Nothing,
            "an in-flight final reviewer must not be surfaced as an implementation loop or duplicated"
        );
    }

    #[test]
    fn integrated_open_task_is_reported_as_awaiting_orchestrator_closure() {
        let temp = tempfile::tempdir().unwrap();
        let delivery = DeliveryState {
            tickets: BTreeMap::from([(
                "run-internal".into(),
                TicketDelivery {
                    issue: 15,
                    title: "Implement exact-commit review".into(),
                    url: "https://github.com/example/project/issues/15".into(),
                    integrated_commit: Some("0123456789abcdef".into()),
                    ..TicketDelivery::default()
                },
            )]),
            open_children: vec![open_child(15, "Implement exact-commit review", true)],
            ..DeliveryState::default()
        };
        save(temp.path(), &delivery).unwrap();
        let milestones = chat_milestones(temp.path()).unwrap();
        assert!(milestones.iter().any(|line| {
            line.contains(
                "[Implement exact-commit review](https://github.com/example/project/issues/15)",
            ) && line.contains("awaiting orchestrator closure")
        }));
    }

    fn open_child(number: u64, title: &str, task: bool) -> ChildTicket {
        ChildTicket {
            number,
            title: title.into(),
            url: format!("https://github.com/example/project/issues/{number}"),
            state: "open".into(),
            labels: if task {
                vec!["wayfinder:task".into()]
            } else {
                vec!["wayfinder:decision".into()]
            },
        }
    }

    #[test]
    fn integration_lock_serializes_simultaneous_requests() {
        use std::{
            sync::{Arc, Barrier},
            thread,
            time::Duration,
        };
        let f = tempfile::tempdir().unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let path = f.path().join("integration.lock");
        let first_barrier = barrier.clone();
        let first_path = path.clone();
        let first = thread::spawn(move || {
            let _lock = Lock::acquire(&first_path).unwrap();
            first_barrier.wait();
            thread::sleep(Duration::from_millis(30));
        });
        barrier.wait();
        let second = Lock::acquire(&path);
        assert!(second.is_err());
        assert!(second.err().unwrap().to_string().contains("lock busy"));
        first.join().unwrap();
    }

    #[test]
    fn chat_milestones_use_ticket_title_and_link_without_internal_run_ids() {
        let temp = tempfile::tempdir().unwrap();
        let mut delivery = DeliveryState {
            repository: Some("example/project".into()),
            ..DeliveryState::default()
        };
        delivery.tickets.insert(
            "run-00000000000000000001".into(),
            TicketDelivery {
                issue: 15,
                title: "Integrate exact-commit reviews".into(),
                url: "https://github.com/example/project/issues/15".into(),
                integrated_commit: Some("0123456789abcdef".into()),
                ..TicketDelivery::default()
            },
        );
        save(temp.path(), &delivery).unwrap();
        let messages = chat_milestones(temp.path()).unwrap();
        assert_eq!(messages.len(), 1);
        assert!(messages[0].contains(
            "[Integrate exact-commit reviews](https://github.com/example/project/issues/15)"
        ));
        assert!(!messages[0].contains("run-"));
        assert!(!messages[0].starts_with("Ticket #15"));
    }

    #[test]
    fn draft_pr_creation_is_reconciled_by_head_and_base_and_repeated_hooks_are_safe() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        fs::create_dir(&repo).unwrap();
        command(&repo, &["init", "--quiet"]);
        command(&repo, &["config", "user.name", "PR Fixture"]);
        command(&repo, &["config", "user.email", "pr@example.invalid"]);
        fs::write(repo.join("README.md"), "fixture\n").unwrap();
        command(&repo, &["add", "README.md"]);
        command(&repo, &["commit", "--quiet", "-m", "integrated"]);
        command(&repo, &["branch", "develop"]);
        command(&repo, &["branch", "-M", "feature/delivery-test"]);
        let remote = temp.path().join("remote.git");
        Command::new("git")
            .args(["init", "--bare", "--quiet"])
            .arg(&remote)
            .status()
            .unwrap();
        command(
            &repo,
            &["remote", "add", "origin", remote.to_str().unwrap()],
        );
        command(
            &repo,
            &["push", "--set-upstream", "origin", "feature/delivery-test"],
        );
        command(&repo, &["push", "origin", "develop"]);
        let head = command(&repo, &["rev-parse", "HEAD"]);

        let state_dir = temp.path().join("state");
        fs::create_dir_all(&state_dir).unwrap();
        let mut delivery = DeliveryState {
            feature_branch: Some("feature/delivery-test".into()),
            map_children: Some(vec![open_child(15, "Integrate exact-commit reviews", true)]),
            ..DeliveryState::default()
        };
        delivery.tickets.insert(
            "run-1".into(),
            TicketDelivery {
                reviewed_commit: Some(head.clone()),
                candidate_commit: Some(head.clone()),
                integrated_commit: Some(head.clone()),
                issue: 15,
                title: "Integrate exact-commit reviews".into(),
                url: "https://github.com/example/project/issues/15".into(),
                review: Some(ReviewSummary {
                    commit: head.clone(),
                    summary: "The ticket implementation passes its independent review.".into(),
                    unresolved_findings: vec![],
                    known_limitations: vec!["The test exercises a disposable local remote.".into()],
                }),
                checks: vec![CheckEvidence {
                    commit: head.clone(),
                    command: "cargo test --locked --all-targets".into(),
                    result: "passed (exit 0)".into(),
                }],
                ..TicketDelivery::default()
            },
        );
        save(&state_dir, &delivery).unwrap();
        let gh = temp.path().join("gh");
        fs::write(&gh, r##"#!/usr/bin/env python3
import json, os, sys
path=os.path.join(os.path.dirname(__file__), 'pr.json')
args=sys.argv[1:]
state=json.load(open(path)) if os.path.exists(path) else {'creates':0,'edits':0,'pr':None,'body':''}
if '--body-file' in args:
    state['body']=open(args[args.index('--body-file')+1]).read()
if args[:2] == ['pr','list']:
    print(json.dumps([state['pr']] if state['pr'] else []))
elif args[:2] == ['pr','create']:
    state['creates'] += 1
    meta=json.load(open(os.path.join(os.path.dirname(__file__), 'meta.json')))
    state['pr']={'number':17,'url':'https://github.com/example/project/pull/17','isDraft':True,'headRefName':'feature/delivery-test','baseRefName':'develop','mergedAt':None,'headRefOid':meta['head'],'baseRefOid':meta['base']}
    json.dump(state,open(path,'w'))
    print(state['pr']['url'])
elif args[:2] == ['pr','edit']:
    state['edits'] += 1
    json.dump(state,open(path,'w'))
elif args[:2] == ['pr','ready'] and '--undo' not in args:
    state['pr']['isDraft']=False
    if state.get('ready_head_override'):
        state['pr']['headRefOid']=state['ready_head_override']
    if state.get('ready_ambiguous_after_effect',0) > 0:
        state['ready_ambiguous_after_effect'] -= 1
        json.dump(state,open(path,'w'))
        sys.stderr.write('simulated lost readiness response\n')
        sys.exit(1)
    json.dump(state,open(path,'w'))
elif args[:2] == ['pr','view']:
    if state['pr'] and state['pr']['isDraft'] and state.get('view_fail_after_redraft',0) > 0:
        state['view_fail_after_redraft'] -= 1
        json.dump(state,open(path,'w'))
        sys.stderr.write('simulated post-redraft view failure\n')
        sys.exit(1)
    if state['pr'] and not state['pr']['isDraft'] and state.get('view_fail_after_ready',0) > 0:
        state['view_fail_after_ready'] -= 1
        json.dump(state,open(path,'w'))
        sys.stderr.write('simulated post-ready view failure\n')
        sys.exit(1)
    print(json.dumps(state['pr']))
elif args[:2] == ['pr','ready'] and '--undo' in args:
    if state.get('redraft_fail_before_effect',0) > 0:
        state['redraft_fail_before_effect'] -= 1
        json.dump(state,open(path,'w'))
        sys.stderr.write('simulated pre-effect redraft failure\n')
        sys.exit(1)
    state['pr']['isDraft']=True
    if state.get('redraft_ambiguous_after_effect',0) > 0:
        state['redraft_ambiguous_after_effect'] -= 1
        json.dump(state,open(path,'w'))
        sys.stderr.write('simulated lost redraft response\n')
        sys.exit(1)
    json.dump(state,open(path,'w'))
    print('{}')
else:
    sys.exit(2)
"##).unwrap();
        fs::set_permissions(&gh, fs::Permissions::from_mode(0o755)).unwrap();
        fs::write(
            gh.with_file_name("pr.json"),
            serde_json::to_vec(&json!({
                "creates":0,"edits":0,"pr":null,"body":"",
                "ready_ambiguous_after_effect":1
            }))
            .unwrap(),
        )
        .unwrap();
        let mut state_json = json!({
            "format_version":1,"map":"example/project#42",
            "binding":{"repository":repo,"herdr_binary":"/bin/true","socket":"/tmp/test.sock","herdr_config":null},
            "authorization":"started","poll_seconds":30,"concurrency":3,"reconciled":true,
            "suspension":"","history":[],"workers":{"next_run":0,"runs":[],"providers":{}}
        });
        let workers: State = serde_json::from_value(state_json.take()).unwrap();
        fs::write(gh.with_file_name("meta.json"), serde_json::to_vec(&json!({"head":head,"base":command(&repo, &["rev-parse", "refs/remotes/origin/develop"])})).unwrap()).unwrap();
        let gh_state_path = temp.path().join("pr.json");
        let mut readiness_remote: serde_json::Value =
            serde_json::from_slice(&fs::read(&gh_state_path).unwrap()).unwrap();
        readiness_remote["ready_ambiguous_after_effect"] = json!(1);
        fs::write(
            &gh_state_path,
            serde_json::to_vec(&readiness_remote).unwrap(),
        )
        .unwrap();
        let mut missing_evidence = delivery.clone();
        let ticket = missing_evidence.tickets.get_mut("run-1").unwrap();
        ticket.review = None;
        ticket.checks.clear();
        save(&state_dir, &missing_evidence).unwrap();
        assert!(
            ensure_draft_pr_with_gh(&state_dir, &repo, "example/project#42", &workers, &gh)
                .unwrap_err()
                .to_string()
                .contains("exact-commit ticket review and required-check evidence")
        );
        save(&state_dir, &delivery).unwrap();
        let first = ensure_draft_pr_with_gh(&state_dir, &repo, "example/project#42", &workers, &gh)
            .unwrap()
            .unwrap();
        let second =
            ensure_draft_pr_with_gh(&state_dir, &repo, "example/project#42", &workers, &gh)
                .unwrap()
                .unwrap();
        assert_eq!(first.number, 17);
        assert_eq!(second.number, 17);
        let calls: serde_json::Value =
            serde_json::from_slice(&fs::read(temp.path().join("pr.json")).unwrap()).unwrap();
        assert_eq!(calls["creates"], 1);
        assert_eq!(calls["edits"], 2);
        assert!(read(&state_dir).unwrap().draft_pr.unwrap().draft);
        assert!(
            calls["body"]
                .as_str()
                .unwrap()
                .contains("The ticket implementation passes its independent review.")
        );
        assert!(
            calls["body"]
                .as_str()
                .unwrap()
                .contains("The test exercises a disposable local remote.")
        );
        assert!(
            calls["body"]
                .as_str()
                .unwrap()
                .contains("cargo test --locked --all-targets")
        );

        let reviewer_path = temp.path().join("final-reviewer");
        command(
            &repo,
            &[
                "worktree",
                "add",
                "--detach",
                reviewer_path.to_str().unwrap(),
                &head,
            ],
        );
        let evidence = temp.path().join("final-review.json");
        let review_base = command(&repo, &["rev-parse", "refs/remotes/origin/develop"]);
        fs::write(
            &evidence,
            serde_json::to_vec(&json!({
                "format_version":1,"run_id":"run-final-review","ticket":15,"role":"reviewer",
                "status":"completed","summary":"Final review found the integration boundaries correct.",
                "reviewed_commit":head,"verdict":"approved","unresolved_findings":[],
                "known_limitations":["The test used a disposable local GitHub endpoint."],
                "final_feature_review":{
                    "scope":"complete_feature","map":"example/project#42",
                    "base_ref":"origin/develop","base_commit":review_base,
                    "reviewed_commit":head,"spec":"none-linked",
                    "accepted_decisions_reviewed":true
                }
            })).unwrap(),
        )
        .unwrap();
        let reviewer = json!({"id":"run-final-review","ticket":15,"role":"reviewer","attempt":2,"rework_round":0,"status":"completed","worktree":reviewer_path,"workspace_id":null,"tab_id":null,"pane_id":null,"base_commit":head,"result_commit":null,"summary":"feature approved","question":null,"source_run":"run-final-first","purpose":"final_feature_review","claim_login":null,"context":"Human explicitly authorized this retry after the preceding worker was confirmed stopped or absent.","last_activity_ms":null,"terminal_id":null,"agent_provider":null,"agent_session":null,"foreground_process":null,"result_evidence":evidence});
        let retry_parent = json!({"id":"run-final-first","ticket":15,"role":"reviewer","attempt":1,"status":"stopped","worktree":repo,"base_commit":head,"source_run":"run-1","purpose":"final_feature_review","context":"Human explicitly authorized this retry after the preceding worker was confirmed stopped or absent."});
        let mut ready_workers: State = serde_json::from_value(json!({"format_version":1,"map":"example/project#42","binding":{"repository":repo,"herdr_binary":"/bin/true","socket":"/tmp/test.sock","herdr_config":null},"authorization":"started","poll_seconds":30,"concurrency":3,"reconciled":true,"suspension":"","history":[],"workers":{"next_run":4,"runs":[reviewer,retry_parent,{"id":"run-1","ticket":15,"role":"implementer","attempt":1,"status":"reviewed","worktree":repo,"base_commit":head,"result_commit":head}],"providers":{}}})).unwrap();
        let reviewer_run = &ready_workers.workers.runs[0];
        assert!(final_feature_review_scope_matches(
            reviewer_run,
            "example/project#42",
            &review_base,
            &head
        ));
        let mut missing_scope: serde_json::Value =
            serde_json::from_slice(&fs::read(&evidence).unwrap()).unwrap();
        missing_scope
            .as_object_mut()
            .unwrap()
            .remove("final_feature_review");
        fs::write(&evidence, serde_json::to_vec(&missing_scope).unwrap()).unwrap();
        assert!(!final_feature_review_scope_matches(
            reviewer_run,
            "example/project#42",
            &review_base,
            &head
        ));
        fs::write(
            &evidence,
            serde_json::to_vec(&json!({
                "format_version":1,"run_id":"run-final-review","ticket":15,"role":"reviewer",
                "status":"completed","summary":"Final review found the integration boundaries correct.",
                "reviewed_commit":head,"verdict":"approved","unresolved_findings":[],
                "known_limitations":["The test used a disposable local GitHub endpoint."],
                "final_feature_review":{
                    "scope":"complete_feature","map":"example/project#42",
                    "base_ref":"origin/develop","base_commit":review_base,
                    "reviewed_commit":head,"spec":"none-linked",
                    "accepted_decisions_reviewed":true
                }
            })).unwrap(),
        )
        .unwrap();
        let mut requested_changes: serde_json::Value =
            serde_json::from_slice(&fs::read(&evidence).unwrap()).unwrap();
        requested_changes["verdict"] = json!("changes_requested");
        requested_changes["unresolved_findings"] =
            json!(["The final feature still has a required defect."]);
        fs::write(&evidence, serde_json::to_vec(&requested_changes).unwrap()).unwrap();
        assert!(
            mark_ready_with(
                &state_dir,
                &repo,
                "example/project#42",
                &ready_workers,
                "run-final-review",
                &head,
                |_| panic!("changes-requested final review must block checks and readiness"),
                &gh,
            )
            .is_err()
        );
        assert!(read(&state_dir).unwrap().ready_commit.is_none());
        requested_changes["verdict"] = json!("approved");
        requested_changes["unresolved_findings"] = json!([]);
        fs::write(&evidence, serde_json::to_vec(&requested_changes).unwrap()).unwrap();
        store::create_scheduler_decision(
            &mut ready_workers,
            store::NewSchedulerDecision {
                ticket: 15,
                run_id: "run-completed-worker",
                source_run_id: "run-completed-worker",
                kind: store::SchedulerDecisionKind::ConflictExhaustion,
                blocked_status: WorkerStatus::Completed,
                base_commit: Some("feature-target"),
                question: "Choose whether to accept the documented limitation.",
            },
        )
        .unwrap();
        let pending_decision = mark_ready_with(
            &state_dir,
            &repo,
            "example/project#42",
            &ready_workers,
            "run-final-review",
            &head,
            |_| panic!("pending scheduler decision must block readiness before checks"),
            &gh,
        )
        .unwrap_err();
        assert!(
            pending_decision
                .to_string()
                .contains("scheduler decision awaits an explicit human action")
        );
        ready_workers.scheduler_decisions.clear();
        let mut closed_children = read(&state_dir).unwrap();
        let mut closed_task = open_child(16, "Externally closed implementation", true);
        closed_task.state = "closed".into();
        closed_children
            .map_children
            .as_mut()
            .unwrap()
            .push(closed_task);
        save(&state_dir, &closed_children).unwrap();
        let blocked = mark_ready_with(
            &state_dir,
            &repo,
            "example/project#42",
            &ready_workers,
            "run-final-review",
            &head,
            |_| panic!("readiness gate must reject closed child before checks"),
            &gh,
        );
        assert!(blocked.is_err());
        assert!(read(&state_dir).unwrap().ready_commit.is_none());
        closed_children.map_children.as_mut().unwrap().pop();
        save(&state_dir, &closed_children).unwrap();
        mark_ready_with(
            &state_dir,
            &repo,
            "example/project#42",
            &ready_workers,
            "run-final-review",
            &head,
            |_| {
                Ok(vec![CheckEvidence {
                    commit: head.clone(),
                    command: "cargo test --locked --all-targets".into(),
                    result: "passed (exit 0)".into(),
                }])
            },
            &gh,
        )
        .unwrap();
        let ready = read(&state_dir).unwrap();
        assert_eq!(ready.ready_commit.as_deref(), Some(head.as_str()));
        assert!(!ready.draft_pr.unwrap().draft);
        ensure_draft_pr_with_gh(&state_dir, &repo, "example/project#42", &ready_workers, &gh)
            .unwrap();
        let calls: serde_json::Value =
            serde_json::from_slice(&fs::read(temp.path().join("pr.json")).unwrap()).unwrap();
        assert!(
            calls["body"]
                .as_str()
                .unwrap()
                .contains("Final review found the integration boundaries correct.")
        );
        assert!(
            calls["body"]
                .as_str()
                .unwrap()
                .contains("The test used a disposable local GitHub endpoint.")
        );
        assert!(
            calls["body"]
                .as_str()
                .unwrap()
                .contains("**Door:** Two-way")
        );
        let ready = read(&state_dir).unwrap();
        assert_eq!(ready.final_checks.len(), 1);

        // A legacy/narrow final review cannot keep a ready PR. The durable
        // readiness intent returns it to draft and verifies GitHub's result.
        let accepted_review_evidence = fs::read(&evidence).unwrap();
        let mut narrow_review: serde_json::Value =
            serde_json::from_slice(&accepted_review_evidence).unwrap();
        narrow_review
            .as_object_mut()
            .unwrap()
            .remove("final_feature_review");
        fs::write(&evidence, serde_json::to_vec(&narrow_review).unwrap()).unwrap();
        ensure_draft_pr_with_gh(&state_dir, &repo, "example/project#42", &ready_workers, &gh)
            .unwrap();
        let invalidated = read(&state_dir).unwrap();
        assert!(invalidated.ready_commit.is_none());
        assert!(invalidated.final_review_commit.is_none());
        assert!(invalidated.readiness_intent.is_none());
        assert!(invalidated.draft_pr.as_ref().unwrap().draft);
        let mut remote: serde_json::Value =
            serde_json::from_slice(&fs::read(&gh_state_path).unwrap()).unwrap();
        assert_eq!(remote["pr"]["isDraft"], true);
        fs::write(&evidence, accepted_review_evidence).unwrap();
        mark_ready_with(
            &state_dir,
            &repo,
            "example/project#42",
            &ready_workers,
            "run-final-review",
            &head,
            |_| {
                Ok(vec![CheckEvidence {
                    commit: head.clone(),
                    command: "cargo test --locked --all-targets".into(),
                    result: "passed (exit 0)".into(),
                }])
            },
            &gh,
        )
        .unwrap();

        remote = serde_json::from_slice(&fs::read(&gh_state_path).unwrap()).unwrap();
        remote["pr"]["isDraft"] = json!(true);
        remote["pr"]["headRefOid"] = json!(head);
        remote["ready_head_override"] = json!("concurrent-head-change");
        remote["redraft_fail_before_effect"] = json!(1);
        fs::write(&gh_state_path, serde_json::to_vec(&remote).unwrap()).unwrap();

        let failed_invalidation = mark_ready_with(
            &state_dir,
            &repo,
            "example/project#42",
            &ready_workers,
            "run-final-review",
            &head,
            |_| {
                Ok(vec![CheckEvidence {
                    commit: head.clone(),
                    command: "cargo test --locked --all-targets".into(),
                    result: "passed (exit 0)".into(),
                }])
            },
            &gh,
        )
        .unwrap_err();
        assert!(
            failed_invalidation
                .to_string()
                .contains("draft invalidation remains pending")
        );
        let pending = read(&state_dir).unwrap();
        assert!(pending.ready_commit.is_none());
        let intent = pending.readiness_intent.as_ref().unwrap();
        assert_eq!(intent.action, ReadinessAction::Draft);
        assert!(intent.reason.contains("post-ready verification observed"));
        assert!(chat_milestones(&state_dir).unwrap().iter().any(|line| {
            line.contains("may still be ready") && line.contains("pending remote verification")
        }));
        remote = serde_json::from_slice(&fs::read(&gh_state_path).unwrap()).unwrap();
        assert_eq!(remote["pr"]["isDraft"], false);

        // Simulate a restart, then a draft request that takes effect but loses
        // its response. The persisted intent is reconciled from the remote view.
        remote["ready_head_override"] = serde_json::Value::Null;
        remote["redraft_ambiguous_after_effect"] = json!(1);
        remote["view_fail_after_redraft"] = json!(1);
        fs::write(&gh_state_path, serde_json::to_vec(&remote).unwrap()).unwrap();
        assert!(
            ensure_draft_pr_with_gh(&state_dir, &repo, "example/project#42", &ready_workers, &gh)
                .is_err()
        );
        let still_pending = read(&state_dir).unwrap();
        assert_eq!(
            still_pending.readiness_intent.as_ref().unwrap().action,
            ReadinessAction::Draft
        );
        assert!(still_pending.ready_commit.is_none());

        // Another process observes the already-draft PR and safely resolves the
        // retained intent without repeating a potentially unsafe transition.
        ensure_draft_pr_with_gh(&state_dir, &repo, "example/project#42", &ready_workers, &gh)
            .unwrap();
        let recovered = read(&state_dir).unwrap();
        assert!(recovered.readiness_intent.is_none());
        assert!(recovered.ready_commit.is_none());
        assert!(recovered.draft_pr.as_ref().unwrap().draft);
        remote = serde_json::from_slice(&fs::read(&gh_state_path).unwrap()).unwrap();
        assert_eq!(remote["pr"]["isDraft"], true);

        // Once the remote head is again the independently reviewed commit, a
        // later explicit readiness attempt can complete normally.
        remote["pr"]["headRefOid"] = json!(head);
        remote["ready_head_override"] = serde_json::Value::Null;
        fs::write(&gh_state_path, serde_json::to_vec(&remote).unwrap()).unwrap();
        mark_ready_with(
            &state_dir,
            &repo,
            "example/project#42",
            &ready_workers,
            "run-final-review",
            &head,
            |_| {
                Ok(vec![CheckEvidence {
                    commit: head.clone(),
                    command: "cargo test --locked --all-targets".into(),
                    result: "passed (exit 0)".into(),
                }])
            },
            &gh,
        )
        .unwrap();
        assert_eq!(
            read(&state_dir).unwrap().ready_commit.as_deref(),
            Some(head.as_str())
        );

        // A failed post-ready PR read is treated as uncertain and forces a
        // confirmed return to draft before readiness is cleared locally.
        remote = serde_json::from_slice(&fs::read(&gh_state_path).unwrap()).unwrap();
        remote["pr"]["isDraft"] = json!(true);
        remote["pr"]["headRefOid"] = json!(head);
        remote["view_fail_after_ready"] = json!(1);
        fs::write(&gh_state_path, serde_json::to_vec(&remote).unwrap()).unwrap();
        assert!(
            mark_ready_with(
                &state_dir,
                &repo,
                "example/project#42",
                &ready_workers,
                "run-final-review",
                &head,
                |_| {
                    Ok(vec![CheckEvidence {
                        commit: head.clone(),
                        command: "cargo test --locked --all-targets".into(),
                        result: "passed (exit 0)".into(),
                    }])
                },
                &gh,
            )
            .is_err()
        );
        let unreadable = read(&state_dir).unwrap();
        assert!(unreadable.ready_commit.is_none());
        assert!(unreadable.readiness_intent.is_none());
        assert!(unreadable.draft_pr.as_ref().unwrap().draft);
        remote = serde_json::from_slice(&fs::read(&gh_state_path).unwrap()).unwrap();
        assert_eq!(remote["pr"]["isDraft"], true);

        mark_ready_with(
            &state_dir,
            &repo,
            "example/project#42",
            &ready_workers,
            "run-final-review",
            &head,
            |_| {
                Ok(vec![CheckEvidence {
                    commit: head.clone(),
                    command: "cargo test --locked --all-targets".into(),
                    result: "passed (exit 0)".into(),
                }])
            },
            &gh,
        )
        .unwrap();
        let develop_path = temp.path().join("develop-advance");
        command(
            &repo,
            &[
                "worktree",
                "add",
                "--detach",
                develop_path.to_str().unwrap(),
                &head,
            ],
        );
        fs::write(develop_path.join("target-only.txt"), "advanced develop\n").unwrap();
        command(&develop_path, &["add", "target-only.txt"]);
        command(
            &develop_path,
            &["commit", "--quiet", "-m", "advance develop target"],
        );
        command(
            &develop_path,
            &["push", "origin", "HEAD:refs/heads/develop"],
        );
        command(&repo, &["fetch", "origin", "develop"]);
        command(
            &repo,
            &["merge", "--no-edit", "refs/remotes/origin/develop"],
        );
        fs::write(
            repo.join("new-feature-change.txt"),
            "changed after readiness\n",
        )
        .unwrap();
        command(&repo, &["add", "new-feature-change.txt"]);
        command(
            &repo,
            &["commit", "--quiet", "-m", "concurrent feature change"],
        );
        command(&repo, &["push", "origin", "feature/delivery-test"]);
        remote = serde_json::from_slice(&fs::read(&gh_state_path).unwrap()).unwrap();
        remote["redraft_fail_before_effect"] = json!(1);
        fs::write(&gh_state_path, serde_json::to_vec(&remote).unwrap()).unwrap();
        assert!(
            ensure_draft_pr_with_gh(&state_dir, &repo, "example/project#42", &ready_workers, &gh)
                .is_err()
        );
        let changed_head = read(&state_dir).unwrap();
        assert!(changed_head.ready_commit.is_none());
        assert_eq!(
            changed_head.readiness_intent.as_ref().unwrap().action,
            ReadinessAction::Draft
        );
        assert!(
            changed_head
                .readiness_intent
                .as_ref()
                .unwrap()
                .reason
                .contains("feature head or develop target changed")
        );
        remote = serde_json::from_slice(&fs::read(&gh_state_path).unwrap()).unwrap();
        remote["redraft_ambiguous_after_effect"] = json!(1);
        fs::write(&gh_state_path, serde_json::to_vec(&remote).unwrap()).unwrap();
        ensure_draft_pr_with_gh(&state_dir, &repo, "example/project#42", &ready_workers, &gh)
            .unwrap();
        let recovered_head_change = read(&state_dir).unwrap();
        assert!(recovered_head_change.readiness_intent.is_none());
        assert!(recovered_head_change.ready_commit.is_none());
        assert!(recovered_head_change.draft_pr.as_ref().unwrap().draft);
    }

    #[test]
    fn moved_develop_target_updates_feature_and_invalidates_final_review() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        fs::create_dir(&repo).unwrap();
        command(&repo, &["init", "--quiet"]);
        command(&repo, &["config", "user.name", "Target Fixture"]);
        command(&repo, &["config", "user.email", "target@example.invalid"]);
        fs::write(repo.join("README.md"), "base\n").unwrap();
        command(&repo, &["add", "README.md"]);
        command(&repo, &["commit", "--quiet", "-m", "base"]);
        let base = command(&repo, &["rev-parse", "HEAD"]);
        command(&repo, &["branch", "develop"]);
        command(&repo, &["branch", "-M", "feature/target-update"]);
        let feature_head = command(&repo, &["rev-parse", "HEAD"]);

        let remote = temp.path().join("remote.git");
        Command::new("git")
            .args(["init", "--bare", "--quiet"])
            .arg(&remote)
            .status()
            .unwrap();
        command(
            &repo,
            &["remote", "add", "origin", remote.to_str().unwrap()],
        );
        command(
            &repo,
            &["push", "--set-upstream", "origin", "feature/target-update"],
        );
        command(&repo, &["push", "origin", "develop"]);

        let update_path = temp.path().join("develop");
        command(
            &repo,
            &[
                "worktree",
                "add",
                "--detach",
                update_path.to_str().unwrap(),
                &base,
            ],
        );
        fs::write(update_path.join("target.txt"), "new target commit\n").unwrap();
        command(&update_path, &["add", "target.txt"]);
        command(
            &update_path,
            &["commit", "--quiet", "-m", "advance develop"],
        );
        let new_base = command(&update_path, &["rev-parse", "HEAD"]);
        command(
            &repo,
            &["push", "origin", &format!("{new_base}:refs/heads/develop")],
        );

        let state_dir = temp.path().join("state");
        fs::create_dir_all(&state_dir).unwrap();
        let mut delivery = DeliveryState {
            feature_branch: Some("feature/target-update".into()),
            pr_base_commit: Some(base),
            ready_commit: Some(feature_head),
            ..DeliveryState::default()
        };
        let (synced_base, changed) = sync_develop_target_with_checks(
            &state_dir,
            &repo,
            "feature/target-update",
            &mut delivery,
            |_| Ok(()),
        )
        .unwrap();
        let updated_head = command(&repo, &["rev-parse", "refs/heads/feature/target-update"]);
        assert!(changed);
        assert_eq!(synced_base, new_base);
        assert!(is_ancestor(&repo, &synced_base, &updated_head).unwrap());
        assert_eq!(
            delivery.pr_base_commit.as_deref(),
            Some(synced_base.as_str())
        );
        assert!(delivery.ready_commit.is_none());
        assert!(delivery.final_review_commit.is_none());
        assert!(
            !state_dir
                .join("integration-worktrees")
                .join(format!("develop-update-{}", short_commit(&synced_base)))
                .exists()
        );
    }
}
