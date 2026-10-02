//! One per-map process supervises durable dispatch and reconciliation.
use crate::{
    delivery::{self, Outcome as DeliveryOutcome},
    herdr::Client,
    host,
    store::{self, Authorization, Lock, Provider, State, WorkerRun, WorkerStatus},
    tracker::{FrontierTicket, GitHub, MapRef},
};
use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const FAILURE_RETRIES: u8 = 2;
const REWORK_ROUNDS: u8 = store::DEFAULT_REWORK_ROUNDS;
const SUBMITTED_IDLE_GRACE: Duration = Duration::from_secs(5 * 60);

/// systemd supervises this process; the kernel releases its lifetime lock on death.
pub fn serve(root: &Path, key: &str, once: bool) -> Result<()> {
    let dir = store::map_dir(root, key)?;
    let _runtime_lock = Lock::acquire(&dir.join("runtime.lock"))?;
    let mut next_check = Instant::now();
    let mut failures = 0u32;
    loop {
        {
            let _state_lock = Lock::acquire_wait(&dir.join("state.lock"))?;
            let mut state = store::read_state(&dir)?;
            let before = state.history.len();
            store::process_requests(&dir, &mut state)?;
            if before != state.history.len() || Instant::now() >= next_check {
                let message = match reconcile(&dir, &mut state) {
                    Ok(message) => {
                        failures = 0;
                        message
                    }
                    Err(error) => {
                        failures = failures.saturating_add(1);
                        state.reconciled = false;
                        format!("{error:#}")
                    }
                };
                state.suspension = message;
                store::atomic_json(&dir.join("state.json"), &state)?;
                next_check = Instant::now() + Duration::from_secs(check_delay(&state, failures));
            }
        }
        if once {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(250));
    }
}

fn reconcile(dir: &Path, state: &mut State) -> Result<String> {
    host::check(&state.binding)?;
    let map = MapRef::parse(&state.map)?;
    let github = GitHub::default();
    state.canonical_linked_spec_url = github.canonical_linked_spec_url(&map)?;
    state.canonical_linked_spec_resolved = true;
    let frontier = github.reconcile(&map)?;
    let tracker_dir = dir.join("tracker");
    fs::create_dir_all(&tracker_dir)?;
    store::atomic_json(&tracker_dir.join("frontier.json"), &frontier)?;
    let herdr = Client::new(&state.binding.socket);
    let repository = state.binding.repository.clone();
    apply_scheduler_decisions(dir, state, &repository, &map)?;
    reconcile_workers(dir, state, &map, &github, &herdr)?;
    let children = github.child_tickets(&map)?;
    apply_delivery_outcome(
        dir,
        state,
        &map,
        &delivery::reconcile(dir, state, &children)?,
    )?;
    let delivery_milestones = delivery::chat_milestones(dir)?;
    let delivery_state = delivery::read(dir)?;
    let scheduler_decisions = state
        .scheduler_decisions
        .iter()
        .map(|decision| {
            let child = children
                .iter()
                .find(|child| child.number == decision.ticket);
            let ticket = delivery_state
                .tickets
                .values()
                .find(|ticket| ticket.issue == decision.ticket);
            let title = child
                .map(|child| child.title.as_str())
                .filter(|title| !title.trim().is_empty())
                .or_else(|| ticket.map(|ticket| ticket.title.as_str()))
                .filter(|title| !title.trim().is_empty())
                .unwrap_or("Wayfinder ticket with missing title metadata");
            let url = child
                .map(|child| child.url.as_str())
                .filter(|url| !url.trim().is_empty())
                .or_else(|| ticket.map(|ticket| ticket.url.as_str()))
                .filter(|url| !url.trim().is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    format!(
                        "https://github.com/{}/{}/issues/{}",
                        map.owner, map.repository, decision.ticket
                    )
                });
            crate::orchestration::SchedulerDecisionNotice {
                request_id: decision.request_id.clone(),
                ticket_link: format!("[{title}]({url})"),
                run_id: decision.run_id.clone(),
                question: decision.question.clone(),
                response: decision.response.clone(),
                action_required: decision.awaits_human_action(),
            }
        })
        .collect::<Vec<_>>();
    let chat_result = crate::orchestration::reconcile(
        dir,
        state,
        &herdr,
        &github,
        &delivery_milestones,
        &scheduler_decisions,
    );
    let chat_warning = chat_result.err().map(|error| {
        let warning = format!("orchestrator chat is held for explicit reconciliation: {error:#}");
        state.suspension = warning.clone();
        warning
    });
    state.reconciled = true;
    match state.authorization {
        Authorization::AwaitingStart => {
            return Ok(
                "Host and GitHub reconciled; explicit Start required before dispatch".into(),
            );
        }
        Authorization::Paused => return Ok("Host and GitHub reconciled; dispatch paused".into()),
        Authorization::Started => {}
    }
    if state.binding.source_workspace_id.is_none() {
        return Ok(
            "Host and GitHub reconciled; dispatch held: attach this map with its owning Herdr workspace ID".into(),
        );
    }
    if let Some(warning) = chat_warning
        .as_ref()
        .filter(|warning| warning.contains("identity"))
    {
        return Ok(format!(
            "Host and GitHub reconciled; dispatch held while {warning}"
        ));
    }
    let ready = match github.dispatch_frontier(&map, dir) {
        Ok(ready) => ready,
        Err(error) if format!("{error:#}").contains("execution override") => {
            return Ok(format!(
                "Host and GitHub reconciled; dispatch held: {error:#}"
            ));
        }
        Err(error) => return Err(error),
    };
    let repository = state.binding.repository.clone();
    let dispatch_base = git(&repository, &["rev-parse", "HEAD"])?.trim().to_owned();
    ensure!(
        is_full_commit(&dispatch_base),
        "resolved dispatch base is not a full Git commit"
    );
    queue_frontier(state, &ready, &repository, &map, &dispatch_base);
    launch_queued(dir, state, &map, &github, &herdr)?;
    let active = state
        .workers
        .runs
        .iter()
        .filter(|r| r.reserves_capacity())
        .count();
    let worker_status = format!(
        "Host and GitHub reconciled; {active}/{} worker slots reserved",
        state.concurrency
    );
    Ok(chat_warning.map_or(worker_status.clone(), |warning| {
        format!("{worker_status}; {warning}")
    }))
}

/// Apply a typed scheduler choice as one durable state transition. Queue
/// creation and the decision marker are saved together before Herdr sees any
/// launch request, so restart/replay cannot create duplicate workers.
fn apply_scheduler_decisions(
    dir: &Path,
    state: &mut State,
    repository: &Path,
    map: &MapRef,
) -> Result<()> {
    for index in 0..state.scheduler_decisions.len() {
        let decision = state.scheduler_decisions[index].clone();
        let Some(disposition) = decision.disposition else {
            continue;
        };
        if disposition == store::SchedulerDecisionDisposition::Defer {
            if decision.application != store::SchedulerDecisionApplication::Deferred {
                state.scheduler_decisions[index].application =
                    store::SchedulerDecisionApplication::Deferred;
                save(dir, state)?;
            }
            continue;
        }
        if matches!(
            decision.application,
            store::SchedulerDecisionApplication::Continued
                | store::SchedulerDecisionApplication::Abandoned
        ) {
            continue;
        }
        let blocked_index = state
            .workers
            .runs
            .iter()
            .position(|run| run.id == decision.run_id)
            .context(
                "scheduler decision's blocked run is missing; human reconciliation required",
            )?;
        let source_index = state
            .workers
            .runs
            .iter()
            .position(|run| run.id == decision.source_run_id)
            .context(
                "scheduler decision's implementation run is missing; human reconciliation required",
            )?;
        let source = state.workers.runs[source_index].clone();
        let blocked = state.workers.runs[blocked_index].clone();
        let finished_before_hold = decision.blocked_status.as_ref().is_some_and(|status| {
            matches!(status, WorkerStatus::Completed | WorkerStatus::Reviewed)
        });
        let known_held_conflict = decision.kind == store::SchedulerDecisionKind::ConflictExhaustion
            && decision.run_id == decision.source_run_id
            && source.status == WorkerStatus::NeedsHuman
            && finished_before_hold;
        let blocked_worker_finished = matches!(
            blocked.status,
            WorkerStatus::Completed | WorkerStatus::Reviewed
        ) || known_held_conflict;
        ensure!(
            blocked_worker_finished && finished_before_hold,
            "scheduler decision's blocked worker is not proven finished; inspect worker liveness before applying a choice"
        );
        ensure!(
            matches!(
                source.status,
                WorkerStatus::Completed | WorkerStatus::Reviewed
            ) || known_held_conflict,
            "scheduler decision source is not proven finished; inspect and resolve the active worker before applying a choice"
        );
        ensure!(
            source.ticket == decision.ticket && source.role == "implementer",
            "scheduler decision is no longer bound to its implementation run"
        );
        match disposition {
            store::SchedulerDecisionDisposition::Defer => unreachable!(),
            store::SchedulerDecisionDisposition::Abandon => {
                let restore = decision
                    .blocked_status
                    .clone()
                    .context("scheduler decision lacks its prior finished status")?;
                ensure!(
                    matches!(restore, WorkerStatus::Completed | WorkerStatus::Reviewed),
                    "abandon cannot release an unverified active worker"
                );
                state.workers.runs[blocked_index].status = restore;
                state.scheduler_decisions[index].application =
                    store::SchedulerDecisionApplication::Abandoned;
                save(dir, state)?;
            }
            store::SchedulerDecisionDisposition::Continue => {
                let context_marker = format!("scheduler-decision:{}", decision.request_id);
                if let Some(existing) = state.workers.runs.iter().find(|run| {
                    run.role == "implementer"
                        && run.source_run.as_deref() == Some(decision.run_id.as_str())
                        && run
                            .context
                            .as_deref()
                            .is_some_and(|context| context.contains(&context_marker))
                }) {
                    state.scheduler_decisions[index].successor_run_id = Some(existing.id.clone());
                    state.scheduler_decisions[index].application =
                        store::SchedulerDecisionApplication::Continued;
                    save(dir, state)?;
                    continue;
                }
                let base_commit = decision
                    .base_commit
                    .clone()
                    .or_else(|| source.base_commit.clone())
                    .context("scheduler decision has no fixed commit for safe rework")?;
                let prior_limit = source.rework_round_limit.max(REWORK_ROUNDS);
                ensure!(
                    source.rework_round >= prior_limit,
                    "scheduler continuation is not exhausted at its recorded rework round"
                );
                let continued_limit = source.rework_round.saturating_add(1);
                let human_response = decision
                    .response
                    .as_deref()
                    .context("scheduler disposition has no preserved human response")?;
                let work = match decision.kind {
                    store::SchedulerDecisionKind::ReviewExhaustion => format!(
                        "{context_marker}\nHuman selected one bounded implementation rework round. Rework the exact candidate {base_commit} to address the independent reviewer report: {}. Human's exact response: {human_response}. Preserve the existing attempt and evidence.",
                        decision.question,
                    ),
                    store::SchedulerDecisionKind::ConflictExhaustion => format!(
                        "{context_marker}\nHuman selected one bounded conflict-repair round. Resolve the integration conflict in a fresh detached worktree against exact feature commit {base_commit}. Human's exact response: {human_response}. Preserve prior attempts and evidence. Details: {}",
                        decision.question,
                    ),
                    store::SchedulerDecisionKind::RequiredChecksExhaustion => format!(
                        "{context_marker}\nHuman selected one bounded implementation repair round. Fix the required-check failure for exact reviewed candidate {base_commit} in a fresh detached worktree. Preserve the prior approved review and complete check-failure evidence. Human's exact response: {human_response}. Details: {}",
                        decision.question,
                    ),
                };
                new_run(
                    state,
                    NewRun {
                        ticket: decision.ticket,
                        role: "implementer",
                        repository,
                        map,
                        source_run: Some(decision.run_id.clone()),
                        base_commit: Some(base_commit),
                        context: Some(work),
                    },
                );
                let successor = state.workers.runs.last_mut().unwrap();
                successor.rework_round = source.rework_round.saturating_add(1);
                successor.rework_round_limit = continued_limit;
                successor.automatic_retries = source.automatic_retries;
                let successor_id = successor.id.clone();
                state.scheduler_decisions[index].successor_run_id = Some(successor_id);
                state.scheduler_decisions[index].application =
                    store::SchedulerDecisionApplication::Continued;
                save(dir, state)?;
            }
        }
    }
    Ok(())
}

fn apply_delivery_outcome(
    dir: &Path,
    state: &mut State,
    map: &MapRef,
    outcome: &DeliveryOutcome,
) -> Result<()> {
    let repository = state.binding.repository.clone();
    match outcome {
        DeliveryOutcome::Nothing | DeliveryOutcome::Integrated { .. } => {}
        DeliveryOutcome::FinalReviewNeeded { run_id, commit } => {
            if state.authorization != Authorization::Started {
                return Ok(());
            }
            let delivery_state = delivery::read(dir)?;
            if !state.workers.runs.iter().any(|run| {
                run.role == "reviewer"
                    && run.is_final_feature_review()
                    && store::implementation_source(&state.workers.runs, run)
                        .is_some_and(|source| source.id == *run_id)
                    && run.base_commit.as_deref() == Some(commit)
                    && !matches!(run.status, WorkerStatus::Failed | WorkerStatus::Stopped)
                    && (run.status != WorkerStatus::Completed
                        || (state.canonical_linked_spec_resolved
                            && delivery_state
                                .pr_base_commit
                                .as_deref()
                                .is_some_and(|base| {
                                    delivery::final_feature_review_scope_matches(
                                        run,
                                        &state.map,
                                        base,
                                        commit,
                                        state.canonical_linked_spec_url.as_deref(),
                                    )
                                })))
            }) {
                let source = state
                    .workers
                    .runs
                    .iter()
                    .find(|run| run.id == *run_id)
                    .context("final review source run disappeared")?;
                let ticket = source.ticket;
                let rework_round = source.rework_round;
                new_run(
                    state,
                    NewRun {
                        ticket,
                        role: "reviewer",
                        repository: &repository,
                        map,
                        source_run: Some(run_id.clone()),
                        base_commit: Some(commit.clone()),
                        context: Some(format!(
                            "wayfinder-final-feature-review\nIndependently review the complete feature against the specification at fixed commit {commit}. Include required check results, unresolved findings, and known limitations."
                        )),
                    },
                );
                state.workers.runs.last_mut().unwrap().rework_round = rework_round;
                state.workers.runs.last_mut().unwrap().purpose =
                    Some(store::RunPurpose::FinalFeatureReview);
                save(dir, state)?;
            }
        }
        DeliveryOutcome::FeatureReady { run_id, commit } => {
            if let Err(error) =
                delivery::mark_ready(dir, &repository, &state.map, state, run_id, commit)
            {
                delivery::record_ready_error(
                    dir,
                    &format!("final feature review is not ready for handoff: {error:#}"),
                )?;
            }
        }
        DeliveryOutcome::ReviewRenewal { run_id, commit } => {
            if !state.workers.runs.iter().any(|run| {
                run.role == "reviewer"
                    && run.source_run.as_deref() == Some(run_id)
                    && run.base_commit.as_deref() == Some(commit)
                    && !matches!(run.status, WorkerStatus::Failed | WorkerStatus::Stopped)
            }) {
                let source = state
                    .workers
                    .runs
                    .iter()
                    .find(|run| run.id == *run_id)
                    .context("renewed review source run disappeared")?;
                let source_round = source.rework_round;
                let source_ticket = source.ticket;
                new_run(
                    state,
                    NewRun {
                        ticket: source_ticket,
                        role: "reviewer",
                        repository: &repository,
                        map,
                        source_run: Some(run_id.clone()),
                        base_commit: Some(commit.clone()),
                        context: Some(format!(
                            "The feature branch target changed after review. Independently review the rebased candidate {commit}; the earlier approval applies only to its prior commit."
                        )),
                    },
                );
                state.workers.runs.last_mut().unwrap().rework_round = source_round;
                save(dir, state)?;
            }
        }
        DeliveryOutcome::Conflict {
            run_id,
            base_commit,
            detail,
        } => {
            let source = state
                .workers
                .runs
                .iter()
                .find(|run| run.id == *run_id)
                .context("conflicted integration source run disappeared")?
                .clone();
            if source.rework_round >= source.rework_round_limit {
                let question = format!(
                    "The three-round review/rework budget is exhausted while resolving an integration conflict. Choose one explicit action: continue grants one bounded conflict-repair round, defer keeps ticket readiness held while releasing this completed worker slot, or abandon releases this proven-finished run while leaving the ticket unintegrated. {detail}"
                );
                store::create_scheduler_decision(
                    state,
                    store::NewSchedulerDecision {
                        ticket: source.ticket,
                        run_id: &source.id,
                        source_run_id: &source.id,
                        kind: store::SchedulerDecisionKind::ConflictExhaustion,
                        blocked_status: source.status.clone(),
                        base_commit: Some(base_commit),
                        question: &question,
                    },
                )?;
                if let Some(worker) = state.workers.runs.iter_mut().find(|run| run.id == *run_id) {
                    // A scheduler choice is pending, but this source run is
                    // already proven finished; ticket readiness is held by
                    // the durable decision itself, not by consuming a slot.
                    worker.question = Some(question);
                }
            } else if !state.workers.runs.iter().any(|run| {
                run.role == "implementer"
                    && run.ticket == source.ticket
                    && run.source_run.as_deref() == Some(run_id)
                    && run.base_commit.as_deref() == Some(base_commit)
                    && !matches!(run.status, WorkerStatus::Failed | WorkerStatus::Stopped)
            }) {
                new_run(
                    state,
                    NewRun {
                        ticket: source.ticket,
                        role: "implementer",
                        repository: &repository,
                        map,
                        source_run: Some(run_id.clone()),
                        base_commit: Some(base_commit.clone()),
                        context: Some(format!(
                            "Rebase conflict while integrating the independently reviewed ticket. Resolve it in this fresh detached worktree against feature commit {base_commit}. Preserve the existing integration conflict worktree and explain the resolution. Details: {detail}"
                        )),
                    },
                );
                state.workers.runs.last_mut().unwrap().rework_round = source.rework_round + 1;
                state.workers.runs.last_mut().unwrap().rework_round_limit =
                    source.rework_round_limit;
                state.workers.runs.last_mut().unwrap().automatic_retries = source.automatic_retries;
            }
            save(dir, state)?;
        }
        DeliveryOutcome::CheckFailure {
            run_id,
            commit,
            detail,
        } => {
            let source = state
                .workers
                .runs
                .iter()
                .find(|run| run.id == *run_id)
                .context("required-check failure source run disappeared")?
                .clone();
            let failed_delivery = delivery::read(dir)?;
            let recorded_failure = failed_delivery
                .tickets
                .get(run_id)
                .is_some_and(|entry| entry.check_failure_commit.as_deref() == Some(commit));
            ensure!(
                source.role == "implementer"
                    && source.status == WorkerStatus::Reviewed
                    && recorded_failure,
                "required-check failure is not bound to a reviewed implementation commit"
            );
            let existing_rework = state.workers.runs.iter().any(|run| {
                run.role == "implementer"
                    && run.ticket == source.ticket
                    && run.source_run.as_deref() == Some(run_id)
                    && run.base_commit.as_deref() == Some(commit)
            });
            if existing_rework {
                return Ok(());
            }
            if source.rework_round < source.rework_round_limit {
                new_run(
                    state,
                    NewRun {
                        ticket: source.ticket,
                        role: "implementer",
                        repository: &repository,
                        map,
                        source_run: Some(run_id.clone()),
                        base_commit: Some(commit.clone()),
                        context: Some(format!(
                            "Required integration checks failed for exact independently reviewed candidate {commit}. Preserve the prior approved review and failed-check record, fix the confirmed failures in this fresh detached worktree, and report the actual required checks. This is confirmed check failure, not an ambiguous integration effect. Details: {detail}"
                        )),
                    },
                );
                let successor = state.workers.runs.last_mut().unwrap();
                successor.rework_round = source.rework_round + 1;
                successor.rework_round_limit = source.rework_round_limit;
                successor.automatic_retries = source.automatic_retries;
                let successor_id = successor.id.clone();
                delivery::record_check_failure_rework(dir, run_id, &successor_id, commit)?;
                save(dir, state)?;
            } else {
                let question = format!(
                    "Three automatic review/rework rounds were exhausted after confirmed required-check failures. Choose one explicit action: continue grants one bounded implementation repair round, defer keeps ticket integration held, or abandon leaves the ticket unintegrated. Failed exact candidate: {commit}. Required-check failure: {detail}"
                );
                store::create_scheduler_decision(
                    state,
                    store::NewSchedulerDecision {
                        ticket: source.ticket,
                        run_id: &source.id,
                        source_run_id: &source.id,
                        kind: store::SchedulerDecisionKind::RequiredChecksExhaustion,
                        blocked_status: WorkerStatus::Reviewed,
                        base_commit: Some(commit),
                        question: &question,
                    },
                )?;
                if let Some(worker) = state.workers.runs.iter_mut().find(|run| run.id == *run_id) {
                    worker.question = Some(question);
                }
                save(dir, state)?;
            }
        }
        DeliveryOutcome::Held { run_id, detail } => {
            state.suspension = format!("ticket delivery for {run_id} is held: {detail}");
        }
    }
    if delivery::read(dir)?
        .tickets
        .values()
        .any(|ticket| ticket.integrated_commit.is_some())
    {
        if let Err(error) = delivery::ensure_draft_pr(dir, &repository, &state.map, state) {
            eprintln!(
                "Wayfinder ticket integration succeeded; feature PR handoff remains pending: {error:#}"
            );
            delivery::record_handoff_error(dir, &format!("{error:#}"))?;
        }
    }
    Ok(())
}

fn queue_frontier(
    state: &mut State,
    frontier: &[FrontierTicket],
    repository: &Path,
    map: &MapRef,
    dispatch_base: &str,
) {
    for ticket in frontier {
        let role = if ticket.labels.iter().any(|l| l == "wayfinder:task") {
            "implementer"
        } else if ticket
            .labels
            .iter()
            .any(|l| l == "wayfinder:research" || l == "wayfinder:prototype")
        {
            "researcher"
        } else {
            continue;
        };
        if state.workers.runs.iter().any(|r| {
            r.ticket == ticket.number
                && !matches!(r.status, WorkerStatus::Failed | WorkerStatus::Stopped)
        }) {
            continue;
        }
        if state
            .workers
            .runs
            .iter()
            .filter(|r| r.ticket == ticket.number)
            .all(|r| r.status == WorkerStatus::Failed)
            && state
                .workers
                .runs
                .iter()
                .filter(|r| r.ticket == ticket.number)
                .map(|r| r.automatic_retries)
                .max()
                .unwrap_or(0)
                >= FAILURE_RETRIES
        {
            continue;
        }
        new_run(
            state,
            NewRun {
                ticket: ticket.number,
                role,
                repository,
                map,
                source_run: None,
                // Pin the bound checkout's resolved commit before launch_queued can
                // create a worktree or start an external worker.
                base_commit: Some(dispatch_base.to_owned()),
                context: Some(format!(
                    "GitHub ticket title: {}\nTicket body as read during dispatch:\n{}",
                    ticket.title, ticket.body
                )),
            },
        );
    }
}

struct NewRun<'a> {
    ticket: u64,
    role: &'a str,
    repository: &'a Path,
    map: &'a MapRef,
    source_run: Option<String>,
    base_commit: Option<String>,
    context: Option<String>,
}

fn new_run(state: &mut State, request: NewRun<'_>) {
    let NewRun {
        ticket,
        role,
        repository,
        map,
        source_run,
        base_commit,
        context,
    } = request;
    state.workers.next_run = state.workers.next_run.saturating_add(1);
    let number = state.workers.next_run;
    let id = format!("run-{number:020}");
    let path = repository
        .parent()
        .unwrap_or(repository)
        .join(format!(".wayfinder-{}-{ticket}-{number}", map.number));
    let attempt = state
        .workers
        .runs
        .iter()
        .filter(|r| r.ticket == ticket && r.role == role)
        .map(|r| r.attempt)
        .max()
        .unwrap_or(0)
        .saturating_add(1);
    state.workers.runs.push(WorkerRun {
        id,
        ticket,
        role: role.into(),
        attempt,
        automatic_retries: 0,
        rework_round: 0,
        rework_round_limit: REWORK_ROUNDS,
        status: WorkerStatus::Queued,
        worktree: path,
        workspace_id: None,
        tab_id: None,
        pane_id: None,
        base_commit,
        result_commit: None,
        summary: None,
        question: None,
        human_response: None,
        human_decision: None,
        human_request_seq: 0,
        human_request_id: None,
        human_request_kind: None,
        human_request_fingerprint: None,
        answer_request_id: None,
        answer_request_kind: None,
        answer_history: Vec::new(),
        source_run,
        claim_login: None,
        context,
        purpose: Some(store::RunPurpose::TicketWork),
        last_activity_ms: None,
        terminal_id: None,
        agent_provider: None,
        agent_session: None,
        foreground_process: None,
        result_evidence: None,
        result_evidence_history: Vec::new(),
        initial_prompt_pending: false,
        initial_prompt_acknowledged: false,
        initial_prompt_attempted: None,
        initial_prompt_reconnect_pending: false,
        known_prelaunch_failure: false,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{delivery::Outcome, store::Binding};
    use std::{path::PathBuf, process::Command};

    #[test]
    fn typed_final_review_retry_gets_complete_feature_prompt_and_scope_contract() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("repo");
        std::fs::create_dir(&repository).unwrap();
        let git = |args: &[&str]| {
            let output = Command::new("git")
                .arg("-C")
                .arg(&repository)
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap().trim().to_owned()
        };
        git(&["init", "--quiet"]);
        git(&["config", "user.name", "Final Review Fixture"]);
        git(&["config", "user.email", "review@example.invalid"]);
        std::fs::write(repository.join("README.md"), "complete feature\n").unwrap();
        git(&["add", "README.md"]);
        git(&["commit", "--quiet", "-m", "feature"]);
        let head = git(&["rev-parse", "HEAD"]);
        git(&["update-ref", "refs/remotes/origin/develop", &head]);
        let binding = Binding {
            repository: repository.clone(),
            herdr_binary: PathBuf::from("/bin/true"),
            socket: temp.path().join("owned-session.sock"),
            herdr_config: None,
            source_workspace_id: Some("workspace-parent".into()),
        };
        let (key, _) = store::attach(temp.path(), "example/project#42", binding, 30).unwrap();
        let dir = store::map_dir(temp.path(), &key).unwrap();
        let map = MapRef::parse("example/project#42").unwrap();
        let mut state = store::read_state(&dir).unwrap();
        state.canonical_linked_spec_resolved = true;
        new_run(
            &mut state,
            NewRun {
                ticket: 15,
                role: "reviewer",
                repository: &repository,
                map: &map,
                source_run: Some("run-previous-final-review".into()),
                base_commit: Some(head.clone()),
                context: Some("Human explicitly authorized this retry.".into()),
            },
        );
        let run = state.workers.runs.last_mut().unwrap();
        run.purpose = Some(store::RunPurpose::FinalFeatureReview);
        run.worktree = repository;
        let repository_for_run = state.binding.repository.clone();
        new_run(
            &mut state,
            NewRun {
                ticket: 6,
                role: "implementer",
                repository: &repository_for_run,
                map: &map,
                source_run: None,
                base_commit: None,
                context: None,
            },
        );
        state.workers.runs[1]
            .answer_history
            .push(crate::store::HumanAnswerEvidence {
                request_id: "human-run-implementer-0001".into(),
                request_kind: crate::store::HumanRequestKind::WorkerQuestion,
                response: "comparison table".into(),
                disposition: crate::store::AnswerDisposition::Submitted,
            });
        state.workers.runs[1]
            .answer_history
            .push(crate::store::HumanAnswerEvidence {
                request_id: "human-run-uncertain-0001".into(),
                request_kind: crate::store::HumanRequestKind::WorkerQuestion,
                response: "unconfirmed choice".into(),
                disposition: crate::store::AnswerDisposition::Uncertain,
            });
        let run = state.workers.runs[0].clone();

        let prompt = worker_prompt(&run, &state).unwrap();
        assert!(prompt.contains("independent COMPLETE-FEATURE review"));
        assert!(prompt.contains(&format!("origin/develop..{head}")));
        assert!(prompt.contains("every linked ticket, linked spec (if any)"));
        assert!(prompt.contains("accepted map/spec comments and human decisions"));
        assert!(prompt.contains("human-run-implementer-0001"));
        assert!(prompt.contains("exact human answer: \"comparison table\""));
        assert!(
            prompt.contains("This request is resolved; do not ask the human this question again.")
        );
        assert!(!prompt.contains("unconfirmed choice"));
        assert!(prompt.contains("final_feature_review"));
        assert!(prompt.contains("accepted_decisions_reviewed"));
        assert!(!prompt.contains("Human explicitly authorized this retry"));
    }

    #[test]
    fn only_typed_final_feature_review_runs_skip_child_ticket_claims() {
        assert!(!requires_ticket_claim(
            "reviewer",
            Some(store::RunPurpose::FinalFeatureReview)
        ));
        assert!(requires_ticket_claim(
            "reviewer",
            Some(store::RunPurpose::TicketWork)
        ));
        assert!(requires_ticket_claim(
            "implementer",
            Some(store::RunPurpose::FinalFeatureReview)
        ));
        assert!(requires_ticket_claim("reviewer", None));
    }

    #[test]
    fn final_review_retry_ancestry_prevents_duplicate_dispatch() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("repo");
        std::fs::create_dir(&repository).unwrap();
        let binding = Binding {
            repository: repository.clone(),
            herdr_binary: PathBuf::from("/bin/true"),
            socket: temp.path().join("owned-session.sock"),
            herdr_config: None,
            source_workspace_id: Some("workspace-parent".into()),
        };
        let (key, _) = store::attach(temp.path(), "example/project#42", binding, 30).unwrap();
        let dir = store::map_dir(temp.path(), &key).unwrap();
        let map = MapRef::parse("example/project#42").unwrap();
        let mut state = store::read_state(&dir).unwrap();
        state.authorization = Authorization::Started;
        new_run(
            &mut state,
            NewRun {
                ticket: 15,
                role: "implementer",
                repository: &repository,
                map: &map,
                source_run: None,
                base_commit: Some("feature-exact-sha".into()),
                context: None,
            },
        );
        let implementation = state.workers.runs.last_mut().unwrap();
        implementation.status = WorkerStatus::Completed;
        implementation.result_commit = Some("feature-exact-sha".into());
        let implementation_id = implementation.id.clone();
        new_run(
            &mut state,
            NewRun {
                ticket: 15,
                role: "reviewer",
                repository: &repository,
                map: &map,
                source_run: Some(implementation_id.clone()),
                base_commit: Some("feature-exact-sha".into()),
                context: Some("wayfinder-final-feature-review\nReview fixed commit".into()),
            },
        );
        let old_review = state.workers.runs.last_mut().unwrap();
        old_review.purpose = Some(store::RunPurpose::FinalFeatureReview);
        old_review.status = WorkerStatus::Failed;
        let old_review_id = old_review.id.clone();
        new_run(
            &mut state,
            NewRun {
                ticket: 15,
                role: "reviewer",
                repository: &repository,
                map: &map,
                source_run: Some(old_review_id),
                base_commit: Some("feature-exact-sha".into()),
                context: Some("Human explicitly authorized this retry.".into()),
            },
        );
        state.workers.runs.last_mut().unwrap().purpose =
            Some(store::RunPurpose::FinalFeatureReview);

        apply_delivery_outcome(
            &dir,
            &mut state,
            &map,
            &Outcome::FinalReviewNeeded {
                run_id: implementation_id,
                commit: "feature-exact-sha".into(),
            },
        )
        .unwrap();

        assert_eq!(
            state
                .workers
                .runs
                .iter()
                .filter(|run| run.is_final_feature_review())
                .count(),
            2,
            "a queued retry in the same final-review ancestry must reserve the review slot"
        );

        let retry = state.workers.runs.last_mut().unwrap();
        retry.status = WorkerStatus::Completed;
        let evidence = temp.path().join("narrow-ticket-review.json");
        std::fs::write(
            &evidence,
            serde_json::to_vec(&serde_json::json!({
                "format_version": 1,
                "run_id": retry.id,
                "ticket": retry.ticket,
                "role": "reviewer",
                "status": "completed",
                "summary": "Approved the child ticket README only.",
                "reviewed_commit": "feature-exact-sha",
                "verdict": "approved",
                "unresolved_findings": [],
                "known_limitations": []
            }))
            .unwrap(),
        )
        .unwrap();
        retry.result_evidence = Some(evidence);
        let implementation_run_id = state.workers.runs[0].id.clone();
        apply_delivery_outcome(
            &dir,
            &mut state,
            &map,
            &Outcome::FinalReviewNeeded {
                run_id: implementation_run_id,
                commit: "feature-exact-sha".into(),
            },
        )
        .unwrap();
        assert_eq!(state.workers.runs.len(), 4);
        assert_eq!(
            state.workers.runs.last().unwrap().purpose,
            Some(store::RunPurpose::FinalFeatureReview),
            "a completed narrow ticket review must not qualify as a complete-feature review"
        );
    }

    #[test]
    fn exhausted_conflict_persists_scheduler_decision_for_completed_worker() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("repo");
        std::fs::create_dir(&repository).unwrap();
        let binding = Binding {
            repository: repository.clone(),
            herdr_binary: PathBuf::from("/bin/true"),
            socket: temp.path().join("owned-session.sock"),
            herdr_config: None,
            source_workspace_id: Some("workspace-parent".into()),
        };
        let (key, _) = store::attach(temp.path(), "example/project#42", binding, 30).unwrap();
        let dir = store::map_dir(temp.path(), &key).unwrap();
        let map = MapRef::parse("example/project#42").unwrap();
        let mut state = store::read_state(&dir).unwrap();
        state.authorization = Authorization::Started;
        new_run(
            &mut state,
            NewRun {
                ticket: 15,
                role: "implementer",
                repository: &repository,
                map: &map,
                source_run: None,
                base_commit: Some("feature-target".into()),
                context: None,
            },
        );
        let run = state.workers.runs.last_mut().unwrap();
        run.status = WorkerStatus::Completed;
        run.rework_round = REWORK_ROUNDS;
        let run_id = run.id.clone();
        apply_delivery_outcome(
            &dir,
            &mut state,
            &map,
            &Outcome::Conflict {
                run_id: run_id.clone(),
                base_commit: "feature-target".into(),
                detail: "the same file conflicts again".into(),
            },
        )
        .unwrap();

        let persisted = store::read_state(&dir).unwrap();
        let decision = persisted.scheduler_decisions.first().unwrap();
        assert_eq!(
            decision.request_kind,
            store::HumanRequestKind::SchedulerDecision
        );
        assert_eq!(decision.run_id, run_id);
        assert_eq!(decision.ticket, 15);
        assert!(
            decision
                .question
                .contains("three-round review/rework budget")
        );
        let request_id = decision.request_id.clone();
        let question = decision.question.clone();
        let mut legacy = persisted.clone();
        let legacy_request_id = legacy.scheduler_decisions[0].request_id.clone();
        legacy.scheduler_decisions[0].source_run_id.clear();
        legacy.scheduler_decisions[0].blocked_status = None;
        legacy.scheduler_decisions[0].base_commit = None;
        legacy.scheduler_decisions[0].response = Some("Legacy freeform answer only".into());
        let recovered_id = store::create_scheduler_decision(
            &mut legacy,
            store::NewSchedulerDecision {
                ticket: 15,
                run_id: &run_id,
                source_run_id: &run_id,
                kind: store::SchedulerDecisionKind::ConflictExhaustion,
                blocked_status: WorkerStatus::Completed,
                base_commit: Some("feature-target"),
                question: &format!("{question} Choose one explicit action."),
            },
        )
        .unwrap();
        assert_eq!(recovered_id, legacy_request_id);
        assert_eq!(legacy.scheduler_decisions.len(), 1);
        assert_eq!(
            legacy.scheduler_decisions[0].response.as_deref(),
            Some("Legacy freeform answer only")
        );
        assert_eq!(legacy.scheduler_decisions[0].source_run_id, run_id);
        assert_eq!(
            legacy.scheduler_decisions[0].base_commit.as_deref(),
            Some("feature-target")
        );
        assert!(legacy.scheduler_decisions[0].awaits_human_action());
        store::record_scheduler_decision_response(
            &mut state,
            &request_id,
            "Please repair the conflict once more.",
            store::SchedulerDecisionDisposition::Continue,
        )
        .unwrap();
        save(&dir, &state).unwrap();
        let mut restarted = store::read_state(&dir).unwrap();
        apply_scheduler_decisions(&dir, &mut restarted, &repository, &map).unwrap();
        let after_apply = store::read_state(&dir).unwrap();
        let decision = &after_apply.scheduler_decisions[0];
        assert_eq!(
            decision.application,
            store::SchedulerDecisionApplication::Continued
        );
        let successor_id = decision.successor_run_id.as_deref().unwrap();
        let successor = after_apply
            .workers
            .runs
            .iter()
            .find(|run| run.id == successor_id)
            .unwrap();
        assert_eq!(successor.role, "implementer");
        assert_eq!(successor.status, WorkerStatus::Queued);
        assert_eq!(successor.rework_round, REWORK_ROUNDS + 1);
        assert_eq!(successor.rework_round_limit, REWORK_ROUNDS + 1);
        assert_eq!(successor.source_run.as_deref(), Some(run_id.as_str()));
        assert!(
            successor
                .context
                .as_deref()
                .unwrap()
                .contains("Human's exact response: Please repair the conflict once more.")
        );
        let count = after_apply.workers.runs.len();
        let answer_count = decision.answers.len();
        store::record_scheduler_decision_response(
            &mut restarted,
            &request_id,
            "Please repair the conflict once more.",
            store::SchedulerDecisionDisposition::Continue,
        )
        .unwrap();
        assert_eq!(restarted.scheduler_decisions[0].answers.len(), answer_count);
        assert!(
            store::record_scheduler_decision_response(
                &mut restarted,
                &request_id,
                "Actually defer this.",
                store::SchedulerDecisionDisposition::Defer,
            )
            .is_err()
        );
        let mut restarted_again = store::read_state(&dir).unwrap();
        apply_scheduler_decisions(&dir, &mut restarted_again, &repository, &map).unwrap();
        assert_eq!(store::read_state(&dir).unwrap().workers.runs.len(), count);
        assert_eq!(
            decision.response.as_deref(),
            Some("Please repair the conflict once more.")
        );
        assert_eq!(persisted.workers.runs[0].status, WorkerStatus::Completed);
        assert_eq!(persisted.workers.runs[0].human_request_id, None);
    }

    #[test]
    fn confirmed_required_check_failure_queues_one_bounded_rework_and_preserves_review() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("repo");
        std::fs::create_dir(&repository).unwrap();
        let binding = Binding {
            repository: repository.clone(),
            herdr_binary: PathBuf::from("/bin/true"),
            socket: temp.path().join("owned-session.sock"),
            herdr_config: None,
            source_workspace_id: Some("workspace-parent".into()),
        };
        let (key, _) = store::attach(temp.path(), "example/project#42", binding, 30).unwrap();
        let dir = store::map_dir(temp.path(), &key).unwrap();
        let map = MapRef::parse("example/project#42").unwrap();
        let mut state = store::read_state(&dir).unwrap();
        new_run(
            &mut state,
            NewRun {
                ticket: 15,
                role: "implementer",
                repository: &repository,
                map: &map,
                source_run: None,
                base_commit: Some("base".into()),
                context: None,
            },
        );
        let source = state.workers.runs.last_mut().unwrap();
        source.status = WorkerStatus::Reviewed;
        source.result_commit = Some("candidate-with-failed-checks".into());
        source.summary = Some("implemented and independently approved".into());
        let source_id = source.id.clone();
        let review = delivery::ReviewSummary {
            commit: "candidate-with-failed-checks".into(),
            summary: "independent approval retained".into(),
            unresolved_findings: vec![],
            known_limitations: vec![],
        };
        let mut delivery_state = delivery::DeliveryState::default();
        delivery_state.tickets.insert(
            source_id.clone(),
            delivery::TicketDelivery {
                reviewed_commit: Some("candidate-with-failed-checks".into()),
                candidate_commit: Some("candidate-with-failed-checks".into()),
                check_failure_commit: Some("candidate-with-failed-checks".into()),
                issue: 15,
                title: "Add exact parser".into(),
                url: "https://github.com/example/project/issues/15".into(),
                last_error: Some("cargo test --locked --all-targets failed: no Cargo.lock".into()),
                review: Some(review.clone()),
                ..delivery::TicketDelivery::default()
            },
        );
        store::atomic_json(&dir.join("delivery.json"), &delivery_state).unwrap();
        let failure = DeliveryOutcome::CheckFailure {
            run_id: source_id.clone(),
            commit: "candidate-with-failed-checks".into(),
            detail: "cargo test --locked --all-targets failed: no Cargo.lock".into(),
        };

        apply_delivery_outcome(&dir, &mut state, &map, &failure).unwrap();
        let first = store::read_state(&dir).unwrap();
        assert_eq!(first.workers.runs.len(), 2);
        let rework = &first.workers.runs[1];
        assert_eq!(rework.role, "implementer");
        assert_eq!(rework.status, WorkerStatus::Queued);
        assert_eq!(rework.source_run.as_deref(), Some(source_id.as_str()));
        assert_eq!(
            rework.base_commit.as_deref(),
            Some("candidate-with-failed-checks")
        );
        assert_eq!(rework.rework_round, 1);
        assert!(rework.context.as_deref().unwrap().contains("no Cargo.lock"));
        let retained = delivery::read(&dir).unwrap();
        let prior = retained.tickets.get(&source_id).unwrap();
        assert_eq!(prior.review.as_ref(), Some(&review));
        assert_eq!(
            prior.check_failure_commit.as_deref(),
            Some("candidate-with-failed-checks")
        );
        assert!(
            prior
                .superseded_by
                .as_deref()
                .is_some_and(|id| id == rework.id)
        );

        apply_delivery_outcome(&dir, &mut state, &map, &failure).unwrap();
        assert_eq!(store::read_state(&dir).unwrap().workers.runs.len(), 2);
    }

    #[test]
    fn abandon_releases_only_a_proven_finished_run_and_keeps_artifacts() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("repo");
        std::fs::create_dir(&repository).unwrap();
        let binding = Binding {
            repository: repository.clone(),
            herdr_binary: PathBuf::from("/bin/true"),
            socket: temp.path().join("owned-session.sock"),
            herdr_config: None,
            source_workspace_id: Some("workspace-parent".into()),
        };
        let (key, _) = store::attach(temp.path(), "example/project#42", binding, 30).unwrap();
        let dir = store::map_dir(temp.path(), &key).unwrap();
        let map = MapRef::parse("example/project#42").unwrap();
        let mut state = store::read_state(&dir).unwrap();
        new_run(
            &mut state,
            NewRun {
                ticket: 15,
                role: "implementer",
                repository: &repository,
                map: &map,
                source_run: None,
                base_commit: Some("feature-target".into()),
                context: None,
            },
        );
        let run = state.workers.runs.last_mut().unwrap();
        run.status = WorkerStatus::Completed;
        run.rework_round = REWORK_ROUNDS;
        run.result_commit = Some("ticket-commit".into());
        let evidence = temp.path().join("preserved-result.json");
        std::fs::write(&evidence, "evidence").unwrap();
        run.result_evidence = Some(evidence.clone());
        let run_id = run.id.clone();
        apply_delivery_outcome(
            &dir,
            &mut state,
            &map,
            &Outcome::Conflict {
                run_id: run_id.clone(),
                base_commit: "feature-target".into(),
                detail: "cannot apply cleanly".into(),
            },
        )
        .unwrap();
        let request_id = state.scheduler_decisions[0].request_id.clone();
        store::record_scheduler_decision_response(
            &mut state,
            &request_id,
            "Leave this ticket incomplete for now.",
            store::SchedulerDecisionDisposition::Abandon,
        )
        .unwrap();
        save(&dir, &state).unwrap();
        let mut restarted = store::read_state(&dir).unwrap();
        apply_scheduler_decisions(&dir, &mut restarted, &repository, &map).unwrap();
        let abandoned = store::read_state(&dir).unwrap();
        let worker = &abandoned.workers.runs[0];
        assert_eq!(worker.status, WorkerStatus::Completed);
        assert!(!worker.reserves_capacity());
        assert_eq!(worker.result_commit.as_deref(), Some("ticket-commit"));
        assert_eq!(worker.result_evidence.as_deref(), Some(evidence.as_path()));
        assert_eq!(
            abandoned.scheduler_decisions[0].application,
            store::SchedulerDecisionApplication::Abandoned
        );
        assert!(!abandoned.scheduler_decisions[0].awaits_human_action());
    }

    #[test]
    fn scheduler_abandon_refuses_when_worker_liveness_is_uncertain() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("repo");
        std::fs::create_dir(&repository).unwrap();
        let binding = Binding {
            repository: repository.clone(),
            herdr_binary: PathBuf::from("/bin/true"),
            socket: temp.path().join("owned-session.sock"),
            herdr_config: None,
            source_workspace_id: Some("workspace-parent".into()),
        };
        let (key, _) = store::attach(temp.path(), "example/project#42", binding, 30).unwrap();
        let dir = store::map_dir(temp.path(), &key).unwrap();
        let map = MapRef::parse("example/project#42").unwrap();
        let mut state = store::read_state(&dir).unwrap();
        new_run(
            &mut state,
            NewRun {
                ticket: 15,
                role: "implementer",
                repository: &repository,
                map: &map,
                source_run: None,
                base_commit: Some("feature-target".into()),
                context: None,
            },
        );
        let run = state.workers.runs.last_mut().unwrap();
        run.status = WorkerStatus::Completed;
        run.rework_round = REWORK_ROUNDS;
        let run_id = run.id.clone();
        apply_delivery_outcome(
            &dir,
            &mut state,
            &map,
            &Outcome::Conflict {
                run_id: run_id.clone(),
                base_commit: "feature-target".into(),
                detail: "conflict".into(),
            },
        )
        .unwrap();
        let request_id = state.scheduler_decisions[0].request_id.clone();
        store::record_scheduler_decision_response(
            &mut state,
            &request_id,
            "I cannot confirm the worker stopped.",
            store::SchedulerDecisionDisposition::Abandon,
        )
        .unwrap();
        state.workers.runs[0].status = WorkerStatus::Uncertain;
        let error = apply_scheduler_decisions(&dir, &mut state, &repository, &map).unwrap_err();
        assert!(error.to_string().contains("not proven finished"));
        assert!(state.workers.runs[0].reserves_capacity());
        assert_eq!(
            state.scheduler_decisions[0].application,
            store::SchedulerDecisionApplication::Awaiting
        );
    }
}

fn reconcile_workers(
    dir: &Path,
    state: &mut State,
    map: &MapRef,
    github: &GitHub,
    herdr: &Client,
) -> Result<()> {
    recover_reviewed_implementation_bases(dir, state)?;
    reconcile_approved_review_ancestry(dir, state)?;
    recover_open_intents(dir, state, herdr)?;
    let ids: Vec<_> = state
        .workers
        .runs
        .iter()
        .filter(|r| {
            matches!(
                r.status,
                WorkerStatus::Running
                    | WorkerStatus::AgentIntent
                    | WorkerStatus::PromptIntent
                    | WorkerStatus::AnswerIntent
                    | WorkerStatus::StopRequested
            ) || (r.status == WorkerStatus::NeedsHuman
                && r.human_request_kind == Some(crate::store::HumanRequestKind::HerdrBlockedUi))
                || (r.status == WorkerStatus::Uncertain
                    && (r.initial_prompt_reconnect_pending
                        || (r.initial_prompt_pending && r.initial_prompt_attempted == Some(false))
                        || (r.result_evidence.is_some()
                            && r.answer_request_kind
                                == Some(crate::store::HumanRequestKind::WorkerQuestion)
                            && r.answer_history.last().is_some_and(|answer| {
                                answer.disposition == crate::store::AnswerDisposition::Submitted
                                    && answer.request_id
                                        == r.answer_request_id.as_deref().unwrap_or_default()
                                    && r.human_response.as_deref() == Some(answer.response.as_str())
                            }))))
        })
        .map(|r| r.id.clone())
        .collect();
    for id in ids {
        let i = state.workers.runs.iter().position(|r| r.id == id).unwrap();
        let run = state.workers.runs[i].clone();
        if run.status == WorkerStatus::StopRequested {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].question = Some("Stop intent survived restart before a confirmed response; explicit stop is never retried automatically. Inspect the owned pane before making a human retry/abandon decision.".into());
            save(dir, state)?;
            continue;
        }
        if matches!(
            run.status,
            WorkerStatus::AgentIntent | WorkerStatus::PromptIntent | WorkerStatus::AnswerIntent
        ) {
            if let Some(pane) = run.pane_id.as_deref() {
                let _ = herdr.agent(pane);
            }
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            if let Some(answer) = state.workers.runs[i].answer_history.last_mut()
                && answer.disposition == crate::store::AnswerDisposition::Intent
            {
                answer.disposition = crate::store::AnswerDisposition::Uncertain;
            }
            state.workers.runs[i].question = Some("A Herdr start or prompt may have taken effect before restart; it was not repeated. Inspect the owned pane and explicitly reconcile this run.".into());
            save(dir, state)?;
            continue;
        }
        let Some(pane) = run.pane_id.as_deref() else {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            save(dir, state)?;
            continue;
        };
        let info = match herdr.agent(pane) {
            Ok(info) => info,
            Err(error) => {
                state.workers.runs[i].status = WorkerStatus::Uncertain;
                state.workers.runs[i].question = Some(format!(
                    "Could not confirm worker presence; capacity remains reserved: {error:#}"
                ));
                save(dir, state)?;
                continue;
            }
        };
        if let Err(error) = crate::herdr::verify_worker_identity(&info, &run) {
            if !run.initial_prompt_pending && !run.initial_prompt_acknowledged {
                state.workers.runs[i].status = WorkerStatus::Uncertain;
                state.workers.runs[i].question = Some(format!(
                    "The pane no longer proves this run's terminal and agent-session identity; no further effect was sent and capacity remains reserved: {error:#}"
                ));
                save(dir, state)?;
                continue;
            }
            if let Err(process_error) = crate::herdr::verify_pre_prompt_agent_identity(&info, &run)
            {
                state.workers.runs[i].status = WorkerStatus::Uncertain;
                state.workers.runs[i].question = Some(format!(
                    "The worker's process identity could not be verified, so no further effect was sent: {process_error:#}"
                ));
                save(dir, state)?;
                continue;
            }
        }
        let process_info = match herdr.pane_process_info(pane) {
            Ok(info) => info,
            Err(error) => {
                state.workers.runs[i].status = WorkerStatus::Uncertain;
                state.workers.runs[i].question = Some(format!(
                    "Could not verify the original foreground process; no effect was sent and capacity remains reserved: {error:#}"
                ));
                save(dir, state)?;
                continue;
            }
        };
        if let Err(error) = crate::herdr::verify_foreground_process(&process_info, &run) {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].question = Some(format!(
                "The pane no longer proves this run's Linux process continuity; no effect was sent and capacity remains reserved: {error:#}"
            ));
            save(dir, state)?;
            continue;
        }
        if run.initial_prompt_pending
            && run.initial_prompt_attempted == Some(false)
            && run.agent_provider.is_none()
        {
            let expected_provider = state.workers.providers.for_role(&run.role).kind.clone();
            let provider = crate::herdr::capture_agent_identity(&info, &run)
                .ok()
                .and_then(|identity| identity.1);
            let worktree = run.worktree.to_str();
            let provider_process = worktree
                .ok_or_else(|| anyhow::anyhow!("worker worktree path is not valid UTF-8"))
                .and_then(|worktree| {
                    crate::herdr::capture_provider_process(
                        &process_info,
                        pane,
                        &expected_provider,
                        worktree,
                    )
                });
            if provider.as_deref() != Some(expected_provider.as_str())
                || provider_process
                    .as_ref()
                    .ok()
                    .zip(run.foreground_process.as_ref())
                    .is_none_or(|(current, original)| current != original)
            {
                state.workers.runs[i].status = WorkerStatus::Uncertain;
                state.workers.runs[i].question = Some("Herdr's provider identity was missing at launch; the pending prompt remains unsent because the exact configured provider and saved process could not both be re-established.".into());
                save(dir, state)?;
                continue;
            }
            state.workers.runs[i].agent_provider = provider;
            save(dir, state)?;
        }
        if run.initial_prompt_acknowledged && run.agent_session.is_none() {
            match crate::herdr::capture_agent_identity(&info, &run) {
                Ok((_, _, Some(session))) => {
                    state.workers.runs[i].agent_session = Some(session);
                    state.workers.runs[i].initial_prompt_acknowledged = false;
                    save(dir, state)?;
                }
                Ok((_, _, None)) => {}
                Err(error) => {
                    state.workers.runs[i].status = WorkerStatus::Uncertain;
                    state.workers.runs[i].question = Some(format!(
                        "The acknowledged task prompt is retained, but Herdr could not bind its session identity: {error:#}"
                    ));
                    save(dir, state)?;
                    continue;
                }
            }
        }
        if run.status == WorkerStatus::Uncertain && run.initial_prompt_reconnect_pending {
            state.workers.runs[i].status = WorkerStatus::Running;
            state.workers.runs[i].question = None;
        }
        if run.initial_prompt_reconnect_pending {
            state.workers.runs[i].initial_prompt_reconnect_pending = false;
            save(dir, state)?;
        }
        match status(&info).unwrap_or("unknown") {
            "working" => {
                let worker = &mut state.workers.runs[i];
                worker.last_activity_ms = Some(now_ms());
                if worker.initial_prompt_pending {
                    worker.status = WorkerStatus::NeedsHuman;
                    worker.question = Some("Herdr reports activity after the startup UI was handled, but the initial task prompt was not submitted. The task prompt will be sent after this startup activity settles to idle; reconcile continues to supervise this pane.".into());
                } else if worker.status == WorkerStatus::NeedsHuman
                    && worker.human_request_kind
                        == Some(crate::store::HumanRequestKind::HerdrBlockedUi)
                {
                    // The human interacted with the named Herdr UI outside this
                    // process. Observed activity resumes supervision; it does not
                    // claim the task is complete or synthesize an answer.
                    worker.status = WorkerStatus::Running;
                    worker.question = None;
                    worker.human_request_id = None;
                    worker.human_request_kind = None;
                    worker.human_request_fingerprint = None;
                }
                save(dir, state)?;
            }
            "blocked" => {
                let question = herdr
                    .read_recent(pane)
                    .ok()
                    .and_then(|v| v["read"]["text"].as_str().map(str::to_owned))
                    .filter(|text| !text.trim().is_empty())
                    .unwrap_or_else(|| format!("Herdr reports a blocked worker, but its current prompt could not be read. Inspect the named pane {pane} directly before responding."));
                let worker = &mut state.workers.runs[i];
                worker.status = WorkerStatus::NeedsHuman;
                worker.set_human_request(crate::store::HumanRequestKind::HerdrBlockedUi, &question);
                worker.question = Some(question);
                save(dir, state)?;
            }
            "idle" | "done" => {
                if run.initial_prompt_pending {
                    submit_initial_task_prompt(dir, state, herdr, i)?;
                    continue;
                }
                let result = match capture_result(dir, &run) {
                    Ok(Some((result, evidence))) => {
                        let worker = &mut state.workers.runs[i];
                        if let Some(previous) = worker.result_evidence.replace(evidence.clone())
                            && previous != evidence
                            && !worker.result_evidence_history.contains(&previous)
                        {
                            worker.result_evidence_history.push(previous);
                        }
                        save(dir, state)?;
                        result
                    }
                    Ok(None) => {
                        let within_grace = run.last_activity_ms.is_some_and(|last| {
                            now_ms().saturating_sub(last) < SUBMITTED_IDLE_GRACE.as_millis() as u64
                        });
                        if within_grace {
                            // Prompt submission is confirmed. An early idle/done
                            // snapshot can race visible startup; retain the run and
                            // let later polls observe activity without relaunching it.
                            continue;
                        }
                        state.workers.runs[i].status = WorkerStatus::Uncertain;
                        state.workers.runs[i].question = Some("Worker remained idle/done without a result after the bounded submission grace; lifecycle status is not task success and the launch was not retried.".into());
                        save(dir, state)?;
                        continue;
                    }
                    Err(error) => {
                        state.workers.runs[i].status = WorkerStatus::Uncertain;
                        state.workers.runs[i].question = Some(format!(
                            "Worker result could not be validated or durably retained; it was not accepted: {error:#}"
                        ));
                        save(dir, state)?;
                        continue;
                    }
                };
                if let Some(login) = run.claim_login.as_deref() {
                    if let Err(error) = github.confirm_claim(map, run.ticket, login) {
                        state.workers.runs[i].status = WorkerStatus::NeedsHuman;
                        state.workers.runs[i].question = Some(format!(
                            "Ticket ownership changed before result acceptance: {error:#}"
                        ));
                        save(dir, state)?;
                        continue;
                    }
                }
                accept_result(dir, state, i, run, result)?;
            }
            status => {
                state.workers.runs[i].status = WorkerStatus::Uncertain;
                state.workers.runs[i].question = Some(format!(
                    "Herdr reports worker status {status}; this does not prove task completion or absence. The run remains retained and reserves capacity."
                ));
                save(dir, state)?;
            }
        }
    }
    Ok(())
}

/// Repair approval state when an older runtime completed a retried reviewer
/// against an intermediate reviewer rather than its implementation ancestor.
/// The archived result and clean, exact-commit checkout must still prove the
/// approval; names and status alone are not sufficient.
fn reconcile_approved_review_ancestry(dir: &Path, state: &mut State) -> Result<()> {
    let reviews: Vec<_> = state
        .workers
        .runs
        .iter()
        .filter(|run| run.role == "reviewer" && run.status == WorkerStatus::Completed)
        .cloned()
        .collect();
    for review in reviews {
        let Some(source) = store::implementation_source(&state.workers.runs, &review).cloned()
        else {
            continue;
        };
        if source.status != WorkerStatus::Completed {
            continue;
        }
        let (Some(target), Some(evidence)) = (
            review.base_commit.as_deref(),
            review.result_evidence.as_ref(),
        ) else {
            continue;
        };
        if source.result_commit.as_deref() != Some(target) {
            continue;
        }
        let bytes = fs::read(evidence)
            .with_context(|| format!("read retained review evidence for {}", review.id))?;
        let result = decode_result(&bytes)?;
        validate_result_identity(&result, &review)?;
        ensure!(
            result.reviewed_commit.as_deref() == Some(target),
            "retained review {} targets a different commit",
            review.id
        );
        ensure!(
            git(&review.worktree, &["rev-parse", "HEAD"])?.trim() == target
                && git(&review.worktree, &["status", "--porcelain"])?
                    .trim()
                    .is_empty(),
            "retained reviewer checkout for {} no longer proves the pinned commit",
            review.id
        );
        if result.verdict.as_deref() == Some("approved")
            && result
                .unresolved_findings
                .as_deref()
                .is_some_and(|findings| findings.is_empty())
        {
            let implementation = state
                .workers
                .runs
                .iter_mut()
                .find(|run| run.id == source.id)
                .context("review implementation ancestor disappeared")?;
            implementation.status = WorkerStatus::Reviewed;
            save(dir, state)?;
        }
    }
    Ok(())
}

/// Recover legacy reviewed implementations that predate dispatch-time base pinning.
/// The base is accepted only from the owned detached worktree's initial HEAD
/// reflog entry, with the retained candidate still at HEAD and descending from it.
fn recover_reviewed_implementation_bases(dir: &Path, state: &mut State) -> Result<()> {
    let candidates: Vec<_> = state
        .workers
        .runs
        .iter()
        .filter(|run| {
            run.role == "implementer"
                && run.status == WorkerStatus::Reviewed
                && run.base_commit.is_none()
                && run.result_commit.as_deref().is_some_and(is_full_commit)
        })
        .map(|run| run.id.clone())
        .collect();
    for id in candidates {
        let index = state
            .workers
            .runs
            .iter()
            .position(|run| run.id == id)
            .unwrap();
        let run = state.workers.runs[index].clone();
        validate_detached_worktree(&state.binding.repository, &run.worktree)
            .context("validate legacy implementation worktree before base recovery")?;
        let candidate = run.result_commit.as_deref().unwrap();
        ensure!(
            git(&run.worktree, &["rev-parse", "HEAD"])?.trim() == candidate,
            "cannot recover base for {id}: retained worktree HEAD differs from reviewed candidate"
        );
        let reflog = git(&run.worktree, &["reflog", "show", "--format=%H", "HEAD"])?;
        let entries: Vec<_> = reflog.lines().collect();
        ensure!(
            entries.len() >= 2,
            "cannot recover base for {id}: retained HEAD reflog does not prove a prior checkout commit"
        );
        // `reflog show` is newest-first. Its oldest retained HEAD is the
        // worktree's checkout base; an absent/expired reflog is not guessed.
        let initial = entries
            .last()
            .context("cannot recover base: retained worktree has no HEAD reflog")?;
        ensure!(
            is_full_commit(initial),
            "cannot recover base for {id}: initial reflog entry is not a full commit"
        );
        ensure!(
            Command::new("git")
                .args(["-C"])
                .arg(&run.worktree)
                .args(["merge-base", "--is-ancestor", initial, candidate])
                .status()
                .context("verify candidate ancestry from reflog base")?
                .success(),
            "cannot recover base for {id}: candidate is not descended from initial reflog commit"
        );
        state.workers.runs[index].base_commit = Some((*initial).to_owned());
        save(dir, state)?;
    }
    Ok(())
}

fn is_full_commit(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Recover after the detached checkout exists and `open_intent` was persisted. Herdr
/// creation is never replayed from this stage; existing session resources are inspected.
fn recover_open_intents(dir: &Path, state: &mut State, herdr: &Client) -> Result<()> {
    let ids: Vec<_> = state
        .workers
        .runs
        .iter()
        .filter(|r| r.status == WorkerStatus::OpenIntent)
        .map(|r| r.id.clone())
        .collect();
    for id in ids {
        let i = state.workers.runs.iter().position(|r| r.id == id).unwrap();
        let path = state.workers.runs[i].worktree.clone();
        if validate_detached_worktree(&state.binding.repository, &path).is_err() {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].question = Some(
                "Detached worktree identity could not be verified; Herdr open was not retried."
                    .into(),
            );
            save(dir, state)?;
            continue;
        }
        match opened_worktree(herdr, &path) {
            Ok(Some((workspace, tab, pane))) => {
                state.workers.runs[i].workspace_id = Some(workspace);
                state.workers.runs[i].tab_id = Some(tab);
                state.workers.runs[i].pane_id = Some(pane);
                state.workers.runs[i].status = WorkerStatus::AgentStartReady;
            }
            Ok(None) => {
                state.workers.runs[i].status = WorkerStatus::Uncertain;
                state.workers.runs[i].question = Some("Herdr worktree.open outcome is ambiguous and no unique opened workspace was found; inspect the session before retrying.".into());
            }
            Err(error) => {
                state.workers.runs[i].status = WorkerStatus::Uncertain;
                state.workers.runs[i].question = Some(format!(
                    "Could not reconcile opened worktree identity; no retry was attempted: {error:#}"
                ));
            }
        }
        save(dir, state)?;
    }
    Ok(())
}

fn opened_worktree(herdr: &Client, path: &Path) -> Result<Option<(String, String, String)>> {
    let listing = herdr.worktree_list(path)?;
    let path = fs::canonicalize(path)?;
    let matches: Vec<_> = listing["worktrees"]
        .as_array()
        .context("worktree.list omitted worktrees")?
        .iter()
        .filter(|tree| {
            tree["path"]
                .as_str()
                .and_then(|p| fs::canonicalize(p).ok())
                .as_deref()
                == Some(path.as_path())
        })
        .collect();
    ensure!(
        matches.len() <= 1,
        "multiple herdr worktree records match the owned checkout"
    );
    let Some(worktree) = matches.first() else {
        return Ok(None);
    };
    let Some(workspace) = worktree["open_workspace_id"].as_str() else {
        return Ok(None);
    };
    let panes = herdr.pane_list(workspace)?;
    let matches: Vec<_> = panes["panes"]
        .as_array()
        .context("pane.list omitted panes")?
        .iter()
        .filter(|pane| {
            pane["cwd"]
                .as_str()
                .or_else(|| pane["foreground_cwd"].as_str())
                .and_then(|cwd| fs::canonicalize(cwd).ok())
                .as_deref()
                == Some(path.as_path())
        })
        .collect();
    ensure!(
        matches.len() == 1,
        "opened worktree does not have exactly one matching root pane"
    );
    let pane = matches[0];
    Ok(Some((
        workspace.to_owned(),
        pane["tab_id"]
            .as_str()
            .context("pane omitted tab ID")?
            .to_owned(),
        pane["pane_id"]
            .as_str()
            .context("pane omitted pane ID")?
            .to_owned(),
    )))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerResult {
    format_version: u32,
    run_id: String,
    ticket: u64,
    role: String,
    status: String,
    summary: String,
    #[serde(default)]
    question: Option<String>,
    #[serde(default)]
    commit: Option<String>,
    #[serde(default)]
    reviewed_commit: Option<String>,
    #[serde(default)]
    verdict: Option<String>,
    #[serde(default)]
    unresolved_findings: Option<Vec<String>>,
    #[serde(default)]
    known_limitations: Option<Vec<String>>,
    #[serde(default)]
    final_feature_review: Option<delivery::FinalFeatureReviewScope>,
}
fn decode_result(bytes: &[u8]) -> Result<WorkerResult> {
    let value: WorkerResult = serde_json::from_slice(bytes).context("decode worker result")?;
    ensure!(
        value.format_version == 1 && !value.summary.trim().is_empty(),
        "unsupported or incomplete worker result"
    );
    Ok(value)
}

fn evidence_path(dir: &Path, run_id: &str) -> Result<std::path::PathBuf> {
    let digits = run_id
        .strip_prefix("run-")
        .context("invalid worker run ID")?;
    ensure!(
        digits.len() == 20 && digits.bytes().all(|b| b.is_ascii_digit()),
        "invalid worker run ID"
    );
    Ok(dir.join("worker-results").join(format!("{run_id}.json")))
}

/// Move a validated result out of the checkout before clean-tree validation. The
/// archive is create-once and compared byte-for-byte on replay, so a crash between
/// evidence creation and state commit cannot overwrite accepted evidence.
fn capture_result(
    dir: &Path,
    run: &WorkerRun,
) -> Result<Option<(WorkerResult, std::path::PathBuf)>> {
    let source = run.worktree.join(".wayfinder-result.json");
    let evidence = run
        .result_evidence
        .clone()
        .map(Ok)
        .unwrap_or_else(|| evidence_path(dir, &run.id))?;
    let source_bytes = match fs::read(&source) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error).with_context(|| format!("read {}", source.display())),
    };
    let mut target_evidence = evidence.clone();
    let mut archived_bytes = match fs::read(&evidence) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error).with_context(|| format!("read {}", evidence.display())),
    };
    if let (Some(source_bytes), Some(previous_bytes)) =
        (source_bytes.as_ref(), archived_bytes.as_ref())
        && source_bytes != previous_bytes
    {
        let previous_result = decode_result(previous_bytes)?;
        let next_result = decode_result(source_bytes)?;
        validate_result_identity(&previous_result, run)?;
        validate_result_identity(&next_result, run)?;
        ensure!(
            answered_blocked_result(run, &previous_result),
            "source result differs from retained evidence"
        );
        let revision = run.result_evidence_history.len() + 2;
        target_evidence = evidence_revision_path(dir, &run.id, revision);
        archived_bytes = match fs::read(&target_evidence) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(error).with_context(|| format!("read {}", target_evidence.display()));
            }
        };
    }
    let bytes = match (source_bytes.as_ref(), archived_bytes.as_ref()) {
        (None, None) => return Ok(None),
        (Some(source), Some(archived)) => {
            ensure!(
                source == archived,
                "source result differs from retained evidence"
            );
            archived.clone()
        }
        (Some(source), None) => source.clone(),
        (None, Some(archived)) => archived.clone(),
    };
    let result = decode_result(&bytes)?;
    validate_result_identity(&result, run)?;
    if archived_bytes.is_none() {
        persist_evidence(&target_evidence, &bytes)?;
    }
    if source_bytes.is_some() {
        let current = fs::read(&source).context("recheck source result before removing it")?;
        ensure!(
            current == bytes,
            "source result changed while being archived"
        );
        fs::remove_file(&source).context("remove archived result from source checkout")?;
        if let Some(parent) = source.parent() {
            fs::File::open(parent)?.sync_all()?;
        }
    }
    Ok(Some((result, target_evidence)))
}

fn evidence_revision_path(dir: &Path, run_id: &str, revision: usize) -> PathBuf {
    dir.join("worker-results")
        .join(format!("{run_id}-revision-{revision:04}.json"))
}

fn validate_result_identity(result: &WorkerResult, run: &WorkerRun) -> Result<()> {
    ensure!(
        result.run_id == run.id && result.ticket == run.ticket && result.role == run.role,
        "worker result identity does not match the durable run"
    );
    Ok(())
}

fn answered_blocked_result(run: &WorkerRun, result: &WorkerResult) -> bool {
    let expected_request = format!("human-{}-{:04}", run.id, run.human_request_seq);
    result.status == "blocked"
        && result
            .question
            .as_deref()
            .is_some_and(|question| !question.trim().is_empty())
        && run.answer_request_id.as_deref() == Some(expected_request.as_str())
        && run.answer_request_kind == Some(crate::store::HumanRequestKind::WorkerQuestion)
        && run.answer_history.last().is_some_and(|answer| {
            answer.request_id == expected_request
                && answer.request_kind == crate::store::HumanRequestKind::WorkerQuestion
                && answer.disposition == crate::store::AnswerDisposition::Submitted
                && run.human_response.as_deref() == Some(answer.response.as_str())
        })
}

fn persist_evidence(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("result evidence has no parent")?;
    store::private_dir(parent)?;
    if path.exists() {
        ensure!(
            fs::read(path)? == bytes,
            "retained result evidence already differs"
        );
        return Ok(());
    }
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    std::io::Write::write_all(&mut temp, bytes)?;
    temp.as_file().sync_all()?;
    match temp.persist_noclobber(path) {
        Ok(_) => fs::File::open(parent)?.sync_all()?,
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            ensure!(
                fs::read(path)? == bytes,
                "retained result evidence already differs"
            );
        }
        Err(error) => return Err(error.error.into()),
    }
    Ok(())
}

fn accept_result(
    dir: &Path,
    state: &mut State,
    i: usize,
    run: WorkerRun,
    result: WorkerResult,
) -> Result<()> {
    if result.status == "blocked" {
        let question = result
            .question
            .context("blocked worker result omitted its question")?;
        state.workers.runs[i].status = WorkerStatus::NeedsHuman;
        state.workers.runs[i]
            .set_human_request(crate::store::HumanRequestKind::WorkerQuestion, &question);
        state.workers.runs[i].question = Some(question);
        state.workers.runs[i].summary = Some(result.summary);
        return save(dir, state);
    }
    if result.status == "failed" {
        state.workers.runs[i].status = WorkerStatus::Failed;
        state.workers.runs[i].summary = Some(result.summary.clone());
        save(dir, state)?;
        if run.automatic_retries < FAILURE_RETRIES {
            let map = MapRef::parse(&state.map)?;
            let repository = state.binding.repository.clone();
            new_run(
                state,
                NewRun {
                    ticket: run.ticket,
                    role: &run.role,
                    repository: &repository,
                    map: &map,
                    source_run: run.source_run,
                    base_commit: run.base_commit,
                    context: Some(format!("Previous confirmed failure: {}", result.summary)),
                },
            );
            let retry = state.workers.runs.last_mut().unwrap();
            retry.automatic_retries = run.automatic_retries + 1;
            retry.rework_round = run.rework_round;
        } else {
            state.workers.runs[i].question = Some("Two automatic retries were exhausted after confirmed worker failures; human reconciliation is required.".into());
        }
        return save(dir, state);
    }
    ensure!(
        result.status == "completed",
        "unknown worker artifact status"
    );
    match run.role.as_str() {
        "implementer" => {
            let commit = result
                .commit
                .as_deref()
                .context("implementation artifact omitted commit")?;
            ensure!(
                git(&run.worktree, &["rev-parse", "HEAD"])?.trim() == commit,
                "artifact commit does not match worktree HEAD"
            );
            ensure!(
                git(&run.worktree, &["status", "--porcelain"])?
                    .trim()
                    .is_empty(),
                "implementation worktree has uncommitted changes"
            );
            state.workers.runs[i].status = WorkerStatus::Completed;
            state.workers.runs[i].result_commit = Some(commit.into());
            state.workers.runs[i].summary = Some(result.summary.clone());
            save(dir, state)?;
            let map = MapRef::parse(&state.map)?;
            let repository = state.binding.repository.clone();
            let context = reviewer_context(&result.summary, &run);
            new_run(
                state,
                NewRun {
                    ticket: run.ticket,
                    role: "reviewer",
                    repository: &repository,
                    map: &map,
                    source_run: Some(run.id),
                    base_commit: Some(commit.into()),
                    context: Some(context),
                },
            );
            state.workers.runs.last_mut().unwrap().rework_round = run.rework_round;
            save(dir, state)
        }
        "reviewer" => {
            let target = run
                .base_commit
                .as_deref()
                .context("review run omitted fixed commit")?;
            ensure!(
                result.reviewed_commit.as_deref() == Some(target),
                "review artifact did not identify the fixed commit"
            );
            if run.is_final_feature_review() {
                let scope = result
                    .final_feature_review
                    .as_ref()
                    .context("final-feature reviewer omitted correlated scope evidence")?;
                let base = git(&run.worktree, &["rev-parse", "refs/remotes/origin/develop"])?
                    .trim()
                    .to_owned();
                ensure!(
                    state.canonical_linked_spec_resolved
                        && scope.matches(
                            &state.map,
                            &base,
                            target,
                            state.canonical_linked_spec_url.as_deref(),
                        ),
                    "final-feature review scope does not match the complete map, origin/develop base, accepted decisions, and pinned commit"
                );
            }
            ensure!(
                git(&run.worktree, &["rev-parse", "HEAD"])?.trim() == target,
                "reviewer worktree HEAD moved away from the pinned implementation commit"
            );
            ensure!(
                git(&run.worktree, &["status", "--porcelain"])?
                    .trim()
                    .is_empty(),
                "reviewer worktree contains edits; review result is not accepted"
            );
            let unresolved_findings = result
                .unresolved_findings
                .as_ref()
                .context("reviewer artifact omitted unresolved_findings")?;
            ensure!(
                result.known_limitations.is_some(),
                "reviewer artifact omitted known_limitations"
            );
            match result.verdict.as_deref() {
                Some("approved") if unresolved_findings.is_empty() => {
                    state.workers.runs[i].status = WorkerStatus::Completed;
                    state.workers.runs[i].summary = Some(result.summary.clone());
                    let source = store::implementation_source(&state.workers.runs, &run)
                        .context("review has no implementation ancestor")?;
                    let source_id = source.id.clone();
                    if let Some(parent) = state
                        .workers
                        .runs
                        .iter_mut()
                        .find(|candidate| candidate.id == source_id)
                    {
                        parent.status = WorkerStatus::Reviewed;
                    }
                    save(dir, state)
                }
                Some("changes_requested") | Some("approved") => {
                    state.workers.runs[i].status = WorkerStatus::Completed;
                    state.workers.runs[i].summary = Some(result.summary.clone());
                    let source = store::implementation_source(&state.workers.runs, &run)
                        .context("review has no implementation ancestor")?
                        .clone();
                    if source.rework_round < source.rework_round_limit {
                        let map = MapRef::parse(&state.map)?;
                        let repository = state.binding.repository.clone();
                        new_run(
                            state,
                            NewRun {
                                ticket: run.ticket,
                                role: "implementer",
                                repository: &repository,
                                map: &map,
                                source_run: Some(run.id),
                                base_commit: Some(target.into()),
                                context: Some(format!(
                                    "Address independent reviewer findings: {}",
                                    result.summary
                                )),
                            },
                        );
                        state.workers.runs.last_mut().unwrap().rework_round =
                            source.rework_round + 1;
                        state.workers.runs.last_mut().unwrap().rework_round_limit =
                            source.rework_round_limit;
                        state.workers.runs.last_mut().unwrap().automatic_retries =
                            source.automatic_retries;
                    } else {
                        state.workers.runs[i].status = WorkerStatus::Completed;
                        let findings = unresolved_findings.join("; ");
                        let question = format!(
                            "Three automatic review/rework rounds were exhausted. Choose one explicit action: continue grants one bounded implementation rework round, defer keeps ticket readiness held while releasing this completed reviewer slot, or abandon releases this proven-finished reviewer while leaving the ticket unintegrated. Reviewer report: {}. Unresolved findings: {}",
                            result.summary,
                            if findings.is_empty() {
                                "review remained unresolved"
                            } else {
                                &findings
                            }
                        );
                        store::create_scheduler_decision(
                            state,
                            store::NewSchedulerDecision {
                                ticket: run.ticket,
                                run_id: &run.id,
                                source_run_id: &source.id,
                                kind: store::SchedulerDecisionKind::ReviewExhaustion,
                                blocked_status: WorkerStatus::Completed,
                                base_commit: run.base_commit.as_deref(),
                                question: &question,
                            },
                        )?;
                        state.workers.runs[i].question = Some(question);
                    }
                    save(dir, state)
                }
                _ => anyhow::bail!("review verdict must be approved or changes_requested"),
            }
        }
        _ => {
            state.workers.runs[i].status = WorkerStatus::Completed;
            state.workers.runs[i].summary = Some(result.summary);
            save(dir, state)
        }
    }
}

fn launch_queued(
    dir: &Path,
    state: &mut State,
    map: &MapRef,
    github: &GitHub,
    herdr: &Client,
) -> Result<()> {
    let mut intents: Vec<_> = state
        .workers
        .runs
        .iter()
        .filter(|r| {
            matches!(
                r.status,
                WorkerStatus::LaunchIntent
                    | WorkerStatus::AgentStartReady
                    | WorkerStatus::InitialPromptReady
            )
        })
        .map(|r| r.id.clone())
        .collect();
    intents.sort_by_key(|id| {
        let r = state.workers.runs.iter().find(|r| &r.id == id).unwrap();
        (if r.role == "reviewer" { 0 } else { 1 }, r.ticket)
    });
    for id in intents {
        launch_one(dir, state, map, github, herdr, &id)?;
    }
    let mut queued: Vec<_> = state
        .workers
        .runs
        .iter()
        .filter(|r| r.status == WorkerStatus::Queued)
        .map(|r| r.id.clone())
        .collect();
    queued.sort_by_key(|id| {
        let r = state.workers.runs.iter().find(|r| &r.id == id).unwrap();
        (if r.role == "reviewer" { 0 } else { 1 }, r.ticket)
    });
    for id in queued {
        let reserved = state
            .workers
            .runs
            .iter()
            .filter(|run| run.reserves_capacity())
            .count();
        if reserved >= state.concurrency as usize {
            break;
        }
        let i = state.workers.runs.iter().position(|r| r.id == id).unwrap();
        state.workers.runs[i].status = WorkerStatus::LaunchIntent;
        save(dir, state)?;
        launch_one(dir, state, map, github, herdr, &id)?;
    }
    Ok(())
}

fn launch_one(
    dir: &Path,
    state: &mut State,
    map: &MapRef,
    github: &GitHub,
    herdr: &Client,
    id: &str,
) -> Result<()> {
    let i = state.workers.runs.iter().position(|r| r.id == id).unwrap();
    let mut run = state.workers.runs[i].clone();
    if run.status == WorkerStatus::AgentStartReady {
        return start_agent_from_ready(dir, state, herdr, i);
    }
    if run.status == WorkerStatus::InitialPromptReady {
        return submit_initial_task_prompt(dir, state, herdr, i);
    }
    if run.pane_id.is_some() {
        state.workers.runs[i].status = WorkerStatus::Uncertain;
        state.workers.runs[i].question =
            Some("Launch intent already has pane resources; inspect before retrying.".into());
        return save(dir, state);
    }
    save(dir, state)?;
    if requires_ticket_claim(&run.role, run.purpose) {
        let login = match github.claim_for_runtime(map, run.ticket, dir) {
            Ok(login) => login,
            Err(e) => {
                state.workers.runs[i].status = WorkerStatus::Uncertain;
                state.workers.runs[i].known_prelaunch_failure = true;
                state.workers.runs[i].question = Some(format!(
                    "Claim outcome is uncertain; no worker launched and no worktree or pane was requested. Reconcile the retained GitHub claim before retrying: {e:#}"
                ));
                return save(dir, state);
            }
        };
        run.claim_login = Some(login);
    }
    if run.worktree.exists() {
        if validate_detached_worktree(&state.binding.repository, &run.worktree).is_err() {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].claim_login = run.claim_login;
            state.workers.runs[i].known_prelaunch_failure = true;
            state.workers.runs[i].question=Some("A path exists at the intended location but does not identify this detached worktree; no Herdr workspace or agent was requested and the existing path was retained. Reconcile the path before retrying.".into());
            return save(dir, state);
        }
    } else if let Err(error) = herdr.add_detached_worktree(
        &state.binding.repository,
        &run.worktree,
        run.base_commit.as_deref(),
    ) {
        if !run.worktree.exists()
            || validate_detached_worktree(&state.binding.repository, &run.worktree).is_err()
        {
            state.workers.runs[i].status = WorkerStatus::NeedsHuman;
            state.workers.runs[i].claim_login = run.claim_login;
            state.workers.runs[i].known_prelaunch_failure = true;
            state.workers.runs[i].question = Some(format!(
                "Detached checkout creation failed without producing the requested worktree: {error:#}. Fix the local Git/setup error, then retry explicitly with `retry-worker --confirmed-absent-or-stopped`."
            ));
            return save(dir, state);
        }
    }
    state.workers.runs[i].claim_login = run.claim_login.clone();
    state.workers.runs[i].status = WorkerStatus::OpenIntent;
    save(dir, state)?;
    let opened = herdr.open_worktree(
        &run.worktree,
        &format!("Wayfinder ticket {} {}", run.ticket, run.role),
        state
            .binding
            .source_workspace_id
            .as_deref()
            .context("attach this map with its owning Herdr workspace ID before dispatch")?,
    );
    let opened = match opened {
        Ok(value) => value,
        Err(error) => match opened_worktree(herdr, &run.worktree) {
            Ok(Some((workspace, tab, pane))) => {
                state.workers.runs[i].workspace_id = Some(workspace);
                state.workers.runs[i].tab_id = Some(tab);
                state.workers.runs[i].pane_id = Some(pane);
                state.workers.runs[i].status = WorkerStatus::AgentStartReady;
                save(dir, state)?;
                Value::Null
            }
            Ok(None) => {
                state.workers.runs[i].status = WorkerStatus::NeedsHuman;
                state.workers.runs[i].known_prelaunch_failure = true;
                state.workers.runs[i].question = Some(format!(
                    "Herdr refused to open this checkout. Trust was not changed. Address the reported host-side requirement, then retry explicitly with `retry-worker --confirmed-absent-or-stopped`: {error:#}"
                ));
                return save(dir, state);
            }
            Err(inspect_error) => {
                state.workers.runs[i].status = WorkerStatus::Uncertain;
                state.workers.runs[i].question = Some(format!(
                    "Herdr open response and resource inspection failed; trust was not changed and no retry was attempted: {error:#}; {inspect_error:#}"
                ));
                return save(dir, state);
            }
        },
    };
    if opened != Value::Null {
        let actual = Path::new(&field(&opened, &["worktree", "path"])?).canonicalize()?;
        ensure!(
            actual == run.worktree.canonicalize()?,
            "Herdr opened a different worktree path"
        );
        state.workers.runs[i].workspace_id = Some(field(&opened, &["workspace", "workspace_id"])?);
        state.workers.runs[i].tab_id = Some(field(&opened, &["tab", "tab_id"])?);
        state.workers.runs[i].pane_id = Some(field(&opened, &["root_pane", "pane_id"])?);
        state.workers.runs[i].status = WorkerStatus::AgentStartReady;
        save(dir, state)?;
    }
    start_agent_from_ready(dir, state, herdr, i)
}

fn requires_ticket_claim(role: &str, purpose: Option<store::RunPurpose>) -> bool {
    !(role == "reviewer" && purpose == Some(store::RunPurpose::FinalFeatureReview))
}

fn empty_worker_shell_identity(
    herdr: &Client,
    run: &WorkerRun,
) -> Result<Option<(String, store::LinuxProcessIdentity)>> {
    let pane = run
        .pane_id
        .as_deref()
        .context("Herdr worktree.open omitted root pane ID")?;
    match herdr.agent(pane) {
        Err(error)
            if error
                .downcast_ref::<crate::herdr::HerdrApiError>()
                .is_some_and(crate::herdr::HerdrApiError::is_agent_not_found) => {}
        Ok(_) => return Ok(None),
        Err(error) => return Err(error).context("could not confirm the new worker pane is empty"),
    }
    let pane_info = herdr.pane(pane)?;
    let observed = &pane_info["pane"];
    ensure!(
        observed["pane_id"].as_str() == Some(pane),
        "Herdr pane identity changed before agent.start"
    );
    ensure!(
        observed["workspace_id"].as_str() == run.workspace_id.as_deref()
            && observed["tab_id"].as_str() == run.tab_id.as_deref(),
        "Herdr workspace or tab identity changed before agent.start"
    );
    let worktree = run
        .worktree
        .to_str()
        .context("worker worktree path is not valid UTF-8")?;
    ensure!(
        observed["cwd"].as_str() == Some(worktree),
        "Herdr pane is not in the exact detached worktree before agent.start"
    );
    let terminal = observed["terminal_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .context("Herdr pane omitted its terminal identity before agent.start")?
        .to_owned();
    let process =
        crate::herdr::capture_shell_process(&herdr.pane_process_info(pane)?, pane, worktree)?;
    Ok(Some((terminal, process)))
}

fn wait_for_empty_worker_shell(
    herdr: &Client,
    run: &WorkerRun,
    timeout: Duration,
) -> Result<Option<(String, store::LinuxProcessIdentity)>> {
    let deadline = Instant::now() + timeout;
    loop {
        match empty_worker_shell_identity(herdr, run) {
            Ok(Some(identity)) => return Ok(Some(identity)),
            Ok(None) => return Ok(None),
            Err(error) if Instant::now() >= deadline => {
                return Err(error).context("wait for exact empty worker shell before agent.start");
            }
            Err(_) => thread::sleep(Duration::from_millis(50)),
        }
    }
}

fn start_agent_from_ready(dir: &Path, state: &mut State, herdr: &Client, i: usize) -> Result<()> {
    let run = state.workers.runs[i].clone();
    let pane = run
        .pane_id
        .clone()
        .context("Herdr worktree.open omitted root pane ID")?;
    let (shell_terminal, shell_process) = match wait_for_empty_worker_shell(
        herdr,
        &run,
        Duration::from_secs(5),
    ) {
        Ok(Some(identity)) => identity,
        Ok(None) => {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].known_prelaunch_failure = false;
            state.workers.runs[i].question = Some(format!(
                "The exact new worker pane {pane} already has an agent. It was not replaced or prompted; inspect its identity and reconcile explicitly."
            ));
            return save(dir, state);
        }
        Err(error) => {
            state.workers.runs[i].status = WorkerStatus::NeedsHuman;
            state.workers.runs[i].known_prelaunch_failure = true;
            state.workers.runs[i].question = Some(format!(
                "Herdr did not provide the exact empty shell for pane {pane}; agent.start and the task prompt were not sent. Inspect the retained pane/worktree and retry explicitly after resolving the setup issue: {error:#}"
            ));
            return save(dir, state);
        }
    };
    let provider = state.workers.providers.for_role(&run.role).clone();
    let args = match provider_args(&provider) {
        Ok(args) => args,
        Err(error) => {
            state.workers.runs[i].status = WorkerStatus::NeedsHuman;
            state.workers.runs[i].known_prelaunch_failure = true;
            state.workers.runs[i].question = Some(format!(
                "Provider configuration prevented agent.start; the confirmed empty pane and worktree were retained. Fix the provider configuration, then retry explicitly with `retry-worker --confirmed-absent-or-stopped`: {error:#}"
            ));
            return save(dir, state);
        }
    };
    state.workers.runs[i].terminal_id = Some(shell_terminal.clone());
    state.workers.runs[i].foreground_process = Some(shell_process.clone());
    state.workers.runs[i].status = WorkerStatus::AgentIntent;
    save(dir, state)?;
    let mut start_attempt = 0;
    let started = loop {
        start_attempt += 1;
        match herdr.start_agent(
            &format!("wf-{}-{}", run.ticket, run.id),
            &provider.kind,
            &pane,
            &args,
        ) {
            Ok(started) => break started,
            Err(error) => {
                let same_empty_shell = empty_worker_shell_identity(herdr, &state.workers.runs[i])
                    .is_ok_and(|identity| {
                        identity.is_some_and(|(terminal, process)| {
                            terminal == shell_terminal && process == shell_process
                        })
                    });
                let explicit_busy = error
                    .downcast_ref::<crate::herdr::HerdrApiError>()
                    .is_some_and(crate::herdr::HerdrApiError::is_agent_pane_busy);
                if explicit_busy && same_empty_shell && start_attempt < 3 {
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
                if same_empty_shell {
                    state.workers.runs[i].status = WorkerStatus::NeedsHuman;
                    state.workers.runs[i].known_prelaunch_failure = true;
                    state.workers.runs[i].question = Some(format!(
                        "Herdr refused agent.start and the exact empty shell identity is unchanged; no provider or task prompt is running. The claim and worktree remain retained. Inspect the reported setup issue, then use the supported retry control: {error:#}"
                    ));
                } else {
                    state.workers.runs[i].status = WorkerStatus::Uncertain;
                    state.workers.runs[i].question = Some(format!(
                        "Agent start may have taken effect and the pre-start shell identity no longer verifies; inspect pane {pane}. No task prompt was submitted: {error:#}"
                    ));
                }
                return save(dir, state);
            }
        }
    };
    let (terminal_id, agent_provider, agent_session) = match wait_for_started_agent_identity(
        herdr,
        &started,
        &state.workers.runs[i],
        &provider.kind,
        std::time::Duration::from_secs(5),
    ) {
        Ok(identity) => identity,
        Err(error) => {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].question = Some(format!(
                "Agent start succeeded without verifiable pane/terminal identity; it was not repeated: {error:#}"
            ));
            return save(dir, state);
        }
    };
    if state.workers.runs[i].terminal_id.as_deref() != Some(terminal_id.as_str()) {
        state.workers.runs[i].status = WorkerStatus::Uncertain;
        state.workers.runs[i].question = Some(format!(
            "The Herdr terminal changed between the verified shell and agent.start in pane {pane}; preserve the attempt and do not submit a task prompt."
        ));
        return save(dir, state);
    }
    state.workers.runs[i].terminal_id = Some(terminal_id);
    state.workers.runs[i].agent_provider = agent_provider;
    state.workers.runs[i].agent_session = agent_session;
    let worktree = state.workers.runs[i]
        .worktree
        .to_str()
        .context("worker worktree path is not valid UTF-8")?;
    let process = match wait_for_provider_process(
        herdr,
        &pane,
        &provider.kind,
        worktree,
        std::time::Duration::from_secs(5),
    ) {
        Ok(process) => process,
        Err(error) => {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].question = Some(format!(
                "Herdr started the agent, but Linux process continuity could not be established; no prompt was sent: {error:#}"
            ));
            return save(dir, state);
        }
    };
    state.workers.runs[i].foreground_process = Some(process);
    state.workers.runs[i].status = WorkerStatus::InitialPromptReady;
    state.workers.runs[i].initial_prompt_pending = true;
    state.workers.runs[i].initial_prompt_attempted = Some(false);
    save(dir, state)?;
    submit_initial_task_prompt(dir, state, herdr, i)
}

fn wait_for_provider_process(
    herdr: &Client,
    pane: &str,
    provider: &str,
    worktree: &str,
    timeout: std::time::Duration,
) -> Result<crate::store::LinuxProcessIdentity> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match herdr.pane_process_info(pane).and_then(|info| {
            crate::herdr::capture_provider_process(&info, pane, provider, worktree)
        }) {
            Ok(process) => return Ok(process),
            Err(error) if std::time::Instant::now() >= deadline => {
                return Err(error).context("wait for Herdr foreground provider process");
            }
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(50)),
        }
    }
}

fn wait_for_started_agent_identity(
    herdr: &Client,
    started: &serde_json::Value,
    run: &WorkerRun,
    expected_provider: &str,
    timeout: std::time::Duration,
) -> Result<(
    String,
    Option<String>,
    Option<crate::store::AgentSessionIdentity>,
)> {
    let first = crate::herdr::capture_agent_identity(started, run)?;
    if first.1.as_deref() == Some(expected_provider) {
        return Ok(first);
    }
    ensure!(
        first.1.is_none(),
        "Herdr started a different provider than the configured {expected_provider}"
    );
    let deadline = std::time::Instant::now() + timeout;
    let pane = run
        .pane_id
        .as_deref()
        .context("launched worker has no pane")?;
    loop {
        let last_error = match herdr
            .agent(pane)
            .and_then(|info| crate::herdr::capture_agent_identity(&info, run))
        {
            Ok(identity) if identity.1.as_deref() == Some(expected_provider) => {
                ensure!(
                    identity.0 == first.0,
                    "Herdr terminal changed while provider identity initialized"
                );
                return Ok(identity);
            }
            Ok(identity) => format!("Herdr agent provider was {:?}", identity.1),
            Err(error) => format!("{error:#}"),
        };
        if std::time::Instant::now() >= deadline {
            bail!(
                "Herdr did not report configured provider {expected_provider} after agent.start: {last_error}"
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

fn submit_initial_task_prompt(
    dir: &Path,
    state: &mut State,
    herdr: &Client,
    i: usize,
) -> Result<()> {
    let run = state.workers.runs[i].clone();
    let pane = run
        .pane_id
        .as_deref()
        .context("initial prompt has no owned pane")?;
    let agent = match crate::herdr::inspect_pre_prompt_worker(herdr, &run) {
        Ok(agent) => agent,
        Err(error) => {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].question = Some(format!(
                "The initial task prompt was not sent because its terminal/process identity could not be verified: {error:#}"
            ));
            return save(dir, state);
        }
    };
    match status(&agent).unwrap_or("unknown") {
        "blocked" => return mark_initial_prompt_blocked(dir, state, herdr, i),
        "idle" | "done" => {}
        "working" => {
            state.workers.runs[i].status = WorkerStatus::InitialPromptReady;
            return save(dir, state);
        }
        other => {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].question = Some(format!(
                "Initial task prompt remains unsent; Herdr reports unrecognized status {other}. Inspect pane {pane}."
            ));
            return save(dir, state);
        }
    }
    state.workers.runs[i].status = WorkerStatus::PromptIntent;
    state.workers.runs[i].initial_prompt_pending = true;
    state.workers.runs[i].initial_prompt_attempted = Some(true);
    save(dir, state)?;
    let prompt = worker_prompt(&state.workers.runs[i], state)?;
    if let Err(error) = herdr.prompt(pane, &prompt) {
        if error
            .downcast_ref::<crate::herdr::HerdrApiError>()
            .is_some_and(crate::herdr::HerdrApiError::is_agent_blocked)
        {
            return mark_initial_prompt_blocked(dir, state, herdr, i);
        }
        state.workers.runs[i].status = WorkerStatus::Uncertain;
        state.workers.runs[i].question = Some(format!(
            "Initial task prompt may have been submitted; it was not repeated: {error:#}"
        ));
        return save(dir, state);
    }
    // Herdr acknowledged this exact initial submission. From this point onward
    // a restart must not treat the still-empty result file as task completion.
    state.workers.runs[i].initial_prompt_pending = false;
    state.workers.runs[i].initial_prompt_acknowledged = true;
    state.workers.runs[i].initial_prompt_reconnect_pending = true;
    state.workers.runs[i].status = WorkerStatus::Running;
    state.workers.runs[i].last_activity_ms = Some(now_ms());
    save(dir, state)?;
    let current = state.workers.runs[i].clone();
    let observed = match herdr
        .agent(pane)
        .and_then(|info| crate::herdr::capture_agent_identity(&info, &current))
    {
        Ok((terminal, provider, session)) => (terminal, provider, session),
        Err(error) => {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].question = Some(format!(
                "Herdr acknowledged the initial prompt but original agent identity could not be verified; it was not repeated: {error:#}"
            ));
            return save(dir, state);
        }
    };
    let process = match herdr
        .pane_process_info(pane)
        .and_then(|info| crate::herdr::capture_foreground_process(&info, pane))
    {
        Ok(process) if current.foreground_process.as_ref() == Some(&process) => process,
        Ok(_) => {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].question = Some("Herdr acknowledged the initial prompt but foreground process identity changed; it was not repeated.".into());
            return save(dir, state);
        }
        Err(error) => {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].question = Some(format!(
                "Herdr acknowledged the initial prompt but Linux process identity could not be verified; it was not repeated: {error:#}"
            ));
            return save(dir, state);
        }
    };
    let worker = &mut state.workers.runs[i];
    worker.status = WorkerStatus::Running;
    worker.last_activity_ms = Some(now_ms());
    worker.terminal_id = Some(observed.0);
    worker.agent_provider = observed.1;
    worker.agent_session = observed.2;
    worker.initial_prompt_acknowledged = worker.agent_session.is_none();
    worker.initial_prompt_reconnect_pending = false;
    worker.foreground_process = Some(process);
    worker.question = None;
    worker.human_request_id = None;
    worker.human_request_kind = None;
    worker.human_request_fingerprint = None;
    save(dir, state)
}

fn mark_initial_prompt_blocked(
    dir: &Path,
    state: &mut State,
    herdr: &Client,
    i: usize,
) -> Result<()> {
    let pane = state.workers.runs[i]
        .pane_id
        .clone()
        .context("blocked initial prompt has no owned pane")?;
    let question = herdr
        .read_recent(&pane)
        .ok()
        .and_then(|value| value["read"]["text"].as_str().map(str::to_owned))
        .filter(|text| !text.trim().is_empty())
        .unwrap_or_else(|| {
            format!(
                "Herdr blocked the startup prompt before task input. Inspect the named pane {pane} directly before responding."
            )
        });
    let worker = &mut state.workers.runs[i];
    worker.status = WorkerStatus::NeedsHuman;
    worker.initial_prompt_pending = true;
    worker.set_human_request(crate::store::HumanRequestKind::HerdrBlockedUi, &question);
    worker.question = Some(question);
    save(dir, state)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn accepted_worker_decision_context(state: &State) -> String {
    let decisions = state
        .workers
        .runs
        .iter()
        .flat_map(|run| {
            run.answer_history
                .iter()
                .filter(|answer| {
                    answer.request_kind == crate::store::HumanRequestKind::WorkerQuestion
                        && answer.disposition == crate::store::AnswerDisposition::Submitted
                })
                .map(move |answer| {
                    format!(
                        "- Ticket #{}; request `{}`; exact human answer: {:?}. This request is resolved; do not ask the human this question again.",
                        run.ticket, answer.request_id, answer.response
                    )
                })
        })
        .collect::<Vec<_>>();
    if decisions.is_empty() {
        String::new()
    } else {
        format!(
            "Already recorded human decisions for this map (durable local answer history):\n{}",
            decisions.join("\n")
        )
    }
}

fn worker_prompt(run: &WorkerRun, state: &State) -> Result<String> {
    let map = MapRef::parse(&state.map).expect("validated map identity in durable state");
    let repository = format!("{}/{}", map.owner, map.repository);
    let ticket_url = format!("https://github.com/{repository}/issues/{}", run.ticket);
    let map_url = format!("https://github.com/{repository}/issues/{}", map.number);
    let skill = match run.role.as_str() {
        "implementer" => "implement",
        "reviewer" => "code-review",
        "orchestrator" => "wayfinder",
        _ => "research",
    };
    let final_feature_review = run.is_final_feature_review();
    let (work, context) = if final_feature_review {
        ensure!(
            state.canonical_linked_spec_resolved,
            "final review cannot launch before canonical linked specification resolution"
        );
        let spec = state
            .canonical_linked_spec_url
            .as_deref()
            .unwrap_or("none-linked");
        let target = run
            .base_commit
            .as_deref()
            .context("final feature review omitted its pinned commit")?;
        let base = git(&run.worktree, &["rev-parse", "refs/remotes/origin/develop"])?
            .trim()
            .to_owned();
        ensure!(is_full_commit(&base), "origin/develop is not a full commit");
        (
            format!(
                "This is the independent COMPLETE-FEATURE review, not a review of ticket #{} alone or only the latest commit. Read the entire map #{}, its every linked ticket, linked spec (if any), and all accepted map/spec comments and human decisions. Inspect the complete change range `origin/develop..{target}` and the resulting tree, then compare the complete feature with the map/spec. Do not edit or commit. The exact review correlation is map `{}`; base ref `origin/develop` at `{base}`; pinned HEAD `{target}`. The canonical linked spec is exactly `{spec}`; report that exact URL, or `none-linked` only when this runtime resolved no canonical link.",
                run.ticket, map.number, state.map
            ),
            accepted_worker_decision_context(state),
        )
    } else if run.role == "reviewer" {
        (
            format!(
                "Independently review fixed commit {} in this separate checkout. Do not edit or commit.",
                run.base_commit.as_deref().unwrap_or("<missing>")
            ),
            run.context.as_deref().unwrap_or("").to_owned(),
        )
    } else {
        (
            format!(
                "Work only in this detached worktree for ticket #{}.",
                run.ticket
            ),
            run.context.as_deref().unwrap_or("").to_owned(),
        )
    };
    let final_review_contract = if final_feature_review {
        let example = serde_json::json!({
            "scope": "complete_feature",
            "map": state.map,
            "base_ref": "origin/develop",
            "base_commit": git(&run.worktree, &["rev-parse", "refs/remotes/origin/develop"])?.trim(),
            "reviewed_commit": run.base_commit.as_deref().unwrap_or_default(),
            "spec": state.canonical_linked_spec_url.as_deref().unwrap_or("none-linked"),
            "accepted_decisions_reviewed": true,
        });
        format!(
            "Also include this correlated object as `final_feature_review` in `.wayfinder-result.json` exactly as shown:\n```json\n{}\n``` The runtime requires this scope block before the result can authorize readiness.",
            serde_json::to_string_pretty(&example)?
        )
    } else {
        String::new()
    };
    Ok(format!(
        "Wayfinder delegated {} work.\nMap identity: {repository}#{} ({map_url}).\nTicket identity: {repository}#{} ({ticket_url}).\nRun ID: {}.\n\nStart by reading the repository's `AGENTS.md` and, when available, the `{}` skill from `.agents/skills/{}/SKILL.md` or `$HOME/.agents/skills/{}/SKILL.md`; follow any more specific instructions. If changing domain terminology, read `CONTEXT.md`.\n\nBefore acting, read the actual GitHub ticket and comments with `gh issue view {} --repo {repository} --json body,title,comments`. Read the map and its accepted comments with `gh issue view {} --repo {repository} --json body,title,comments`; if it links a specification, follow that repository-qualified link and read the spec and its comments with `--json body,title,comments`. Read map/spec comments for accepted decisions that have not yet been refreshed into their bodies. Accepted automation policy: map/spec updates use append-only comments and explicitly leave body refresh pending for a human; never patch existing map or spec bodies. If a read-only `gh` command fails because sandbox networking blocks GitHub, request `require_escalated` for that exact command and retry with the authenticated GitHub configuration. A sandbox failure is not evidence that the token is invalid: do not ask the human to reauthenticate or report invalid credentials unless the escalated command independently confirms that result. If an authorized read still cannot provide a target ticket, map, linked spec, required comment, or applicable decision, report exactly what is missing and pause dependent work. Do not invent, infer, or answer a human response.\n\n{}\n\n{}\n\nWrite `.wayfinder-result.json` with JSON fields `format_version`=1, `run_id`=`{}`, `ticket`={}, `role`=`{}`, `status`=`completed|failed|blocked`, nonempty `summary`, and optional `question`. For implementation include `commit` with the full HEAD hash. For reviewer include `reviewed_commit` equal to the pinned commit, verdict `approved` or `changes_requested`, `unresolved_findings` as an array, and `known_limitations` as an array. Use empty arrays only when there are none; `approved` requires empty `unresolved_findings`. {} Idle/done is not success. If a human decision is needed, include its actual question and stop.",
        run.role,
        map.number,
        run.ticket,
        run.id,
        skill,
        skill,
        skill,
        run.ticket,
        map.number,
        context,
        work,
        run.id,
        run.ticket,
        run.role,
        final_review_contract
    ))
}

fn provider_args(provider: &Provider) -> Result<Vec<String>> {
    ensure!(
        !provider.kind.trim().is_empty(),
        "provider kind cannot be empty"
    );
    let mut args = provider.args.clone();
    if let Some(model) = &provider.model {
        ensure!(!model.trim().is_empty(), "model cannot be empty");
        args.extend(["--model".into(), model.clone()]);
    }
    if let Some(effort) = &provider.reasoning_effort {
        ensure!(
            !effort.trim().is_empty(),
            "reasoning effort cannot be empty"
        );
        if provider.kind == "codex" {
            args.extend(["-c".into(), format!("model_reasoning_effort={effort}")]);
        } else {
            args.extend(["--reasoning-effort".into(), effort.clone()]);
        }
    }
    Ok(args)
}
fn field(value: &Value, path: &[&str]) -> Result<String> {
    let mut v = value;
    for key in path {
        v = v
            .get(*key)
            .with_context(|| format!("Herdr response omitted {}", path.join(".")))?;
    }
    v.as_str()
        .map(str::to_owned)
        .context("Herdr resource ID was not a string")
}
fn status(value: &Value) -> Option<&str> {
    value
        .get("agent")
        .and_then(|a| a.get("agent_status"))
        .and_then(Value::as_str)
        .or_else(|| value.get("agent_status").and_then(Value::as_str))
}
fn git(path: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(["-C"])
        .arg(path)
        .args(args)
        .output()
        .context("run git in retained worktree")?;
    ensure!(
        output.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8(output.stdout).context("git returned non-UTF8 output")
}

fn reviewer_context(summary: &str, implementation: &WorkerRun) -> String {
    let mut context = format!("Review implementation: {summary}");
    let decisions: Vec<_> = implementation
        .answer_history
        .iter()
        .filter(|answer| {
            answer.request_kind == crate::store::HumanRequestKind::WorkerQuestion
                && answer.disposition == crate::store::AnswerDisposition::Submitted
        })
        .collect();
    if !decisions.is_empty() {
        context.push_str(
            "\n\nPreviously answered human worker questions for this implementation (authoritative, correlated decisions; do not ask these again):",
        );
        for answer in decisions {
            context.push_str(&format!(
                "\n- Request {}: exact human response {:?}.",
                answer.request_id, answer.response
            ));
        }
        context.push_str(" Distinguish any genuinely new question from these resolved decisions.");
    }
    context
}

fn validate_detached_worktree(repository: &Path, path: &Path) -> Result<()> {
    let expected = fs::canonicalize(path)?;
    let root = git(path, &["rev-parse", "--show-toplevel"])?;
    ensure!(
        Path::new(root.trim()) == expected,
        "worktree root does not match its durable path"
    );
    let paths = git(repository, &["worktree", "list", "--porcelain"])?;
    ensure!(
        paths.lines().any(|line| line
            .strip_prefix("worktree ")
            .is_some_and(|p| Path::new(p) == expected)),
        "Git repository does not list the durable path as one of its worktrees"
    );
    let head = Command::new("git")
        .args(["-C"])
        .arg(path)
        .args(["symbolic-ref", "--quiet", "HEAD"])
        .output()
        .context("verify detached HEAD")?;
    ensure!(
        !head.status.success(),
        "ticket worktree is attached to a branch; expected detached HEAD"
    );
    Ok(())
}
fn save(dir: &Path, state: &State) -> Result<()> {
    store::atomic_json(&dir.join("state.json"), state)
}
fn check_delay(state: &State, failures: u32) -> u64 {
    state
        .poll_seconds
        .saturating_mul(1u64 << failures.min(4))
        .min(300.max(state.poll_seconds))
}
