//! One per-map process supervises durable dispatch and reconciliation.
use crate::{
    herdr::Client,
    host,
    store::{self, Authorization, Lock, Provider, State, WorkerRun, WorkerStatus},
    tracker::{FrontierTicket, GitHub, MapRef},
};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::Value;
use std::{
    fs,
    path::Path,
    process::Command,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const FAILURE_RETRIES: u8 = 2;
const REWORK_ROUNDS: u8 = 3;
const SUBMITTED_IDLE_GRACE: Duration = Duration::from_secs(5 * 60);

/// systemd supervises this process; the kernel releases its lifetime lock on death.
pub fn serve(root: &Path, key: &str, once: bool) -> Result<()> {
    let dir = store::map_dir(root, key)?;
    let _runtime_lock = Lock::acquire(&dir.join("runtime.lock"))?;
    let mut next_check = Instant::now();
    let mut failures = 0u32;
    loop {
        {
            let _state_lock = Lock::acquire(&dir.join("state.lock"))?;
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
    let frontier = github.reconcile(&map)?;
    let tracker_dir = dir.join("tracker");
    fs::create_dir_all(&tracker_dir)?;
    store::atomic_json(&tracker_dir.join("frontier.json"), &frontier)?;
    let herdr = Client::new(&state.binding.socket);
    reconcile_workers(dir, state, &map, &github, &herdr)?;
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
    let ready = match github.dispatch_frontier(&map) {
        Ok(ready) => ready,
        Err(error) if format!("{error:#}").contains("execution override") => {
            return Ok(format!(
                "Host and GitHub reconciled; dispatch held: {error:#}"
            ));
        }
        Err(error) => return Err(error),
    };
    let repository = state.binding.repository.clone();
    queue_frontier(state, &ready, &repository, &map);
    launch_queued(dir, state, &map, &github, &herdr)?;
    let active = state
        .workers
        .runs
        .iter()
        .filter(|r| r.reserves_capacity())
        .count();
    Ok(format!(
        "Host and GitHub reconciled; {active}/{} worker slots reserved",
        state.concurrency
    ))
}

fn queue_frontier(state: &mut State, frontier: &[FrontierTicket], repository: &Path, map: &MapRef) {
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
                base_commit: None,
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
        last_activity_ms: None,
        terminal_id: None,
        agent_provider: None,
        agent_session: None,
        foreground_process: None,
        result_evidence: None,
        initial_prompt_pending: false,
        initial_prompt_acknowledged: false,
        initial_prompt_reconnect_pending: false,
        known_prelaunch_failure: false,
    });
}

fn reconcile_workers(
    dir: &Path,
    state: &mut State,
    map: &MapRef,
    github: &GitHub,
    herdr: &Client,
) -> Result<()> {
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
                || (r.status == WorkerStatus::Uncertain && r.initial_prompt_reconnect_pending)
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
                        state.workers.runs[i].result_evidence = Some(evidence);
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
    let evidence = evidence_path(dir, &run.id)?;
    let source_bytes = match fs::read(&source) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error).with_context(|| format!("read {}", source.display())),
    };
    let archived_bytes = match fs::read(&evidence) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error).with_context(|| format!("read {}", evidence.display())),
    };
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
    ensure!(
        result.run_id == run.id && result.ticket == run.ticket && result.role == run.role,
        "worker result identity does not match the durable run"
    );
    if archived_bytes.is_none() {
        persist_evidence(&evidence, &bytes)?;
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
    Ok(Some((result, evidence)))
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
            new_run(
                state,
                NewRun {
                    ticket: run.ticket,
                    role: "reviewer",
                    repository: &repository,
                    map: &map,
                    source_run: Some(run.id),
                    base_commit: Some(commit.into()),
                    context: Some(format!("Review implementation: {}", result.summary)),
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
            match result.verdict.as_deref() {
                Some("approved") => {
                    state.workers.runs[i].status = WorkerStatus::Completed;
                    state.workers.runs[i].summary = Some(result.summary.clone());
                    if let Some(source) = run.source_run.as_ref() {
                        if let Some(parent) =
                            state.workers.runs.iter_mut().find(|r| &r.id == source)
                        {
                            parent.status = WorkerStatus::Reviewed;
                        }
                    }
                    save(dir, state)
                }
                Some("changes_requested") => {
                    state.workers.runs[i].status = WorkerStatus::Completed;
                    state.workers.runs[i].summary = Some(result.summary.clone());
                    let source = run
                        .source_run
                        .as_ref()
                        .and_then(|id| state.workers.runs.iter().find(|r| &r.id == id))
                        .context("review has no implementation run")?
                        .clone();
                    if source.rework_round < REWORK_ROUNDS {
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
                        state.workers.runs.last_mut().unwrap().automatic_retries =
                            source.automatic_retries;
                    } else {
                        state.workers.runs[i].status = WorkerStatus::NeedsHuman;
                        state.workers.runs[i].question=Some("Three automatic review/rework rounds were exhausted; human decision required.".into());
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
    let login = match github.claim_for_runtime(map, run.ticket, dir) {
        Ok(login) => login,
        Err(e) => {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].question = Some(format!(
                "Claim outcome is uncertain; no worker launched: {e:#}"
            ));
            return save(dir, state);
        }
    };
    run.claim_login = Some(login);
    if run.worktree.exists() {
        if validate_detached_worktree(&state.binding.repository, &run.worktree).is_err() {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].claim_login = run.claim_login;
            state.workers.runs[i].question=Some("A path exists at the intended location but does not identify this detached worktree; creation was not retried.".into());
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

fn start_agent_from_ready(dir: &Path, state: &mut State, herdr: &Client, i: usize) -> Result<()> {
    let run = state.workers.runs[i].clone();
    let pane = run
        .pane_id
        .clone()
        .context("Herdr worktree.open omitted root pane ID")?;
    match herdr.agent(&pane) {
        Err(error)
            if error
                .downcast_ref::<crate::herdr::HerdrApiError>()
                .is_some_and(crate::herdr::HerdrApiError::is_agent_not_found) =>
        {
            // The exact recovered pane is known to have no occupant, so starting
            // the first agent remains safe after restart.
        }
        Ok(_) => {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].question = Some(format!(
                "The recovered pane {pane} already has an agent; no replacement was started. Inspect its identity before retrying."
            ));
            return save(dir, state);
        }
        Err(error) => {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].question = Some(format!(
                "Could not confirm pane {pane} is empty; agent.start was not sent: {error:#}"
            ));
            return save(dir, state);
        }
    }
    state.workers.runs[i].status = WorkerStatus::AgentIntent;
    save(dir, state)?;
    let provider = state.workers.providers.for_role(&run.role).clone();
    let args = provider_args(&provider)?;
    let started = match herdr.start_agent(
        &format!("wf-{}-{}", run.ticket, run.id),
        &provider.kind,
        &pane,
        &args,
    ) {
        Ok(started) => started,
        Err(error) => {
            state.workers.runs[i].status = WorkerStatus::Uncertain;
            state.workers.runs[i].question = Some(format!(
                "Agent start may have taken effect; inspect pane {pane}: {error:#}"
            ));
            return save(dir, state);
        }
    };
    let (terminal_id, agent_provider, agent_session) = match crate::herdr::capture_agent_identity(
        &started,
        &state.workers.runs[i],
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
    state.workers.runs[i].terminal_id = Some(terminal_id);
    state.workers.runs[i].agent_provider = agent_provider;
    state.workers.runs[i].agent_session = agent_session;
    let process = match herdr
        .pane_process_info(&pane)
        .and_then(|info| crate::herdr::capture_foreground_process(&info, &pane))
    {
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
    save(dir, state)?;
    submit_initial_task_prompt(dir, state, herdr, i)
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
    save(dir, state)?;
    let prompt = worker_prompt(&state.workers.runs[i], state);
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

fn worker_prompt(run: &WorkerRun, state: &State) -> String {
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
    let work = if run.role == "reviewer" {
        format!(
            "Independently review fixed commit {} in this separate checkout. Do not edit or commit.",
            run.base_commit.as_deref().unwrap_or("<missing>")
        )
    } else {
        format!(
            "Work only in this detached worktree for ticket #{}.",
            run.ticket
        )
    };
    format!(
        "Wayfinder delegated {} work.\nMap identity: {repository}#{} ({map_url}).\nTicket identity: {repository}#{} ({ticket_url}).\nRun ID: {}.\n\nStart by reading the repository's `AGENTS.md` and, when available, the `{}` skill from `.agents/skills/{}/SKILL.md` or `$HOME/.agents/skills/{}/SKILL.md`; follow any more specific instructions. If changing domain terminology, read `CONTEXT.md`.\n\nBefore acting, read the actual GitHub ticket and comments with `gh issue view {} --repo {repository} --json body,title,comments`. Read the map and its accepted comments with `gh issue view {} --repo {repository} --json body,title,comments`; if it links a specification, follow that repository-qualified link and read the spec and its comments with `--json body,title,comments`. Read map/spec comments for accepted decisions that have not yet been refreshed into their bodies. Accepted automation policy: map/spec updates use append-only comments and explicitly leave body refresh pending for a human; never patch existing map or spec bodies. If a target ticket, map, linked spec, required comment, or applicable decision cannot be read, report exactly what is missing and pause dependent work. Do not invent, infer, or answer a human response.\n\n{}\n\n{}\n\nWrite `.wayfinder-result.json` with JSON fields `format_version`=1, `run_id`=`{}`, `ticket`={}, `role`=`{}`, `status`=`completed|failed|blocked`, nonempty `summary`, and optional `question`. For implementation include `commit` with the full HEAD hash. For reviewer include `reviewed_commit` equal to the pinned commit and verdict `approved` or `changes_requested`. Idle/done is not success. If a human decision is needed, include its actual question and stop.",
        run.role,
        map.number,
        run.ticket,
        run.id,
        skill,
        skill,
        skill,
        run.ticket,
        map.number,
        run.context.as_deref().unwrap_or(""),
        work,
        run.id,
        run.ticket,
        run.role
    )
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
