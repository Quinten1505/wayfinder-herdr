use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use std::{env, fs, path::PathBuf, process::Command};
use wayfinder_herdr::{
    delivery,
    herdr::Client,
    orchestration, runtime,
    store::{
        self, AnswerDisposition, Binding, HumanAnswerEvidence, HumanRequestKind, Lock, Provider,
        RequestKind, SchedulerDecisionDisposition, WorkerRun, WorkerStatus,
    },
    tracker::{self, MapRef, TicketInput},
};

#[derive(Parser)]
#[command(
    version,
    about = "Wayfinder GitHub map workflow and local worker runtime"
)]
struct Cli {
    /// Stable durable root. Defaults to $XDG_STATE_HOME/wayfinder-herdr.
    #[arg(long, global = true)]
    state_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: CommandName,
}
#[derive(Subcommand)]
enum CommandName {
    /// Bind a map, start its supervised runtime, and show first-use status.
    Attach {
        #[arg(long)]
        map: String,
        #[arg(long)]
        repository: PathBuf,
        #[arg(long)]
        socket: Option<PathBuf>,
        /// Owning repo workspace in the target Herdr session; defaults to HERDR_WORKSPACE_ID.
        #[arg(long)]
        workspace: Option<String>,
        #[arg(long)]
        herdr: Option<PathBuf>,
        #[arg(long, default_value_t = 30)]
        poll_seconds: u64,
        /// Create state only (for service provisioning or diagnostics).
        #[arg(long)]
        no_service: bool,
    },
    /// Explicit first authorization; compatibility and reconciliation still gate dispatch.
    Start {
        #[arg(long)]
        map: String,
    },
    /// Reconcile one explicit human delivery instruction from this map's verified chat.
    AuthorizeExisting {
        #[arg(long)]
        map: String,
        /// Exact human instruction as given in the orchestrator chat.
        #[arg(long)]
        instruction: String,
    },
    /// Verify and bind a separate checked-out feature branch for delivery.
    BindFeatureCheckout {
        #[arg(long)]
        map: String,
        #[arg(long)]
        checkout: PathBuf,
        /// Exact feature branch selected for this map.
        #[arg(long)]
        branch: String,
        /// Exact feature commit expected before binding.
        #[arg(long)]
        head: String,
        /// Explicit Git ref whose current commit is the saved delivery base.
        #[arg(long)]
        base_ref: String,
        /// Existing draft PR proving this map's first branch selection.
        #[arg(long)]
        source_pr: Option<u64>,
    },
    Pause {
        #[arg(long)]
        map: String,
    },
    Resume {
        #[arg(long)]
        map: String,
    },
    Status {
        #[arg(long)]
        map: String,
    },
    /// Open the orchestrating chat for a map. With no map, use this Herdr workspace/session.
    Chat {
        #[arg(long)]
        map: Option<String>,
    },
    /// Recover an interrupted or changed chat after explicit human confirmation.
    RecoverChat {
        #[arg(long)]
        map: Option<String>,
        #[arg(long)]
        confirm_replacement: bool,
        /// Confirm the prior interrupted launch is absent or has been stopped by the human.
        #[arg(long)]
        confirm_launch_absent_or_stopped: bool,
    },
    /// Inspect durable chat delivery state before resolving an uncertain message.
    ChatOutbox {
        #[arg(long)]
        map: String,
    },
    /// Resolve an uncertain message only after the human checks the previous chat history.
    ResolveChatDelivery {
        #[arg(long)]
        map: String,
        #[arg(long)]
        message: String,
        #[arg(long)]
        confirmed_delivered: bool,
        #[arg(long)]
        confirmed_not_delivered: bool,
    },
    /// Configure the provider used for delegated worker roles.
    ConfigureWorker {
        #[arg(long)]
        map: String,
        /// One of orchestrator, researcher, implementer, reviewer; omit to set shared defaults.
        #[arg(long)]
        role: Option<String>,
        #[arg(long, default_value = "codex")]
        kind: String,
        #[arg(long)]
        model: Option<String>,
        #[arg(long)]
        reasoning_effort: Option<String>,
        /// Provider-specific CLI argument. Repeat to pass multiple argv entries.
        #[arg(long = "arg")]
        args: Vec<String>,
        #[arg(long)]
        concurrency: Option<u32>,
    },
    /// Durably stop one worker pane. Its claim and artifacts remain retained.
    StopWorker {
        #[arg(long)]
        map: String,
        #[arg(long)]
        run: String,
    },
    /// Answer a recorded worker question or retain a manual-pane response.
    AnswerWorker {
        #[arg(long)]
        map: String,
        #[arg(long)]
        run: String,
        /// Exact pending request ID shown by status.
        #[arg(long)]
        request_id: String,
        /// Request source: worker_question or herdr_blocked_ui.
        #[arg(long, value_parser = ["worker_question", "herdr_blocked_ui"])]
        request_type: String,
        #[arg(long)]
        response: String,
    },
    /// Record the human's answer to a scheduler decision without contacting a worker.
    AnswerDecision {
        #[arg(long)]
        map: String,
        /// Exact scheduler request ID shown by status and the orchestrator.
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        response: String,
        /// Explicit action selected by the human: continue, defer, or abandon.
        #[arg(long, value_parser = ["continue", "defer", "abandon"])]
        disposition: String,
    },
    /// Resume a retained attempt. Uncertain workers require explicit absence confirmation.
    RetryWorker {
        #[arg(long)]
        map: String,
        #[arg(long)]
        run: String,
        #[arg(long)]
        confirmed_absent_or_stopped: bool,
    },
    /// Record an abandonment decision without implying termination or deleting artifacts.
    AbandonWorker {
        #[arg(long)]
        map: String,
        #[arg(long)]
        run: String,
    },
    /// Short startup/hook request. Never authorizes dispatch.
    Reconcile {
        #[arg(long)]
        map: Option<String>,
    },
    /// Per-map process intended to be supervised by the installed systemd unit.
    Serve {
        #[arg(long)]
        key: String,
        #[arg(long)]
        once: bool,
    },
    /// Manifest action scoped to the invoking workspace and endpoint.
    Action {
        #[arg(value_parser = ["start", "pause", "resume", "status"])]
        name: String,
    },
    /// GitHub Issues map and ticket operations.
    Tracker {
        #[command(subcommand)]
        command: TrackerCommand,
    },
}
#[derive(Subcommand)]
enum TrackerCommand {
    /// Create a map in planning mode; opt in to execution with --execution-override.
    CreateMap {
        #[arg(long)]
        repository: String,
        #[arg(long)]
        title: String,
        #[arg(long, default_value = "")]
        notes: String,
        #[arg(long)]
        execution_override: bool,
    },
    /// Create and attach a decision or task ticket under a map.
    CreateTicket {
        #[arg(long)]
        map: String,
        #[arg(long)]
        title: String,
        #[arg(long, default_value = "")]
        body: String,
        #[arg(long, default_value = "wayfinder:task")]
        label: String,
    },
    /// Show ready, open, unclaimed child tickets in native map order.
    Frontier {
        #[arg(long)]
        map: String,
    },
    /// Add a native blocking dependency between map tickets.
    Block {
        #[arg(long)]
        map: String,
        #[arg(long)]
        ticket: u64,
        #[arg(long)]
        by: u64,
    },
    /// Claim a child ticket by assigning it to a GitHub user.
    Claim {
        #[arg(long)]
        map: String,
        #[arg(long)]
        ticket: u64,
        #[arg(long, default_value = "@me")]
        assignee: String,
    },
    /// Reconcile an uncertain claim from current GitHub state without changing assignments.
    ReconcileClaim {
        #[arg(long)]
        map: String,
        #[arg(long)]
        ticket: u64,
        #[arg(long, default_value = "@me")]
        assignee: String,
    },
    /// Record an orchestrator resolution and update the map and spec index.
    Resolve {
        #[arg(long)]
        map: String,
        #[arg(long)]
        ticket: u64,
        #[arg(long)]
        resolution: String,
        #[arg(long, default_value_t = 10)]
        spec: u64,
    },
}
fn root(explicit: Option<PathBuf>) -> Result<PathBuf> {
    let root = match explicit {
        Some(path) => path,
        None => {
            let base = env::var_os("XDG_STATE_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    PathBuf::from(env::var_os("HOME").unwrap_or_default()).join(".local/state")
                });
            base.join("wayfinder-herdr")
        }
    };
    ensure!(root.is_absolute(), "state directory must be absolute");
    Ok(root)
}
fn executable(given: Option<PathBuf>) -> Result<PathBuf> {
    let binary = given
        .or_else(|| env::var_os("HERDR_BIN_PATH").map(PathBuf::from))
        .unwrap_or_else(|| "herdr".into());
    if binary.components().count() > 1 {
        return Ok(fs::canonicalize(binary)?);
    }
    for dir in env::split_paths(&env::var_os("PATH").unwrap_or_default()) {
        if dir.join(&binary).is_file() {
            return Ok(fs::canonicalize(dir.join(&binary))?);
        }
    }
    anyhow::bail!("herdr executable not found; use --herdr /absolute/path")
}
fn request(root: &std::path::Path, map: &str, kind: RequestKind) -> Result<()> {
    let (_, key) = store::map_identity(map)?;
    let id = store::enqueue(&store::map_dir(root, &key)?, kind)?;
    println!(
        "Request {id} durably queued. The runtime applies it; inspect status for the outcome."
    );
    Ok(())
}
fn tracker_context(root: &std::path::Path, value: &str) -> Result<(MapRef, PathBuf)> {
    let map = MapRef::parse(value)?;
    let (_, key) = store::map_identity(value)?;
    let dir = store::map_dir(root, &key)?;
    let state = store::read_state(&dir).context("attach this map before tracker mutations")?;
    let canonical = format!(
        "{}/{}#{}",
        map.owner.to_ascii_lowercase(),
        map.repository.to_ascii_lowercase(),
        map.number
    );
    ensure!(
        state.map == canonical,
        "requested map is not attached to this state directory"
    );
    Ok((map, dir))
}

fn main() {
    if let Err(error) = run() {
        eprintln!("Wayfinder: {error:#}");
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    let cli = Cli::parse();
    let root = root(cli.state_dir)?;
    match cli.command {
        CommandName::Attach {
            map,
            repository,
            socket,
            workspace,
            herdr,
            poll_seconds,
            no_service,
        } => {
            let repository = fs::canonicalize(repository).context("repository must exist")?;
            ensure!(repository.is_dir(), "repository must be a directory");
            let socket = socket
                .or_else(|| env::var_os("HERDR_SOCKET_PATH").map(PathBuf::from))
                .context("provide --socket or invoke inside the intended herdr session")?;
            ensure!(socket.is_absolute(), "socket must be absolute");
            let binding = Binding {
                repository,
                socket,
                herdr_binary: executable(herdr)?,
                herdr_config: env::var_os("HERDR_CONFIG_PATH").map(PathBuf::from),
                source_workspace_id: workspace.or_else(|| env::var("HERDR_WORKSPACE_ID").ok()),
            };
            let (key, state) = store::attach(&root, &map, binding, poll_seconds)?;
            println!(
                "Map: {}\nAuthorization: {:?}\n{}\nKey: {}",
                state.map, state.authorization, state.suspension, key
            );
            if !no_service {
                // The installed unit has a fixed state root; refuse mismatched ad-hoc roots.
                ensure!(
                    root == self::root(None)?,
                    "custom state root requires --no-service and explicit service configuration"
                );
                let status = Command::new("systemctl")
                    .args([
                        "--user",
                        "enable",
                        "--now",
                        &format!("wayfinder-herdr@{key}.service"),
                    ])
                    .status()?;
                ensure!(
                    status.success(),
                    "service start failed; state retained. Install the unit and retry attach, or inspect systemctl --user status wayfinder-herdr@{key}"
                );
            }
        }
        CommandName::Start { map } => request(&root, &map, RequestKind::Start)?,
        CommandName::AuthorizeExisting { map, instruction } => {
            let map = MapRef::parse(&map)?;
            let outcome =
                tracker::GitHub::default().authorize_existing_map(&root, &map, &instruction)?;
            println!("{outcome}");
        }
        CommandName::BindFeatureCheckout {
            map,
            checkout,
            branch,
            head,
            base_ref,
            source_pr,
        } => {
            let (_, key) = store::map_identity(&map)?;
            let dir = store::map_dir(&root, &key)?;
            let state =
                store::read_state(&dir).context("attach this map before binding delivery")?;
            ensure!(state.map == map, "map binding differs from the named map");
            let (checkout, branch) = delivery::bind_feature_checkout(
                &dir, &state, &checkout, &branch, &head, &base_ref, source_pr,
            )?;
            println!(
                "Feature delivery checkout: {} ({branch})",
                checkout.display()
            );
        }
        CommandName::Pause { map } => request(&root, &map, RequestKind::Pause)?,
        CommandName::Resume { map } => request(&root, &map, RequestKind::Resume)?,
        CommandName::Status { map } => {
            let (_, key) = store::map_identity(&map)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&store::read_state(&store::map_dir(&root, &key)?)?)?
            );
        }
        CommandName::Chat { map } => orchestration::open_chat(&root, map.as_deref())?,
        CommandName::RecoverChat {
            map,
            confirm_replacement,
            confirm_launch_absent_or_stopped,
        } => orchestration::recover_chat(
            &root,
            map.as_deref(),
            confirm_replacement,
            confirm_launch_absent_or_stopped,
        )?,
        CommandName::ChatOutbox { map } => orchestration::list_deliveries(&root, &map)?,
        CommandName::ResolveChatDelivery {
            map,
            message,
            confirmed_delivered,
            confirmed_not_delivered,
        } => orchestration::resolve_uncertain_delivery(
            &root,
            &map,
            &message,
            confirmed_delivered,
            confirmed_not_delivered,
        )?,
        CommandName::ConfigureWorker {
            map,
            role,
            kind,
            model,
            reasoning_effort,
            args,
            concurrency,
        } => {
            let (_, dir) = tracker_context(&root, &map)?;
            if let Some(role) = &role {
                ensure!(
                    ["orchestrator", "researcher", "implementer", "reviewer"]
                        .contains(&role.as_str()),
                    "role must be orchestrator, researcher, implementer, or reviewer"
                );
            }
            let providers = [
                "pi",
                "claude",
                "codex",
                "gemini",
                "cursor",
                "devin",
                "agy",
                "cline",
                "omp",
                "mastracode",
                "opencode",
                "copilot",
                "kimi",
                "kiro",
                "droid",
                "amp",
                "grok",
                "hermes",
                "kilo",
                "qodercli",
                "qwen",
                "letta",
                "maki",
                "muse",
            ];
            ensure!(
                providers.contains(&kind.as_str()),
                "provider kind is not supported by herdr 0.9.3"
            );
            if let Some(concurrency) = concurrency {
                ensure!((1..=32).contains(&concurrency), "concurrency must be 1..32");
            }
            let _lock = Lock::acquire_wait(&dir.join("state.lock"))?;
            let mut state = store::read_state(&dir)?;
            let provider = Provider {
                kind,
                model,
                reasoning_effort,
                args,
            };
            if let Some(role) = role {
                state.workers.providers.roles.insert(role, provider);
            } else {
                state.workers.providers.default = provider;
            }
            if let Some(concurrency) = concurrency {
                state.concurrency = concurrency;
            }
            store::atomic_json(&dir.join("state.json"), &state)?;
            println!(
                "Worker provider configuration updated; existing runs retain their recorded provider-independent identity."
            );
        }
        CommandName::StopWorker { map, run } => {
            let (_, dir) = tracker_context(&root, &map)?;
            let _lock = Lock::acquire_wait(&dir.join("state.lock"))?;
            let mut state = store::read_state(&dir)?;
            let i = state
                .workers
                .runs
                .iter()
                .position(|worker| worker.id == run)
                .context("worker run not found")?;
            let worker = state.workers.runs[i].clone();
            let unsent_queued = worker.status == WorkerStatus::Queued
                && worker.workspace_id.is_none()
                && worker.tab_id.is_none()
                && worker.pane_id.is_none()
                && worker.terminal_id.is_none()
                && worker.agent_provider.is_none()
                && worker.agent_session.is_none()
                && worker.foreground_process.is_none()
                && worker.initial_prompt_attempted.is_none()
                && !worker.initial_prompt_pending
                && !worker.initial_prompt_acknowledged
                && worker.result_commit.is_none()
                && worker.result_evidence.is_none()
                && !worker.worktree.exists();
            if unsent_queued {
                state.workers.runs[i].status = WorkerStatus::Stopped;
                state.workers.runs[i].human_decision = Some(
                    "cancelled before dispatch; no checkout or worker process was created".into(),
                );
                store::atomic_json(&dir.join("state.json"), &state)?;
                println!(
                    "Undispatched queued worker {run} stopped; its durable history was retained."
                );
                return Ok(());
            }
            let confirmed_blocked = worker.status == WorkerStatus::NeedsHuman
                && worker.human_request_kind == Some(HumanRequestKind::HerdrBlockedUi);
            let confirmed_unsent = worker.status == WorkerStatus::Uncertain
                && worker.initial_prompt_pending
                && worker.initial_prompt_attempted == Some(false)
                && !worker.initial_prompt_acknowledged
                && worker.result_evidence.is_none();
            ensure!(
                worker.status == WorkerStatus::Running || confirmed_blocked || confirmed_unsent,
                "only a confirmed running worker, identity-verified Herdr-blocked worker, or exact unsent pre-prompt worker can be stopped"
            );
            let pane = worker.pane_id.clone().context("worker has no owned pane")?;
            let herdr = Client::new(&state.binding.socket);
            let inspected = if worker.initial_prompt_pending || worker.initial_prompt_acknowledged {
                wayfinder_herdr::herdr::inspect_pre_prompt_worker(&herdr, &worker)
            } else {
                wayfinder_herdr::herdr::inspect_worker(&herdr, &worker)
            };
            let agent = match inspected {
                Ok(agent) => agent,
                Err(error) => {
                    state.workers.runs[i].status = WorkerStatus::Uncertain;
                    state.workers.runs[i].question = Some(format!(
                        "Worker identity/process continuity could not be verified; pane was not closed: {error:#}"
                    ));
                    store::atomic_json(&dir.join("state.json"), &state)?;
                    anyhow::bail!(
                        "worker identity/process continuity could not be verified; pane was not closed: {error:#}"
                    );
                }
            };
            if worker.initial_prompt_reconnect_pending {
                state.workers.runs[i].initial_prompt_reconnect_pending = false;
                store::atomic_json(&dir.join("state.json"), &state)?;
            }
            if confirmed_blocked {
                ensure!(
                    agent["agent"]["agent_status"].as_str() == Some("blocked"),
                    "the worker is no longer confirmed blocked; reconcile before stopping"
                );
            }
            state.workers.runs[i].status = WorkerStatus::StopRequested;
            state.workers.runs[i].question =
                Some("Stop requested by the human; claim and worktree will be retained.".into());
            store::atomic_json(&dir.join("state.json"), &state)?;
            match herdr.close_pane(&pane) {
                Ok(_) => {
                    let worker = &mut state.workers.runs[i];
                    worker.status = WorkerStatus::Stopped;
                    worker.human_decision =
                        Some("human requested stop; Herdr confirmed pane close".into());
                    store::atomic_json(&dir.join("state.json"), &state)?;
                    println!("Worker stop confirmed; claim and worktree remain retained.");
                }
                Err(error) => {
                    let worker = &mut state.workers.runs[i];
                    worker.status = WorkerStatus::Uncertain;
                    worker.question = Some(format!(
                        "Stop outcome is uncertain; do not retry without confirming the worker is absent: {error:#}"
                    ));
                    store::atomic_json(&dir.join("state.json"), &state)?;
                    anyhow::bail!(
                        "stop outcome is uncertain; inspect pane {pane} and status before retrying: {error:#}"
                    );
                }
            }
        }
        CommandName::AnswerWorker {
            map,
            run,
            request_id,
            request_type,
            response,
        } => {
            ensure!(
                !response.trim().is_empty(),
                "human response cannot be empty"
            );
            let (_, dir) = tracker_context(&root, &map)?;
            let _lock = Lock::acquire_wait(&dir.join("state.lock"))?;
            let mut state = store::read_state(&dir)?;
            let index = state
                .workers
                .runs
                .iter()
                .position(|worker| worker.id == run)
                .context("worker run not found")?;
            let saved = &state.workers.runs[index];
            let recoverable_pre_effect_identity_hold =
                saved.status == WorkerStatus::Uncertain
                    && saved.question.as_deref().is_some_and(|question| {
                        question.starts_with(
                            "Worker identity/process continuity could not be verified; human response was not submitted:",
                        )
                    })
                    && saved.human_response.is_none()
                    && saved.answer_history.is_empty()
                    && saved.initial_prompt_attempted == Some(true);
            ensure!(
                state.workers.runs[index].status == WorkerStatus::NeedsHuman
                    || recoverable_pre_effect_identity_hold,
                "worker has no pending human question or has an unrelated uncertainty"
            );
            let worker = state.workers.runs[index].clone();
            let request_kind = match request_type.as_str() {
                "worker_question" => HumanRequestKind::WorkerQuestion,
                "herdr_blocked_ui" => HumanRequestKind::HerdrBlockedUi,
                _ => unreachable!("clap validates the request type"),
            };
            ensure!(
                worker.human_request_id.as_deref() == Some(request_id.as_str())
                    && worker.human_request_kind == Some(request_kind),
                "human request ID/type is stale or does not match this worker"
            );
            let pane = worker
                .pane_id
                .clone()
                .context("blocked worker has no owned pane")?;
            let herdr = Client::new(&state.binding.socket);
            let inspected = if worker.initial_prompt_pending || worker.initial_prompt_acknowledged {
                wayfinder_herdr::herdr::inspect_pre_prompt_worker(&herdr, &worker)
            } else {
                wayfinder_herdr::herdr::inspect_worker(&herdr, &worker)
            };
            let agent = match inspected {
                Ok(agent) => agent,
                Err(error) => {
                    state.workers.runs[index].status = WorkerStatus::Uncertain;
                    state.workers.runs[index].question = Some(format!(
                        "Worker identity/process continuity could not be verified; human response was not submitted: {error:#}"
                    ));
                    store::atomic_json(&dir.join("state.json"), &state)?;
                    anyhow::bail!(
                        "worker identity could not be verified; response was not submitted: {error:#}"
                    );
                }
            };
            if worker.initial_prompt_reconnect_pending {
                state.workers.runs[index].initial_prompt_reconnect_pending = false;
                store::atomic_json(&dir.join("state.json"), &state)?;
            }
            let agent_status = agent["agent"]["agent_status"].as_str().unwrap_or("unknown");
            if request_kind == HumanRequestKind::HerdrBlockedUi {
                ensure!(
                    agent_status == "blocked",
                    "the correlated Herdr blocked UI is no longer active"
                );
                let current = herdr.read_recent(&pane)?["read"]["text"]
                    .as_str()
                    .context("Herdr omitted the current blocked prompt text")?
                    .to_owned();
                if store::human_request_fingerprint(&current)
                    != worker
                        .human_request_fingerprint
                        .as_deref()
                        .unwrap_or_default()
                {
                    let pending = &mut state.workers.runs[index];
                    pending.set_human_request(HumanRequestKind::HerdrBlockedUi, &current);
                    pending.question = Some(current);
                    store::atomic_json(&dir.join("state.json"), &state)?;
                    anyhow::bail!(
                        "the blocked Herdr prompt changed; review the updated prompt and use its new request ID/type"
                    );
                }
                let pending = &mut state.workers.runs[index];
                pending.human_response = Some(response.clone());
                pending.answer_request_id = Some(request_id.clone());
                pending.answer_request_kind = Some(request_kind);
                pending.answer_history.push(HumanAnswerEvidence {
                    request_id: request_id.clone(),
                    request_kind,
                    response,
                    disposition: AnswerDisposition::ManualRequired,
                });
                store::atomic_json(&dir.join("state.json"), &state)?;
                anyhow::bail!(
                    "Herdr blocked UI responses are not sent automatically. Open the named pane {pane}, inspect the current approval/question, and interact with it directly; then run `wayfinder-herdr reconcile --map {map}`. The worker remains needs_human and retains capacity and evidence"
                );
            } else if agent_status == "blocked" {
                let current = herdr.read_recent(&pane)?["read"]["text"]
                    .as_str()
                    .context("Herdr omitted the current blocked prompt text")?
                    .to_owned();
                let pending = &mut state.workers.runs[index];
                pending.set_human_request(HumanRequestKind::HerdrBlockedUi, &current);
                pending.question = Some(current);
                store::atomic_json(&dir.join("state.json"), &state)?;
                anyhow::bail!(
                    "worker entered a Herdr blocked UI; no answer was submitted. Review the current prompt and use its new request ID/type"
                );
            } else {
                ensure!(
                    matches!(agent_status, "idle" | "done"),
                    "worker question can only be answered while its exact worker is idle or done"
                );
            }
            state.workers.runs[index].human_response = Some(response.clone());
            state.workers.runs[index].answer_request_id = Some(request_id.clone());
            state.workers.runs[index].answer_request_kind = Some(request_kind);
            state.workers.runs[index]
                .answer_history
                .push(HumanAnswerEvidence {
                    request_id: request_id.clone(),
                    request_kind,
                    response: response.clone(),
                    disposition: AnswerDisposition::Intent,
                });
            state.workers.runs[index].status = WorkerStatus::AnswerIntent;
            store::atomic_json(&dir.join("state.json"), &state)?;
            match herdr.prompt(&pane, &response) {
                Ok(_) => {
                    let observed = wayfinder_herdr::herdr::inspect_worker(&herdr, &worker);
                    if let Err(error) = observed {
                        state.workers.runs[index].status = WorkerStatus::Uncertain;
                        if let Some(answer) = state.workers.runs[index].answer_history.last_mut() {
                            answer.disposition = AnswerDisposition::Uncertain;
                        }
                        state.workers.runs[index].question = Some(format!(
                            "Human response may have been submitted but worker identity changed; it was not repeated: {error:#}"
                        ));
                        store::atomic_json(&dir.join("state.json"), &state)?;
                        anyhow::bail!("response outcome is retained as uncertain: {error:#}");
                    }
                    let worker = &mut state.workers.runs[index];
                    worker.status = WorkerStatus::Running;
                    worker.last_activity_ms = Some(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis()
                            .min(u64::MAX as u128) as u64,
                    );
                    worker.question = None;
                    worker.human_decision = Some("human response submitted".into());
                    worker.human_request_id = None;
                    worker.human_request_kind = None;
                    worker.human_request_fingerprint = None;
                    if let Some(answer) = worker.answer_history.last_mut() {
                        answer.disposition = AnswerDisposition::Submitted;
                    }
                    store::atomic_json(&dir.join("state.json"), &state)?;
                    println!("Human response submitted to owned pane {pane}.");
                }
                Err(error)
                    if request_kind == HumanRequestKind::WorkerQuestion
                        && error
                            .downcast_ref::<wayfinder_herdr::herdr::HerdrApiError>()
                            .is_some_and(
                                wayfinder_herdr::herdr::HerdrApiError::is_agent_blocked,
                            ) =>
                {
                    let question = herdr
                        .read_recent(&pane)
                        .ok()
                        .and_then(|value| value["read"]["text"].as_str().map(str::to_owned))
                        .filter(|text| !text.trim().is_empty())
                        .unwrap_or_else(|| {
                            format!(
                                "Herdr confirmed this worker is blocked, but its prompt could not be read. Inspect the named pane {pane} directly before responding."
                            )
                        });
                    let worker = &mut state.workers.runs[index];
                    worker.status = WorkerStatus::NeedsHuman;
                    if let Some(answer) = worker.answer_history.last_mut() {
                        answer.disposition = AnswerDisposition::RejectedBeforeEffect;
                    }
                    worker.set_human_request(HumanRequestKind::HerdrBlockedUi, &question);
                    worker.question = Some(question);
                    store::atomic_json(&dir.join("state.json"), &state)?;
                    anyhow::bail!(
                        "Herdr confirmed the prompt was rejected before input; no answer was submitted. Inspect the named pane {pane} directly and use the new manual request ID/type after reviewing its current UI"
                    );
                }
                Err(error) => {
                    let worker = &mut state.workers.runs[index];
                    worker.status = WorkerStatus::Uncertain;
                    if let Some(answer) = worker.answer_history.last_mut() {
                        answer.disposition = AnswerDisposition::Uncertain;
                    }
                    worker.question = Some(format!(
                        "Human response submission is ambiguous; it was not repeated: {error:#}"
                    ));
                    store::atomic_json(&dir.join("state.json"), &state)?;
                    anyhow::bail!(
                        "response submission uncertain; inspect pane {pane} and status before retrying: {error:#}"
                    );
                }
            }
        }
        CommandName::AnswerDecision {
            map,
            request_id,
            response,
            disposition,
        } => {
            let (_, dir) = tracker_context(&root, &map)?;
            let _lock = Lock::acquire_wait(&dir.join("state.lock"))?;
            let mut state = store::read_state(&dir)?;
            let disposition = match disposition.as_str() {
                "continue" => SchedulerDecisionDisposition::Continue,
                "defer" => SchedulerDecisionDisposition::Defer,
                "abandon" => SchedulerDecisionDisposition::Abandon,
                _ => unreachable!("clap validated scheduler disposition"),
            };
            store::record_scheduler_decision_response(
                &mut state,
                &request_id,
                &response,
                disposition,
            )?;
            store::atomic_json(&dir.join("state.json"), &state)?;
            println!(
                "Recorded the human response and {disposition:?} action for scheduler decision {request_id}; runtime reconciliation applies it."
            );
        }
        CommandName::RetryWorker {
            map,
            run,
            confirmed_absent_or_stopped,
        } => {
            let (_, dir) = tracker_context(&root, &map)?;
            let _lock = Lock::acquire_wait(&dir.join("state.lock"))?;
            let mut state = store::read_state(&dir)?;
            let old = state
                .workers
                .runs
                .iter()
                .position(|worker| worker.id == run)
                .context("worker run not found")?;
            let prior = state.workers.runs[old].clone();
            if let Some(existing) = state.workers.runs.iter().find(|candidate| {
                candidate.source_run.as_deref() == Some(prior.id.as_str())
                    && !matches!(
                        candidate.status,
                        WorkerStatus::Stopped | WorkerStatus::Failed
                    )
            }) {
                println!(
                    "Retry already recorded as {}; no additional worker was created.",
                    existing.id
                );
                return Ok(());
            }
            ensure!(
                matches!(prior.status, WorkerStatus::Stopped | WorkerStatus::Failed)
                    || (confirmed_absent_or_stopped
                        && matches!(
                            prior.status,
                            WorkerStatus::Uncertain | WorkerStatus::NeedsHuman
                        )),
                "retry requires a stopped/failed worker, or --confirmed-absent-or-stopped for an uncertain worker"
            );
            ensure!(
                prior.status != WorkerStatus::Running
                    && prior.status != WorkerStatus::StopRequested,
                "an active worker cannot be retried"
            );
            state.workers.runs[old].human_decision = Some(if confirmed_absent_or_stopped {
                "human confirmed previous worker absent or stopped".into()
            } else {
                "human resumed confirmed stopped/failed worker".into()
            });
            if confirmed_absent_or_stopped {
                state.workers.runs[old].status = WorkerStatus::Stopped;
            }
            state.workers.next_run = state.workers.next_run.saturating_add(1);
            let number = state.workers.next_run;
            let purpose = prior.effective_purpose();
            let map_ref = MapRef::parse(&state.map)?;
            let worktree = state
                .binding
                .repository
                .parent()
                .unwrap_or(&state.binding.repository)
                .join(format!(
                    ".wayfinder-{}-{}-{number}",
                    map_ref.number, prior.ticket
                ));
            state.workers.runs.push(WorkerRun {
                id: format!("run-{number:020}"), ticket: prior.ticket, role: prior.role.clone(), attempt: prior.attempt.saturating_add(1), automatic_retries: 0, rework_round: prior.rework_round, rework_round_limit: prior.rework_round_limit,
                status: WorkerStatus::Queued, worktree, workspace_id: None, tab_id: None, pane_id: None,
                base_commit: prior.result_commit.clone().or(prior.base_commit.clone()), result_commit: None,
                summary: None, question: None, human_response: None, human_decision: None,
                human_request_seq: 0, human_request_id: None, human_request_kind: None,
                human_request_fingerprint: None, answer_request_id: None, answer_request_kind: None,
                answer_history: Vec::new(),
                source_run: Some(prior.id), claim_login: prior.claim_login, context: Some("Human explicitly authorized this retry after the preceding worker was confirmed stopped or absent.".into()), purpose: Some(purpose), last_activity_ms: None, terminal_id: None, agent_provider: None, agent_session: None, foreground_process: None, result_evidence: None, result_evidence_history: Vec::new(), initial_prompt_pending: false, initial_prompt_acknowledged: false, initial_prompt_attempted: None, initial_prompt_reconnect_pending: false, known_prelaunch_failure: false,
            });
            store::atomic_json(&dir.join("state.json"), &state)?;
            println!(
                "Retry intent durably recorded as run-{:020}; runtime will honor map capacity and reconcile before dispatch.",
                number
            );
        }
        CommandName::AbandonWorker { map, run } => {
            let (_, dir) = tracker_context(&root, &map)?;
            let _lock = Lock::acquire_wait(&dir.join("state.lock"))?;
            let mut state = store::read_state(&dir)?;
            let worker = state
                .workers
                .runs
                .iter_mut()
                .find(|worker| worker.id == run)
                .context("worker run not found")?;
            ensure!(
                matches!(
                    worker.status,
                    WorkerStatus::Uncertain
                        | WorkerStatus::NeedsHuman
                        | WorkerStatus::Stopped
                        | WorkerStatus::Failed
                ),
                "only retained or settled work can be abandoned"
            );
            worker.human_decision =
                Some("human selected abandon; keep resources and artifacts retained".into());
            store::atomic_json(&dir.join("state.json"), &state)?;
            println!(
                "Abandon decision recorded; this does not prove termination, release uncertain capacity, or remove artifacts."
            );
        }
        CommandName::Reconcile { map: Some(map) } => request(&root, &map, RequestKind::Reconcile)?,
        CommandName::Reconcile { map: None } => {
            if !root.join("maps").exists() {
                return Ok(());
            }
            // No state is created by installation, startup, or hooks.
            for entry in fs::read_dir(root.join("maps"))? {
                let path = entry?.path();
                if path.is_dir() {
                    // One corrupt map must not starve requests to other maps.
                    if let Err(error) = store::enqueue(&path, RequestKind::Reconcile) {
                        eprintln!("{error:#}");
                    }
                }
            }
        }
        CommandName::Serve { key, once } => runtime::serve(&root, &key, once)?,
        CommandName::Action { name } => {
            let socket =
                env::var_os("HERDR_SOCKET_PATH").context("missing herdr action socket context")?;
            let context: serde_json::Value = serde_json::from_str(
                &env::var("HERDR_PLUGIN_CONTEXT_JSON").context("missing herdr action context")?,
            )?;
            let cwd = context["workspace_cwd"]
                .as_str()
                .context("action needs workspace cwd; use explicit CLI --map if unavailable")?;
            let cwd = fs::canonicalize(cwd)?;
            let mut matching = vec![];
            if root.join("maps").exists() {
                for entry in fs::read_dir(root.join("maps"))? {
                    let state = store::read_state(&entry?.path())?;
                    if state.binding.repository == cwd && state.binding.socket.as_os_str() == socket
                    {
                        matching.push(state);
                    }
                }
            }
            ensure!(
                matching.len() == 1,
                "action requires exactly one attached map in this workspace/session; use explicit CLI --map"
            );
            let state = &matching[0];
            if name == "status" {
                println!("{}", serde_json::to_string_pretty(state)?);
            } else {
                request(
                    &root,
                    &state.map,
                    match name.as_str() {
                        "start" => RequestKind::Start,
                        "pause" => RequestKind::Pause,
                        _ => RequestKind::Resume,
                    },
                )?;
            }
        }
        CommandName::Tracker { command } => {
            let github = tracker::GitHub::default();
            match command {
                TrackerCommand::CreateMap {
                    repository,
                    title,
                    notes,
                    execution_override,
                } => {
                    let (owner, repo) = repository
                        .split_once('/')
                        .context("repository must use OWNER/REPOSITORY")?;
                    let issue = github.create_map(
                        owner,
                        repo,
                        &title,
                        &notes,
                        execution_override,
                        &root,
                    )?;
                    println!(
                        "Created map: {}",
                        issue["html_url"].as_str().unwrap_or("GitHub issue created")
                    );
                }
                TrackerCommand::CreateTicket {
                    map,
                    title,
                    body,
                    label,
                } => {
                    let (map, dir) = tracker_context(&root, &map)?;
                    let issue =
                        github.create_ticket(&map, &TicketInput { title, body, label }, &dir)?;
                    println!(
                        "Created ticket: {}",
                        issue["html_url"].as_str().unwrap_or("GitHub issue created")
                    );
                }
                TrackerCommand::Frontier { map } => {
                    let map = MapRef::parse(&map)?;
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&github.reconcile(&map)?)?
                    );
                }
                TrackerCommand::Block { map, ticket, by } => {
                    let (map, dir) = tracker_context(&root, &map)?;
                    github.add_dependency(&map, ticket, by, &dir)?;
                    println!("Added native dependency: #{ticket} blocked by #{by}");
                }
                TrackerCommand::Claim {
                    map,
                    ticket,
                    assignee,
                } => {
                    let (map, dir) = tracker_context(&root, &map)?;
                    let login = github.claim(&map, ticket, Some(&assignee), &dir)?;
                    println!("Claimed #{ticket} as @{login}");
                }
                TrackerCommand::ReconcileClaim {
                    map,
                    ticket,
                    assignee,
                } => {
                    let (map, dir) = tracker_context(&root, &map)?;
                    let login = github.reconcile_claim(&map, ticket, Some(&assignee), &dir)?;
                    println!(
                        "Confirmed existing GitHub claim for #{ticket} as @{login}; no assignment was changed."
                    );
                }
                TrackerCommand::Resolve {
                    map,
                    ticket,
                    resolution,
                    spec,
                } => {
                    let (map, dir) = tracker_context(&root, &map)?;
                    github.resolve(&map, ticket, &resolution, spec, &dir)?;
                    println!("Recorded resolution for #{ticket}; map and spec indexes updated.");
                }
            }
        }
    }
    Ok(())
}
