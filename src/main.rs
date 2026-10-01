use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use std::{env, fs, path::PathBuf, process::Command};
use wayfinder_herdr::{
    herdr::Client,
    runtime,
    store::{self, Binding, Lock, Provider, RequestKind, WorkerRun, WorkerStatus},
    tracker::{self, MapRef, TicketInput},
};

#[derive(Parser)]
#[command(
    version,
    about = "Wayfinder GitHub map workflow and local runtime (worker dispatch not implemented)"
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
    /// Configure the provider used for delegated worker roles.
    ConfigureWorker {
        #[arg(long)]
        map: String,
        /// One of researcher, implementer, reviewer; omit to set shared defaults.
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
    /// Submit an actual human answer to a worker blocked on a question.
    AnswerWorker {
        #[arg(long)]
        map: String,
        #[arg(long)]
        run: String,
        #[arg(long)]
        response: String,
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
        CommandName::Pause { map } => request(&root, &map, RequestKind::Pause)?,
        CommandName::Resume { map } => request(&root, &map, RequestKind::Resume)?,
        CommandName::Status { map } => {
            let (_, key) = store::map_identity(&map)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&store::read_state(&store::map_dir(&root, &key)?)?)?
            );
        }
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
            let _lock = Lock::acquire(&dir.join("state.lock"))?;
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
            let _lock = Lock::acquire(&dir.join("state.lock"))?;
            let mut state = store::read_state(&dir)?;
            let worker = state
                .workers
                .runs
                .iter_mut()
                .find(|worker| worker.id == run)
                .context("worker run not found")?;
            ensure!(
                worker.status == WorkerStatus::Running,
                "only a confirmed running worker can be stopped; uncertain and blocked runs require reconciliation"
            );
            let pane = worker
                .pane_id
                .clone()
                .context("running worker has no owned pane")?;
            worker.status = WorkerStatus::StopRequested;
            worker.question =
                Some("Stop requested by the human; claim and worktree will be retained.".into());
            store::atomic_json(&dir.join("state.json"), &state)?;
            let herdr = Client::new(&state.binding.socket);
            match herdr.close_pane(&pane) {
                Ok(_) => {
                    let worker = state
                        .workers
                        .runs
                        .iter_mut()
                        .find(|worker| worker.id == run)
                        .unwrap();
                    worker.status = WorkerStatus::Stopped;
                    worker.human_decision =
                        Some("human requested stop; Herdr confirmed pane close".into());
                    store::atomic_json(&dir.join("state.json"), &state)?;
                    println!("Worker stop confirmed; claim and worktree remain retained.");
                }
                Err(error) => {
                    let worker = state
                        .workers
                        .runs
                        .iter_mut()
                        .find(|worker| worker.id == run)
                        .unwrap();
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
        CommandName::AnswerWorker { map, run, response } => {
            ensure!(
                !response.trim().is_empty(),
                "human response cannot be empty"
            );
            let (_, dir) = tracker_context(&root, &map)?;
            let _lock = Lock::acquire(&dir.join("state.lock"))?;
            let mut state = store::read_state(&dir)?;
            let worker = state
                .workers
                .runs
                .iter_mut()
                .find(|worker| worker.id == run)
                .context("worker run not found")?;
            ensure!(
                worker.status == WorkerStatus::NeedsHuman,
                "worker has no pending human question"
            );
            let pane = worker
                .pane_id
                .clone()
                .context("blocked worker has no owned pane")?;
            worker.human_response = Some(response.clone());
            worker.status = WorkerStatus::AnswerIntent;
            store::atomic_json(&dir.join("state.json"), &state)?;
            let herdr = Client::new(&state.binding.socket);
            match herdr.prompt(&pane, &response) {
                Ok(_) => {
                    let worker = state
                        .workers
                        .runs
                        .iter_mut()
                        .find(|worker| worker.id == run)
                        .unwrap();
                    worker.status = WorkerStatus::Running;
                    worker.question = None;
                    worker.human_decision = Some("human response submitted".into());
                    store::atomic_json(&dir.join("state.json"), &state)?;
                    println!("Human response submitted to owned pane {pane}.");
                }
                Err(error) => {
                    let worker = state
                        .workers
                        .runs
                        .iter_mut()
                        .find(|worker| worker.id == run)
                        .unwrap();
                    worker.status = WorkerStatus::Uncertain;
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
        CommandName::RetryWorker {
            map,
            run,
            confirmed_absent_or_stopped,
        } => {
            let (_, dir) = tracker_context(&root, &map)?;
            let _lock = Lock::acquire(&dir.join("state.lock"))?;
            let mut state = store::read_state(&dir)?;
            let old = state
                .workers
                .runs
                .iter()
                .position(|worker| worker.id == run)
                .context("worker run not found")?;
            let prior = state.workers.runs[old].clone();
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
                id: format!("run-{number:020}"), ticket: prior.ticket, role: prior.role.clone(), attempt: prior.attempt.saturating_add(1), automatic_retries: 0, rework_round: prior.rework_round,
                status: WorkerStatus::Queued, worktree, workspace_id: None, tab_id: None, pane_id: None,
                base_commit: prior.result_commit.clone().or(prior.base_commit.clone()), result_commit: None,
                summary: None, question: None, human_response: None, human_decision: None,
                source_run: Some(prior.id), claim_login: prior.claim_login, context: Some("Human explicitly authorized this retry after the preceding worker was confirmed stopped or absent.".into()), last_activity_ms: None,
            });
            store::atomic_json(&dir.join("state.json"), &state)?;
            println!(
                "Retry intent durably recorded as run-{:020}; runtime will honor map capacity and reconcile before dispatch.",
                number
            );
        }
        CommandName::AbandonWorker { map, run } => {
            let (_, dir) = tracker_context(&root, &map)?;
            let _lock = Lock::acquire(&dir.join("state.lock"))?;
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
