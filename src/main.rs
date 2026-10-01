use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use std::{env, fs, path::PathBuf, process::Command};
use wayfinder_herdr::{
    runtime,
    store::{self, Binding, RequestKind},
};

#[derive(Parser)]
#[command(
    version,
    about = "Wayfinder local runtime foundation (dispatch not implemented)"
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
    }
    Ok(())
}
