//! Small, explicitly-targeted client for the installed Herdr socket API.
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::fmt;
use std::{
    io::{BufRead, BufReader, Read, Write},
    os::unix::net::UnixStream,
    path::Path,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use crate::store::{AgentSessionIdentity, LinuxProcessIdentity, WorkerRun};

static REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct HerdrApiError {
    pub method: String,
    pub code: String,
    pub message: String,
}

impl fmt::Display for HerdrApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "herdr {} failed: {} ({})",
            self.method, self.message, self.code
        )
    }
}

impl std::error::Error for HerdrApiError {}

impl HerdrApiError {
    pub fn is_agent_blocked(&self) -> bool {
        self.method == "agent.prompt" && self.code == "agent_blocked"
    }

    pub fn is_agent_not_found(&self) -> bool {
        self.method == "agent.get" && self.code == "agent_not_found"
    }
}

#[derive(Clone)]
pub struct Client {
    socket: std::path::PathBuf,
}

/// Persist only identity facts present in the installed AgentInfo response. A pane
/// ID or agent name by itself is not enough to identify a restored occupant.
pub fn capture_agent_identity(
    info: &Value,
    run: &WorkerRun,
) -> Result<(String, Option<String>, Option<AgentSessionIdentity>)> {
    let agent = info
        .get("agent")
        .context("Herdr response omitted agent information")?;
    ensure!(
        field(agent, "workspace_id")? == run.workspace_id.as_deref().unwrap_or_default(),
        "Herdr worker workspace changed"
    );
    ensure!(
        field(agent, "tab_id")? == run.tab_id.as_deref().unwrap_or_default(),
        "Herdr worker tab changed"
    );
    ensure!(
        field(agent, "pane_id")? == run.pane_id.as_deref().unwrap_or_default(),
        "Herdr worker pane changed"
    );
    let terminal_id = field(agent, "terminal_id")?.to_owned();
    ensure!(!terminal_id.is_empty(), "Herdr worker terminal ID is empty");
    if let Some(expected) = run.terminal_id.as_deref() {
        ensure!(
            terminal_id == expected,
            "Herdr worker terminal changed during startup"
        );
    }
    let provider = agent
        .get("agent")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if let Some(expected) = run.agent_provider.as_deref() {
        ensure!(
            provider.as_deref() == Some(expected),
            "Herdr worker provider identity changed"
        );
    }
    let session = agent
        .get("agent_session")
        .filter(|value| !value.is_null())
        .map(|session| -> Result<AgentSessionIdentity> {
            Ok(AgentSessionIdentity {
                source: field(session, "source")?.to_owned(),
                agent: field(session, "agent")?.to_owned(),
                kind: field(session, "kind")?.to_owned(),
                value: field(session, "value")?.to_owned(),
            })
        })
        .transpose()?;
    if let (Some(expected), Some(observed)) = (run.agent_session.as_ref(), session.as_ref()) {
        ensure!(observed == expected, "Herdr agent session identity changed");
    }
    Ok((terminal_id, provider, session))
}

/// Require full resource and observed agent-session identity before interpreting
/// lifecycle state or issuing an effect against a pane.
pub fn verify_worker_identity(info: &Value, run: &WorkerRun) -> Result<()> {
    ensure!(
        run.terminal_id.is_some(),
        "worker has no persisted terminal identity"
    );
    ensure!(
        run.agent_session.is_some(),
        "worker has no persisted agent-session identity"
    );
    let (terminal, provider, session) = capture_agent_identity(info, run)?;
    ensure!(
        Some(terminal) == run.terminal_id,
        "Herdr worker terminal identity changed"
    );
    ensure!(
        provider == run.agent_provider,
        "Herdr worker provider identity changed"
    );
    ensure!(
        session == run.agent_session,
        "Herdr agent session identity changed"
    );
    Ok(())
}

pub fn capture_foreground_process(info: &Value, pane_id: &str) -> Result<LinuxProcessIdentity> {
    let process_info = info
        .get("process_info")
        .context("Herdr pane.process_info omitted process_info")?;
    ensure!(
        field(process_info, "pane_id")? == pane_id,
        "Herdr process information belongs to another pane"
    );
    let group = process_info["foreground_process_group_id"]
        .as_u64()
        .context("Herdr did not identify the foreground process group")?;
    let pid = process_info["foreground_processes"]
        .as_array()
        .context("Herdr omitted foreground process list")?
        .iter()
        .find(|process| process["pid"].as_u64() == Some(group))
        .and_then(|process| process["pid"].as_u64())
        .context("Herdr did not report the foreground process-group leader")?;
    let pid = u32::try_from(pid).context("foreground process ID exceeded Linux PID range")?;
    let start_time_ticks = linux_process_start_time(pid)?;
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .to_owned();
    ensure!(!boot_id.is_empty(), "Linux boot ID is empty");
    Ok(LinuxProcessIdentity {
        boot_id,
        pid,
        start_time_ticks,
    })
}

pub fn verify_foreground_process(info: &Value, run: &WorkerRun) -> Result<()> {
    let expected = run
        .foreground_process
        .as_ref()
        .context("worker has no persisted foreground process identity")?;
    let pane = run
        .pane_id
        .as_deref()
        .context("worker has no persisted pane identity")?;
    let current = capture_foreground_process(info, pane)?;
    ensure!(
        &current == expected,
        "Herdr foreground process identity changed"
    );
    Ok(())
}

pub fn inspect_worker(client: &Client, run: &WorkerRun) -> Result<Value> {
    let pane = run
        .pane_id
        .as_deref()
        .context("worker has no persisted pane identity")?;
    let agent = client.agent(pane)?;
    verify_worker_identity(&agent, run)?;
    let process = client.pane_process_info(pane)?;
    verify_foreground_process(&process, run)?;
    Ok(agent)
}

/// Verify the original process and terminal before the first task prompt. Herdr
/// may not expose agent_session until a conversation begins, so this proof uses
/// the observed Linux foreground process fingerprint and binds any session that
/// has appeared only when it matches a previously persisted value.
pub fn inspect_pre_prompt_worker(client: &Client, run: &WorkerRun) -> Result<Value> {
    let pane = run
        .pane_id
        .as_deref()
        .context("worker has no persisted pane identity")?;
    let agent = client.agent(pane)?;
    verify_pre_prompt_agent_identity(&agent, run)?;
    let process = client.pane_process_info(pane)?;
    verify_foreground_process(&process, run)?;
    Ok(agent)
}

pub fn verify_pre_prompt_agent_identity(info: &Value, run: &WorkerRun) -> Result<()> {
    let expected_terminal = run
        .terminal_id
        .as_deref()
        .context("worker has no persisted terminal identity")?;
    let (terminal, provider, session) = capture_agent_identity(info, run)?;
    ensure!(
        terminal == expected_terminal,
        "Herdr worker terminal identity changed"
    );
    ensure!(
        provider == run.agent_provider,
        "Herdr worker provider identity changed"
    );
    if let Some(expected) = run.agent_session.as_ref() {
        ensure!(
            session.as_ref() == Some(expected),
            "Herdr agent session identity changed"
        );
    }
    Ok(())
}

fn linux_process_start_time(pid: u32) -> Result<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .with_context(|| format!("read Linux process identity for PID {pid}"))?;
    let close = stat
        .rfind(')')
        .context("Linux process stat omitted command delimiter")?;
    let fields: Vec<_> = stat[close + 1..].split_whitespace().collect();
    // The suffix starts at proc stat field 3; starttime is field 22.
    fields
        .get(19)
        .context("Linux process stat omitted starttime")?
        .parse()
        .context("Linux process starttime was not an integer")
}

fn field<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("Herdr identity omitted {key}"))
}

impl Client {
    pub fn new(socket: &Path) -> Self {
        Self {
            socket: socket.to_owned(),
        }
    }

    /// Make one bounded request on the caller-selected session socket. All mutations
    /// also carry explicit pane/workspace IDs returned by Herdr; ambient focus is unused.
    pub fn request(&self, method: &str, params: Value) -> Result<Value> {
        let mut stream = UnixStream::connect(&self.socket)
            .with_context(|| format!("connect to bound herdr socket {}", self.socket.display()))?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
        let id = format!("wayfinder-{}", REQUEST_ID.fetch_add(1, Ordering::Relaxed));
        let body = serde_json::to_vec(&json!({"id": id, "method": method, "params": params}))?;
        stream.write_all(&body)?;
        stream.write_all(b"\n")?;
        stream.flush()?;
        let mut line = String::new();
        BufReader::new(stream)
            .take(1_048_577)
            .read_line(&mut line)
            .context("read herdr response")?;
        ensure!(line.len() <= 1_048_576, "herdr response exceeded 1 MiB");
        ensure!(
            !line.is_empty(),
            "herdr closed the socket without a response"
        );
        let response: Value = serde_json::from_str(&line).context("decode herdr response")?;
        ensure!(
            response["id"] == id,
            "herdr response ID did not match request"
        );
        if let Some(error) = response.get("error") {
            return Err(HerdrApiError {
                method: method.to_owned(),
                code: error["code"].as_str().unwrap_or("unknown").to_owned(),
                message: error["message"]
                    .as_str()
                    .unwrap_or("unknown error")
                    .to_owned(),
            }
            .into());
        }
        response
            .get("result")
            .cloned()
            .context("herdr response omitted result")
    }

    pub fn add_detached_worktree(
        &self,
        repository: &Path,
        path: &Path,
        base: Option<&str>,
    ) -> Result<()> {
        let mut command = Command::new("git");
        command
            .args(["-C"])
            .arg(repository)
            .args(["worktree", "add", "--detach"])
            .arg(path);
        if let Some(base) = base {
            command.arg(base);
        }
        let output = command
            .output()
            .context("create detached ticket worktree")?;
        ensure!(
            output.status.success(),
            "git worktree add --detach failed ({}) in {}: {}",
            output.status,
            repository.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
        Ok(())
    }

    pub fn open_worktree(
        &self,
        path: &Path,
        label: &str,
        source_workspace_id: &str,
    ) -> Result<Value> {
        self.request(
            "worktree.open",
            json!({"path":path,"label":label,"workspace_id":source_workspace_id,"focus":false,"trust_repository":false}),
        )
    }

    pub fn worktree_list(&self, path: &Path) -> Result<Value> {
        self.request(
            "worktree.list",
            json!({"cwd":path,"trust_repository":false}),
        )
    }

    pub fn pane_list(&self, workspace_id: &str) -> Result<Value> {
        self.request("pane.list", json!({"workspace_id":workspace_id}))
    }

    pub fn start_agent(
        &self,
        name: &str,
        kind: &str,
        pane_id: &str,
        args: &[String],
    ) -> Result<Value> {
        self.request(
            "agent.start",
            json!({"name":name,"kind":kind,"pane_id":pane_id,"args":args,"timeout_ms":30000}),
        )
    }

    pub fn split_pane(
        &self,
        pane_id: &str,
        workspace_id: &str,
        cwd: &Path,
        focus: bool,
    ) -> Result<Value> {
        self.request(
            "pane.split",
            json!({
                "target_pane_id": pane_id,
                "workspace_id": workspace_id,
                "cwd": cwd,
                "direction": "right",
                "focus": focus,
            }),
        )
    }

    pub fn focus_agent(&self, pane_id: &str) -> Result<Value> {
        self.request("agent.focus", json!({"target": pane_id}))
    }

    pub fn prompt(&self, pane_id: &str, text: &str) -> Result<Value> {
        self.request(
            "agent.prompt",
            json!({
                "target":pane_id,
                "text":text,
                "wait":{"until":["working","blocked"],"timeout_ms":5000}
            }),
        )
    }

    pub fn agent(&self, pane_id: &str) -> Result<Value> {
        self.request("agent.get", json!({"target":pane_id}))
    }

    pub fn pane_process_info(&self, pane_id: &str) -> Result<Value> {
        self.request("pane.process_info", json!({"pane_id":pane_id}))
    }

    pub fn read_recent(&self, pane_id: &str) -> Result<Value> {
        self.request(
            "agent.read",
            json!({"target":pane_id,"source":"recent_unwrapped","lines":120}),
        )
    }

    pub fn close_pane(&self, pane_id: &str) -> Result<Value> {
        self.request("pane.close", json!({"pane_id":pane_id}))
    }
}
