//! Small, explicitly-targeted client for the installed Herdr socket API.
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Read, Write},
    os::unix::net::UnixStream,
    path::Path,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

static REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct Client {
    socket: std::path::PathBuf,
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
            bail!(
                "herdr {method} failed: {} ({})",
                error["message"],
                error["code"]
            );
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

    pub fn open_worktree(&self, path: &Path, label: &str) -> Result<Value> {
        self.request(
            "worktree.open",
            json!({"cwd":path,"path":path,"label":label,"focus":false,"trust_repository":false}),
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
