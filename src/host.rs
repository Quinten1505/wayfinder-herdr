use crate::store::Binding;
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

/// Bounded child execution, with disk-backed output to avoid pipe deadlocks.
fn output(binding: &Binding, args: &[&str]) -> Result<String> {
    let mut stdout = tempfile::tempfile()?;
    let stderr = tempfile::tempfile()?;
    let mut command = Command::new(&binding.herdr_binary);
    command
        .args(args)
        .env("HERDR_SOCKET_PATH", &binding.socket)
        .env_remove("HERDR_PANE_ID")
        .env_remove("HERDR_TAB_ID")
        .env_remove("HERDR_WORKSPACE_ID")
        .stdin(Stdio::null())
        .stdout(stdout.try_clone()?)
        .stderr(stderr);
    if let Some(config) = &binding.herdr_config {
        command.env("HERDR_CONFIG_PATH", config);
    }
    let mut child = command.spawn().context("launch bound herdr executable")?;
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            anyhow::bail!("herdr command timed out; dispatch suspended");
        }
        thread::sleep(Duration::from_millis(20));
    };
    ensure!(
        status.success(),
        "herdr command failed ({status}); dispatch suspended"
    );
    let mut result = String::new();
    stdout.seek(SeekFrom::Start(0))?;
    File::take(stdout, 1024 * 1024).read_to_string(&mut result)?;
    Ok(result)
}

/// Rechecked each reconciliation: never cache an enabled plugin or reachable server.
pub fn check(binding: &Binding) -> Result<()> {
    let cli = output(binding, &["--version"])?;
    ensure!(
        cli.trim() == "herdr 0.9.3",
        "unsupported herdr CLI {}; install tested herdr 0.9.3 before dispatch",
        cli.trim()
    );
    let server: Value = serde_json::from_str(&output(binding, &["status", "server", "--json"])?)?;
    validate_server(&server, binding)?;
    let plugins: Value = serde_json::from_str(&output(
        binding,
        &["plugin", "list", "--plugin", "wayfinder.herdr", "--json"],
    )?)?;
    validate_plugin(&plugins)
}

pub fn validate_server(server: &Value, binding: &Binding) -> Result<()> {
    ensure!(
        server["running"] == true,
        "herdr unavailable; dispatch suspended"
    );
    ensure!(
        server["version"] == "0.9.3" && server["protocol"] == 22,
        "unsupported herdr server; use tested 0.9.3/protocol 22; do not restart active sessions automatically"
    );
    ensure!(
        server["compatible"] == true && server["endpoint_compatible"] == true,
        "herdr endpoint incompatible; dispatch suspended"
    );
    ensure!(
        server["socket"].as_str() == binding.socket.to_str(),
        "herdr returned another socket; dispatch suspended"
    );
    Ok(())
}

pub fn validate_plugin(plugins: &Value) -> Result<()> {
    let entries = plugins["result"]["plugins"]
        .as_array()
        .context("unrecognized herdr plugin list response; dispatch suspended")?;
    ensure!(
        entries
            .iter()
            .any(|p| p["plugin_id"] == "wayfinder.herdr" && p["enabled"] == true),
        "Wayfinder plugin disabled or absent; dispatch suspended"
    );
    Ok(())
}
