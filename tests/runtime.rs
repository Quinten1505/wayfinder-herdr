use serde_json::json;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use wayfinder_herdr::{
    host,
    store::{self, Authorization, Binding, RequestKind},
};
const MAP: &str = "Example/Project#42";

struct Fixture {
    _temp: TempDir,
    root: PathBuf,
    dir: PathBuf,
    key: String,
    binding: Binding,
}
impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("state");
        let binary = temp.path().join("herdr");
        fs::write(&binary, "#!/bin/sh\nroot=$(dirname \"$0\")\ncase \"$1\" in\n --version) cat \"$root/version\";;\n status) cat \"$root/server.json\";;\n plugin) cat \"$root/plugins.json\";;\n *) exit 9;;\nesac\n").unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();
        fs::write(temp.path().join("version"), "herdr 0.9.3\n").unwrap();
        let socket = temp.path().join("isolated.sock");
        fs::write(temp.path().join("server.json"), json!({"running":true,"version":"0.9.3","protocol":22,"compatible":true,"endpoint_compatible":true,"socket":socket}).to_string()).unwrap();
        fs::write(
            temp.path().join("plugins.json"),
            json!({"result":{"plugins":[{"plugin_id":"wayfinder.herdr","enabled":true}]}})
                .to_string(),
        )
        .unwrap();
        let binding = Binding {
            repository: temp.path().to_owned(),
            socket,
            herdr_binary: binary,
            herdr_config: None,
            source_workspace_id: Some("workspace-parent".into()),
        };
        let (key, _) = store::attach(&root, MAP, binding.clone(), 1).unwrap();
        let dir = store::map_dir(&root, &key).unwrap();
        Self {
            _temp: temp,
            root,
            dir,
            key,
            binding,
        }
    }
    fn cli(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_wayfinder-herdr"));
        command.arg("--state-dir").arg(&self.root);
        command
    }
    fn once(&self) -> Output {
        self.cli()
            .args(["serve", "--key", &self.key, "--once"])
            .output()
            .unwrap()
    }
    fn apply(&self, kind: RequestKind) {
        store::enqueue(&self.dir, kind).unwrap();
        success(self.once());
    }
    fn state(&self) -> store::State {
        store::read_state(&self.dir).unwrap()
    }
}
fn success(output: Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn exclusive_runtime_lock_is_released_on_process_death_and_state_survives() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    f.apply(RequestKind::Pause);
    let mut daemon = Process(
        f.cli()
            .args(["serve", "--key", &f.key])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if store::Lock::acquire(&f.dir.join("runtime.lock")).is_err() {
            break;
        }
        assert!(Instant::now() < deadline, "runtime never acquired lock");
        thread::sleep(Duration::from_millis(10));
    }
    let other = f.once();
    assert!(!other.status.success());
    assert!(String::from_utf8_lossy(&other.stderr).contains("lock busy"));
    daemon.0.kill().unwrap();
    daemon.0.wait().unwrap();
    success(f.once());
    assert_eq!(f.state().authorization, Authorization::Paused);
    assert_eq!(f.state().history.len(), 2);
    assert!(!f.state().reconciled);
}

#[test]
fn queued_start_survives_restart_and_duplicate_request_is_not_reapplied() {
    let f = Fixture::new();
    let id = store::enqueue(&f.dir, RequestKind::Start).unwrap();
    let path = f.dir.join("inbox").join(format!("{id}.json"));
    let request = fs::read(&path).unwrap();
    success(f.once());
    f.apply(RequestKind::Pause);
    // Simulate death after state commit but before request deletion.
    fs::write(&path, request).unwrap();
    success(f.once());
    assert_eq!(f.state().authorization, Authorization::Paused);
    assert_eq!(f.state().history.len(), 2);
    assert!(!path.exists());
}

#[test]
fn first_use_requires_start_and_repeated_start_does_not_undo_pause() {
    let f = Fixture::new();
    success(f.once());
    assert_eq!(f.state().authorization, Authorization::AwaitingStart);
    f.apply(RequestKind::Resume);
    f.apply(RequestKind::Reconcile);
    assert_eq!(f.state().authorization, Authorization::AwaitingStart);
    f.apply(RequestKind::Pause);
    f.apply(RequestKind::Resume);
    assert_eq!(f.state().authorization, Authorization::Paused);
    f.apply(RequestKind::Start);
    assert_eq!(f.state().authorization, Authorization::Started);
    f.apply(RequestKind::Pause);
    f.apply(RequestKind::Start);
    assert_eq!(f.state().authorization, Authorization::Paused);
    f.apply(RequestKind::Resume);
    assert_eq!(f.state().authorization, Authorization::Started);
    assert!(
        !f.state().reconciled,
        "foundation must never permit dispatch"
    );
}

#[test]
fn future_state_and_unknown_fields_are_preserved_byte_for_byte() {
    for field in ["format_version", "unknown_future_field"] {
        let f = Fixture::new();
        let path = f.dir.join("state.json");
        let mut state: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        state[field] = json!(999);
        let bytes = serde_json::to_vec_pretty(&state).unwrap();
        fs::write(&path, &bytes).unwrap();
        let failed = f.once();
        assert!(!failed.status.success());
        assert!(String::from_utf8_lossy(&failed.stderr).contains("compatible"));
        assert!(store::enqueue(&f.dir, RequestKind::Start).is_err());
        assert!(store::attach(&f.root, MAP, f.binding.clone(), 1).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}

#[test]
fn corrupt_state_and_future_requests_stop_without_resetting_history() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    let state_before = fs::read(f.dir.join("state.json")).unwrap();
    let pending = f.dir.join("inbox/request-future.json");
    let bytes = b"{\"format_version\":999,\"id\":\"request-future\",\"command\":\"start\"}";
    fs::write(&pending, bytes).unwrap();
    assert!(!f.once().status.success());
    assert_eq!(fs::read(&pending).unwrap(), bytes);
    assert_eq!(fs::read(f.dir.join("state.json")).unwrap(), state_before);
    fs::write(f.dir.join("state.json"), "{ truncated").unwrap();
    assert!(!f.once().status.success());
    assert_eq!(
        fs::read_to_string(f.dir.join("state.json")).unwrap(),
        "{ truncated"
    );
}

#[test]
fn compatibility_and_plugin_disable_are_rechecked_without_changing_authorization() {
    let f = Fixture::new();
    host::check(&f.binding).unwrap();
    f.apply(RequestKind::Start);
    fs::write(
        f._temp.path().join("plugins.json"),
        json!({"result":{"plugins":[{"plugin_id":"wayfinder.herdr","enabled":false}]}}).to_string(),
    )
    .unwrap();
    success(f.once());
    assert!(f.state().suspension.contains("disabled"));
    assert_eq!(f.state().authorization, Authorization::Started);
    fs::write(f._temp.path().join("version"), "herdr 0.9.4").unwrap();
    success(f.once());
    assert!(f.state().suspension.contains("unsupported herdr CLI"));
    fs::write(f._temp.path().join("version"), "herdr 0.9.3").unwrap();
    fs::write(
        f._temp.path().join("server.json"),
        json!({"running":false}).to_string(),
    )
    .unwrap();
    success(f.once());
    assert!(f.state().suspension.contains("unavailable"));
    assert!(!f.state().reconciled);
}

#[test]
fn installed_schema_shapes_are_validated_and_unknown_host_fields_ignored() {
    let f = Fixture::new();
    let mut server = json!({"running":true,"version":"0.9.3","protocol":22,"compatible":true,"endpoint_compatible":true,"socket":f.binding.socket,"future_field":123});
    host::validate_server(&server, &f.binding).unwrap();
    server["version"] = json!("0.10.0");
    assert!(host::validate_server(&server, &f.binding).is_err());
    server["version"] = json!("0.9.3");
    server["socket"] = json!("/another-session.sock");
    assert!(host::validate_server(&server, &f.binding).is_err());
    assert!(
        host::validate_plugin(
            &json!({"result":{"plugins":[{"id":"wayfinder.herdr","enabled":true}]}})
        )
        .is_err()
    );
}

#[test]
fn attach_is_idempotent_but_cannot_rebind_a_map_or_reset_pause() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    f.apply(RequestKind::Pause);
    let mut legacy = f.state();
    legacy.binding.source_workspace_id = None;
    store::atomic_json(&f.dir.join("state.json"), &legacy).unwrap();
    store::attach(&f.root, "example/project#042", f.binding.clone(), 30).unwrap();
    let repaired = f.state();
    assert_eq!(
        repaired.binding.source_workspace_id.as_deref(),
        Some("workspace-parent")
    );
    assert_eq!(repaired.authorization, Authorization::Paused);
    assert_eq!(repaired.history.len(), legacy.history.len());
    assert_eq!(repaired.history[0].id, legacy.history[0].id);
    let before = fs::read(f.dir.join("state.json")).unwrap();
    let mut changed = f.binding.clone();
    changed.socket = "/different.sock".into();
    assert!(store::attach(&f.root, MAP, changed, 30).is_err());
    assert_eq!(fs::read(f.dir.join("state.json")).unwrap(), before);
}

#[test]
fn hook_without_attached_maps_creates_no_state_and_action_uses_real_context_shape() {
    let empty = tempfile::tempdir().unwrap();
    let root = empty.path().join("no-state");
    success(
        Command::new(env!("CARGO_BIN_EXE_wayfinder-herdr"))
            .arg("--state-dir")
            .arg(&root)
            .arg("reconcile")
            .output()
            .unwrap(),
    );
    assert!(!root.exists());
    let f = Fixture::new();
    success(
        f.cli()
            .args(["action", "start"])
            .env("HERDR_SOCKET_PATH", &f.binding.socket)
            .env(
                "HERDR_PLUGIN_CONTEXT_JSON",
                json!({"workspace_cwd":f.binding.repository}).to_string(),
            )
            .output()
            .unwrap(),
    );
    success(f.once());
    assert_eq!(f.state().authorization, Authorization::Started);
}

#[test]
fn commands_follow_durable_submission_sequence_not_file_timestamps() {
    let f = Fixture::new();
    let start = store::enqueue(&f.dir, RequestKind::Start).unwrap();
    let pause = store::enqueue(&f.dir, RequestKind::Pause).unwrap();
    let later = std::time::SystemTime::now() + Duration::from_secs(60);
    fs::OpenOptions::new()
        .write(true)
        .open(f.dir.join("inbox").join(format!("{start}.json")))
        .unwrap()
        .set_times(fs::FileTimes::new().set_modified(later))
        .unwrap();
    success(f.once());
    let state = f.state();
    assert_eq!(state.authorization, Authorization::Paused);
    assert_eq!(state.history[0].id, start);
    assert_eq!(state.history[1].id, pause);
}
