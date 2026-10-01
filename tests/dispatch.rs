use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::{unix::fs::PermissionsExt, unix::net::UnixListener},
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use tempfile::TempDir;
use wayfinder_herdr::store::{self, Authorization, Binding, RequestKind, WorkerRun, WorkerStatus};

const MAP: &str = "example/project#42";
const GH_MOCK: &str = r##"#!/usr/bin/env python3
import json, os, sys
args=sys.argv[1:]
path=args[-1]
state_path=os.environ['MOCK_GH_STATE']
with open(state_path) as f: state=json.load(f)
def dump(value): print(json.dumps(value))
def child():
    return {'id':1300,'number':13,'title':'Implement sample task','body':'Implement the requested feature.','state':'open','assignees':([{'login':state['login']}] if state['assigned'] else []),'labels':[{'name':'wayfinder:task'}]}
def map_issue():
    return {'id':4200,'number':42,'title':'Map','body':'## Notes\n\nExecution override: selected by the user for this effort.','state':'open','assignees':[],'labels':[{'name':'wayfinder:map'}]}
if path == 'user': dump({'login':state['login']})
elif '--paginate' in args:
    route=path.split('?')[0]
    if route.endswith('/sub_issues'): page=[child()]
    elif '/dependencies/blocked_by' in route: page=[]
    else: page=[]
    dump([page])
elif '--method' in args:
    method=args[args.index('--method')+1]
    route=args[args.index('--method')+2]
    body=json.loads(sys.stdin.read() or '{}')
    if route.endswith('/assignees'):
        state['assigned']=body['assignees'][0] == state['login']
        with open(state_path,'w') as f: json.dump(state,f)
        dump({'assignees':[{'login':state['login']}]})
    else: dump({})
elif path.endswith('/issues/42'): dump(map_issue())
elif path.endswith('/issues/13'): dump(child())
else: sys.stderr.write('unhandled fake gh route: '+path+'\n'); sys.exit(2)
"##;

#[derive(Default)]
struct HerdrState {
    requests: Vec<Value>,
    agent_status: String,
    ambiguous_open_response: bool,
    fail_open_without_resource: bool,
    opened: bool,
    worktree_path: Option<String>,
}

struct Server {
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct Fixture {
    _temp: TempDir,
    root: PathBuf,
    dir: PathBuf,
    key: String,
    gh: PathBuf,
    gh_state: PathBuf,
    herdr_state: Arc<Mutex<HerdrState>>,
    _server: Option<Server>,
}
impl Fixture {
    fn new() -> Self {
        Self::build(true)
    }
    fn new_without_session() -> Self {
        Self::build(false)
    }
    fn build(with_session: bool) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "--quiet"]);
        git(&repo, &["config", "user.name", "Fixture"]);
        git(&repo, &["config", "user.email", "fixture@example.invalid"]);
        fs::write(repo.join("README.md"), "fixture\n").unwrap();
        git(&repo, &["add", "README.md"]);
        git(&repo, &["commit", "--quiet", "-m", "initial"]);
        let gh = temp.path().join("gh");
        fs::write(&gh, GH_MOCK).unwrap();
        fs::set_permissions(&gh, fs::Permissions::from_mode(0o755)).unwrap();
        let gh_state = temp.path().join("gh-state.json");
        fs::write(
            &gh_state,
            json!({"login":"fixture-user","assigned":false}).to_string(),
        )
        .unwrap();
        let binary = temp.path().join("herdr");
        fs::write(&binary, "#!/bin/sh\nroot=$(dirname \"$0\")\ncase \"$1\" in\n --version) echo 'herdr 0.9.3';;\n status) cat \"$root/server.json\";;\n plugin) cat \"$root/plugins.json\";;\n *) exit 9;;\nesac\n").unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();
        let socket = temp.path().join("owned-session.sock");
        fs::write(temp.path().join("server.json"), json!({"running":true,"version":"0.9.3","protocol":22,"compatible":true,"endpoint_compatible":true,"socket":socket}).to_string()).unwrap();
        fs::write(
            temp.path().join("plugins.json"),
            json!({"result":{"plugins":[{"plugin_id":"wayfinder.herdr","enabled":true}]}})
                .to_string(),
        )
        .unwrap();
        let (state, server) = if with_session {
            let listener = UnixListener::bind(&socket).unwrap();
            listener.set_nonblocking(true).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let state = Arc::new(Mutex::new(HerdrState {
                agent_status: "working".into(),
                ..HerdrState::default()
            }));
            let worker_stop = stop.clone();
            let worker_state = state.clone();
            let thread = thread::spawn(move || {
                while !worker_stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, _)) => handle_request(stream, &worker_state),
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5))
                        }
                        Err(_) => break,
                    }
                }
            });
            (
                state,
                Some(Server {
                    stop,
                    thread: Some(thread),
                }),
            )
        } else {
            (Arc::new(Mutex::new(HerdrState::default())), None)
        };
        let binding = Binding {
            repository: repo.clone(),
            socket,
            herdr_binary: binary,
            herdr_config: None,
        };
        let root = temp.path().join("state");
        let (key, _) = store::attach(&root, MAP, binding.clone(), 1).unwrap();
        let dir = store::map_dir(&root, &key).unwrap();
        Self {
            _temp: temp,
            root,
            dir,
            key,
            gh,
            gh_state,
            herdr_state: state,
            _server: server,
        }
    }
    fn cli(&self) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_wayfinder-herdr"));
        cmd.env("GH_BIN_PATH", &self.gh)
            .env("MOCK_GH_STATE", &self.gh_state)
            .arg("--state-dir")
            .arg(&self.root);
        cmd
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

fn handle_request(mut stream: std::os::unix::net::UnixStream, state: &Arc<Mutex<HerdrState>>) {
    let mut line = String::new();
    if BufReader::new(&stream).read_line(&mut line).is_err() {
        return;
    }
    let request: Value = serde_json::from_str(&line).unwrap();
    let mut state = state.lock().unwrap();
    state.requests.push(request.clone());
    let method = request["method"].as_str().unwrap();
    let mut result = json!({"type":"ok"});
    let mut error = None;
    match method {
        "worktree.open" => {
            if state.fail_open_without_resource {
                error = Some(
                    json!({"code":"untrusted_repository","message":"repository trust approval required"}),
                );
            } else {
                state.opened = true;
                state.worktree_path = request["params"]["path"].as_str().map(str::to_owned);
                result = json!({"type":"worktree_opened","already_open":false,"worktree":{"path":state.worktree_path},"workspace":{"workspace_id":"workspace-owned"},"tab":{"tab_id":"tab-owned"},"root_pane":{"pane_id":"pane-owned"}});
                if state.ambiguous_open_response {
                    error =
                        Some(json!({"code":"response_lost","message":"simulated lost response"}));
                }
            }
        }
        "worktree.list" => {
            let worktrees = if state.opened {
                vec![json!({"path":state.worktree_path,"open_workspace_id":"workspace-owned"})]
            } else {
                vec![]
            };
            result = json!({"type":"worktree_list","worktrees":worktrees,"source":{}});
        }
        "pane.list" => {
            result = json!({"type":"pane_list","panes":[{"pane_id":"pane-owned","tab_id":"tab-owned","cwd":state.worktree_path}]})
        }
        "agent.start" => {
            result = json!({"type":"agent_started","agent":{"pane_id":request["params"]["pane_id"]},"argv":[]})
        }
        "agent.prompt" => result = json!({"type":"agent_prompted"}),
        "agent.get" => {
            result = json!({"type":"agent_info","agent":{"pane_id":request["params"]["target"],"agent_status":state.agent_status}})
        }
        "agent.read" => {
            result =
                json!({"type":"pane_read","read":{"text":"Please ask the human to choose A or B."}})
        }
        "pane.close" => result = json!({"type":"pane_closed"}),
        _ => {
            error = Some(
                json!({"code":"unsupported","message":format!("unsupported test method {method}")}),
            )
        }
    }
    let response = if let Some(error) = error {
        json!({"id":request["id"],"error":error})
    } else {
        json!({"id":request["id"],"result":result})
    };
    let _ = writeln!(stream, "{}", response);
}

fn git(path: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(["-C"])
        .arg(path)
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {}: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}
fn success(output: Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn cli_dispatch_creates_a_detached_ticket_worktree_and_uses_explicit_herdr_ids() {
    let f = Fixture::new();
    success(f.once());
    assert_eq!(f.state().authorization, Authorization::AwaitingStart);
    assert!(
        f.herdr_state.lock().unwrap().requests.is_empty(),
        "first-use Start gate must precede resource creation"
    );
    f.apply(RequestKind::Start);
    let state = f.state();
    let run = state.workers.runs.iter().find(|r| r.ticket == 13).unwrap();
    assert_eq!(run.status, WorkerStatus::Running);
    assert_eq!(run.workspace_id.as_deref(), Some("workspace-owned"));
    assert_eq!(run.tab_id.as_deref(), Some("tab-owned"));
    assert_eq!(run.pane_id.as_deref(), Some("pane-owned"));
    assert_eq!(
        git(&run.worktree, &["rev-parse", "--show-toplevel"]).trim(),
        run.worktree.to_str().unwrap()
    );
    assert!(
        !Command::new("git")
            .args(["-C"])
            .arg(&run.worktree)
            .args(["symbolic-ref", "--quiet", "HEAD"])
            .status()
            .unwrap()
            .success(),
        "ticket worktree must be detached HEAD"
    );
    let records = f.herdr_state.lock().unwrap();
    let open = records
        .requests
        .iter()
        .find(|r| r["method"] == "worktree.open")
        .unwrap();
    assert_eq!(open["params"]["path"], run.worktree.to_str().unwrap());
    assert_eq!(open["params"]["focus"], false);
    assert_eq!(open["params"]["trust_repository"], false);
    let start = records
        .requests
        .iter()
        .find(|r| r["method"] == "agent.start")
        .unwrap();
    assert_eq!(start["params"]["pane_id"], "pane-owned");
    let prompt = records
        .requests
        .iter()
        .find(|r| r["method"] == "agent.prompt")
        .unwrap();
    assert_eq!(prompt["params"]["target"], "pane-owned");
    assert!(
        prompt["params"]["text"]
            .as_str()
            .unwrap()
            .contains(".wayfinder-result.json")
    );
    let prompt = prompt["params"]["text"].as_str().unwrap();
    assert!(prompt.contains("gh issue view 13 --repo example/project --json body,title,comments"));
    assert!(prompt.contains("gh issue view 42 --repo example/project --json body,title,comments"));
    assert!(
        prompt.contains("append-only comments")
            && prompt.contains("body refresh pending for a human")
    );
    assert!(!prompt.contains("wayfinder-herdr/issues/18"));
    assert!(prompt.contains("$HOME/.agents/skills/implement/SKILL.md"));
    assert!(prompt.contains("Do not invent, infer, or answer a human response"));
}

#[test]
fn ambiguous_open_response_is_reconciled_by_checkout_identity_without_repeating_creation() {
    let f = Fixture::new();
    f.herdr_state.lock().unwrap().ambiguous_open_response = true;
    f.apply(RequestKind::Start);
    let state = f.state();
    assert_eq!(state.workers.runs[0].status, WorkerStatus::Running);
    let records = f.herdr_state.lock().unwrap();
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "worktree.open")
            .count(),
        1
    );
    assert!(
        records
            .requests
            .iter()
            .any(|r| r["method"] == "worktree.list")
    );
    assert!(records.requests.iter().any(|r| r["method"] == "pane.list"));
}

#[test]
fn trust_failure_is_visible_and_does_not_change_repository_trust_or_retry_open() {
    let f = Fixture::new();
    f.herdr_state.lock().unwrap().fail_open_without_resource = true;
    f.apply(RequestKind::Start);
    let state = f.state();
    assert_eq!(state.workers.runs[0].status, WorkerStatus::Failed);
    assert!(
        state.workers.runs[0]
            .question
            .as_deref()
            .unwrap()
            .contains("repository trust approval required")
    );
    let records = f.herdr_state.lock().unwrap();
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "worktree.open")
            .count(),
        1
    );
}

#[test]
fn unavailable_worker_status_reserves_capacity_after_restart_without_duplicate_launch() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    f.herdr_state.lock().unwrap().agent_status = "working".into();
    success(f.once());
    let state = f.state();
    assert_eq!(state.workers.runs[0].status, WorkerStatus::Running);
    let records = f.herdr_state.lock().unwrap();
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "worktree.open")
            .count(),
        1
    );
    assert_eq!(
        state
            .workers
            .runs
            .iter()
            .filter(|r| r.status.reserves_capacity())
            .count(),
        1
    );
}

#[test]
fn early_idle_snapshot_after_prompt_submission_is_retained_until_later_activity() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    let first = f.state().workers.runs[0].clone();
    assert_eq!(first.status, WorkerStatus::Running);
    assert!(first.last_activity_ms.is_some());

    // Simulate the launch hook racing the agent's visible startup. It must neither
    // settle the run nor trigger a second checkout/prompt while the agent is idle.
    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    success(f.once());
    assert_eq!(f.state().workers.runs[0].status, WorkerStatus::Running);

    // Activity arriving on a later hook is accepted without any human decision.
    f.herdr_state.lock().unwrap().agent_status = "working".into();
    success(f.once());
    let state = f.state();
    assert_eq!(state.workers.runs[0].status, WorkerStatus::Running);
    let requests = &f.herdr_state.lock().unwrap().requests;
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "agent.prompt")
            .count(),
        1
    );
    let prompt = requests
        .iter()
        .find(|r| r["method"] == "agent.prompt")
        .unwrap();
    assert_eq!(prompt["params"]["wait"]["timeout_ms"], 5000);
    assert_eq!(
        prompt["params"]["wait"]["until"],
        json!(["working", "blocked"])
    );
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "worktree.open")
            .count(),
        1
    );
}

#[test]
fn bounded_idle_without_result_becomes_uncertain_without_relaunch() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    let mut state = f.state();
    state.workers.runs[0].last_activity_ms = Some(0);
    store::atomic_json(&f.dir.join("state.json"), &state).unwrap();

    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    success(f.once());
    let state = f.state();
    assert_eq!(state.workers.runs[0].status, WorkerStatus::Uncertain);
    assert!(state.workers.runs[0].status.reserves_capacity());
    assert!(
        state.workers.runs[0]
            .question
            .as_deref()
            .unwrap()
            .contains("bounded submission grace")
    );
    let requests = &f.herdr_state.lock().unwrap().requests;
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "agent.prompt")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "worktree.open")
            .count(),
        1
    );
}

#[test]
fn human_stop_persists_before_closing_only_the_owned_pane_and_retains_checkout() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    let run = f.state().workers.runs[0].clone();
    let stopped = f
        .cli()
        .args(["stop-worker", "--map", MAP, "--run", &run.id])
        .output()
        .unwrap();
    success(stopped);
    assert_eq!(f.state().workers.runs[0].status, WorkerStatus::Stopped);
    success(f.once());
    assert_eq!(f.state().workers.runs[0].status, WorkerStatus::Stopped);
    assert!(run.worktree.exists());
    let records = f.herdr_state.lock().unwrap();
    let close = records
        .requests
        .iter()
        .find(|r| r["method"] == "pane.close")
        .unwrap();
    assert_eq!(close["params"]["pane_id"], "pane-owned");
}

#[test]
fn role_configuration_persists_provider_model_effort_and_shared_capacity() {
    let f = Fixture::new_without_session();
    let output = f
        .cli()
        .args([
            "configure-worker",
            "--map",
            MAP,
            "--role",
            "reviewer",
            "--kind",
            "codex",
            "--model",
            "gpt-test",
            "--reasoning-effort",
            "high",
            "--arg=--full-auto",
            "--concurrency",
            "2",
        ])
        .output()
        .unwrap();
    success(output);
    let state = f.state();
    assert_eq!(state.concurrency, 2);
    let role = state.workers.providers.roles.get("reviewer").unwrap();
    assert_eq!(role.model.as_deref(), Some("gpt-test"));
    assert_eq!(role.reasoning_effort.as_deref(), Some("high"));
    assert_eq!(role.args, vec!["--full-auto"]);
}

#[test]
fn first_use_start_gate_leaves_workers_and_herdr_resources_untouched() {
    let f = Fixture::new_without_session();
    success(f.once());
    let state = f.state();
    assert_eq!(state.authorization, Authorization::AwaitingStart);
    assert!(state.workers.runs.is_empty());
    assert!(f.herdr_state.lock().unwrap().requests.is_empty());
}

fn insert_uncertain_run(f: &Fixture) -> WorkerRun {
    let mut state = f.state();
    let run = WorkerRun {
        id: "run-00000000000000000001".into(),
        ticket: 13,
        role: "implementer".into(),
        attempt: 1,
        automatic_retries: 0,
        rework_round: 0,
        status: WorkerStatus::Uncertain,
        worktree: f._temp.path().join("retained-worktree"),
        workspace_id: Some("owned-workspace".into()),
        tab_id: Some("owned-tab".into()),
        pane_id: Some("owned-pane".into()),
        base_commit: None,
        result_commit: None,
        summary: None,
        question: Some("launch outcome unknown".into()),
        human_response: None,
        human_decision: None,
        source_run: None,
        claim_login: Some("fixture-user".into()),
        context: None,
        last_activity_ms: None,
    };
    state.workers.next_run = 1;
    state.workers.runs.push(run.clone());
    store::atomic_json(&f.dir.join("state.json"), &state).unwrap();
    run
}

#[test]
fn uncertain_retry_requires_actual_human_absence_confirmation_and_keeps_old_artifacts() {
    let f = Fixture::new_without_session();
    let old = insert_uncertain_run(&f);
    let denied = f
        .cli()
        .args(["retry-worker", "--map", MAP, "--run", &old.id])
        .output()
        .unwrap();
    assert!(!denied.status.success());
    assert_eq!(f.state().workers.runs[0].status, WorkerStatus::Uncertain);
    let accepted = f
        .cli()
        .args([
            "retry-worker",
            "--map",
            MAP,
            "--run",
            &old.id,
            "--confirmed-absent-or-stopped",
        ])
        .output()
        .unwrap();
    success(accepted);
    let state = f.state();
    assert_eq!(state.workers.runs[0].status, WorkerStatus::Stopped);
    assert!(
        state.workers.runs[0]
            .human_decision
            .as_deref()
            .unwrap()
            .contains("human confirmed")
    );
    assert_eq!(state.workers.runs[1].status, WorkerStatus::Queued);
    assert_eq!(
        state.workers.runs[1].source_run.as_deref(),
        Some(old.id.as_str())
    );
    assert_eq!(state.workers.runs[0].worktree, old.worktree);
}

#[test]
fn abandoning_uncertain_work_records_decision_without_releasing_capacity_or_deleting_artifacts() {
    let f = Fixture::new_without_session();
    let old = insert_uncertain_run(&f);
    let output = f
        .cli()
        .args(["abandon-worker", "--map", MAP, "--run", &old.id])
        .output()
        .unwrap();
    success(output);
    let state = f.state();
    assert_eq!(state.workers.runs[0].status, WorkerStatus::Uncertain);
    assert!(state.workers.runs[0].status.reserves_capacity());
    assert!(
        state.workers.runs[0]
            .human_decision
            .as_deref()
            .unwrap()
            .contains("abandon")
    );
    assert!(!old.worktree.exists());
}
