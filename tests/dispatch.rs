use serde_json::{Value, json};
use std::{
    collections::HashMap,
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
    ambiguous_start_response: bool,
    ambiguous_close_response: bool,
    reject_next_prompt_as_blocked: bool,
    fail_next_read: bool,
    empty_next_read: bool,
    changed_read: bool,
    opened_worktrees: Vec<OpenedWorktree>,
    agents: HashMap<String, Value>,
    sessions: HashMap<String, Value>,
    closed_panes: Vec<String>,
}

#[derive(Clone)]
struct OpenedWorktree {
    path: String,
    workspace: String,
    tab: String,
    pane: String,
    terminal: String,
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
                let n = state.opened_worktrees.len();
                let tree = OpenedWorktree {
                    path: request["params"]["path"].as_str().unwrap().to_owned(),
                    workspace: if n == 0 {
                        "workspace-owned".into()
                    } else {
                        format!("workspace-owned-{n}")
                    },
                    tab: if n == 0 {
                        "tab-owned".into()
                    } else {
                        format!("tab-owned-{n}")
                    },
                    pane: if n == 0 {
                        "pane-owned".into()
                    } else {
                        format!("pane-owned-{n}")
                    },
                    terminal: if n == 0 {
                        "terminal-owned".into()
                    } else {
                        format!("terminal-owned-{n}")
                    },
                };
                state.opened_worktrees.push(tree.clone());
                result = json!({"type":"worktree_opened","already_open":false,"worktree":{"path":tree.path},"workspace":{"workspace_id":tree.workspace},"tab":{"tab_id":tree.tab},"root_pane":{"pane_id":tree.pane}});
                if state.ambiguous_open_response {
                    error =
                        Some(json!({"code":"response_lost","message":"simulated lost response"}));
                }
            }
        }
        "worktree.list" => {
            let worktrees = state
                .opened_worktrees
                .iter()
                .map(|tree| json!({"path":tree.path,"open_workspace_id":tree.workspace}))
                .collect::<Vec<_>>();
            result = json!({"type":"worktree_list","worktrees":worktrees,"source":{}});
        }
        "pane.list" => {
            let workspace = request["params"]["workspace_id"].as_str().unwrap();
            let panes = state
                .opened_worktrees
                .iter()
                .filter(|tree| tree.workspace == workspace)
                .map(|tree| json!({"pane_id":tree.pane,"tab_id":tree.tab,"cwd":tree.path}))
                .collect::<Vec<_>>();
            result = json!({"type":"pane_list","panes":panes})
        }
        "agent.start" => {
            let pane = request["params"]["pane_id"].as_str().unwrap().to_owned();
            let name = request["params"]["name"].as_str().unwrap().to_owned();
            let provider = request["params"]["kind"].as_str().unwrap().to_owned();
            if let Some(tree) = state
                .opened_worktrees
                .iter()
                .find(|tree| tree.pane == pane)
                .cloned()
            {
                state.agent_status = "idle".into();
                let info = json!({"agent":provider,"agent_session":null,"agent_status":"idle","name":name,"terminal_id":tree.terminal,"workspace_id":tree.workspace,"tab_id":tree.tab,"pane_id":tree.pane});
                state.agents.insert(pane.clone(), info.clone());
                result = json!({"type":"agent_started","agent":info,"argv":[]});
                if state.ambiguous_start_response {
                    error = Some(
                        json!({"code":"response_lost","message":"simulated lost agent.start response"}),
                    );
                }
            } else {
                error = Some(json!({"code":"pane_not_found","message":"unknown pane"}));
            }
        }
        "agent.prompt" => {
            let pane = request["params"]["target"].as_str().unwrap().to_owned();
            if state.agent_status == "blocked" || state.reject_next_prompt_as_blocked {
                state.reject_next_prompt_as_blocked = false;
                state.agent_status = "blocked".into();
                error = Some(
                    json!({"code":"agent_blocked","message":"recognized blocked agent rejects prompt before input"}),
                );
            } else if let Some(info) = state.agents.get(&pane) {
                let mut updated = info.clone();
                updated["agent_status"] = json!("working");
                state.agent_status = "working".into();
                let pane = request["params"]["target"].as_str().unwrap();
                state.sessions.entry(pane.into()).or_insert_with(|| json!({"source":"fixture","agent":"codex","kind":"id","value":format!("conversation-{pane}")}));
                updated["agent_session"] = state.sessions.get(pane).cloned().unwrap();
                state.agents.insert(pane.into(), updated);
                result = json!({"type":"agent_prompted"});
            } else {
                error = Some(json!({"code":"agent_not_found","message":"no agent on pane"}));
            }
        }
        "agent.get" => {
            let pane = request["params"]["target"].as_str().unwrap();
            if let Some(info) = state.agents.get(pane) {
                let mut current = info.clone();
                current["agent_status"] = json!(state.agent_status);
                current["agent_session"] = state.sessions.get(pane).cloned().unwrap_or(Value::Null);
                result = json!({"type":"agent_info","agent":current});
            } else {
                error = Some(json!({"code":"agent_not_found","message":"no agent on pane"}));
            }
        }
        "pane.process_info" => {
            let pane = request["params"]["pane_id"].as_str().unwrap();
            let current_pid = std::process::id();
            if let Some(tree) = state.opened_worktrees.iter().find(|tree| tree.pane == pane) {
                result = json!({
                    "type":"pane_process_info",
                    "process_info":{
                        "pane_id":pane,
                        "foreground_process_group_id":current_pid,
                        "foreground_processes":[{"pid":current_pid,"name":"codex","argv":["codex"],"cwd":tree.path}],
                        "shell_pid":current_pid,
                        "tty":"fixture"
                    }
                });
            } else {
                error =
                    Some(json!({"code":"pane_not_found","message":"unknown process-info pane"}));
            }
        }
        "agent.read" => {
            if state.fail_next_read {
                state.fail_next_read = false;
                error =
                    Some(json!({"code":"read_failed","message":"simulated pane snapshot failure"}));
            } else if state.empty_next_read {
                state.empty_next_read = false;
                result = json!({"type":"pane_read","read":{"text":""}})
            } else {
                let text = if state.changed_read {
                    "Approval required: allow running a shell command?"
                } else {
                    "Question: Which option should I use, A or B?"
                };
                result = json!({"type":"pane_read","read":{"text":text}})
            }
        }
        "pane.close" => {
            let pane = request["params"]["pane_id"].as_str().unwrap().to_owned();
            state.closed_panes.push(pane);
            result = json!({"type":"pane_closed"});
            if state.ambiguous_close_response {
                error =
                    Some(json!({"code":"response_lost","message":"simulated lost close response"}));
            }
        }
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

fn write_worker_result(run: &WorkerRun, result: Value) {
    fs::write(
        run.worktree.join(".wayfinder-result.json"),
        serde_json::to_vec(&result).unwrap(),
    )
    .unwrap();
}

fn commit_worker_change(run: &WorkerRun, name: &str) -> String {
    fs::write(run.worktree.join("implementation.txt"), format!("{name}\n")).unwrap();
    git(&run.worktree, &["add", "implementation.txt"]);
    git(&run.worktree, &["commit", "-m", name]);
    git(&run.worktree, &["rev-parse", "HEAD"]).trim().to_owned()
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
fn recovered_open_intent_starts_the_agent_once_without_reopening_the_worktree() {
    let f = Fixture::new();
    let repository = f.state().binding.repository;
    let checkout = f._temp.path().join("recovered-open-intent");
    git(
        &repository,
        &[
            "worktree",
            "add",
            "--detach",
            checkout.to_str().unwrap(),
            "HEAD",
        ],
    );
    let tree = OpenedWorktree {
        path: checkout.to_string_lossy().into_owned(),
        workspace: "recovered-workspace".into(),
        tab: "recovered-tab".into(),
        pane: "recovered-pane".into(),
        terminal: "recovered-terminal".into(),
    };
    f.herdr_state.lock().unwrap().opened_worktrees.push(tree);
    let mut state = f.state();
    state.authorization = Authorization::Started;
    state.workers.next_run = 1;
    let run = WorkerRun {
        id: "run-00000000000000000001".into(),
        ticket: 13,
        role: "implementer".into(),
        attempt: 1,
        automatic_retries: 0,
        rework_round: 0,
        status: WorkerStatus::OpenIntent,
        worktree: checkout,
        workspace_id: None,
        tab_id: None,
        pane_id: None,
        base_commit: None,
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
        source_run: None,
        claim_login: Some("fixture-user".into()),
        context: None,
        last_activity_ms: None,
        terminal_id: None,
        agent_provider: None,
        agent_session: None,
        foreground_process: None,
        result_evidence: None,
        initial_prompt_pending: false,
    };
    state.workers.runs.push(run.clone());
    store::atomic_json(&f.dir.join("state.json"), &state).unwrap();
    store::enqueue(&f.dir, RequestKind::Reconcile).unwrap();

    success(f.once());
    let resumed = f.state().workers.runs[0].clone();
    assert_eq!(resumed.status, WorkerStatus::Running);
    assert_eq!(resumed.pane_id.as_deref(), Some("recovered-pane"));
    let records = f.herdr_state.lock().unwrap();
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.start")
            .count(),
        1
    );
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.prompt")
            .count(),
        1
    );
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "worktree.open")
            .count(),
        0
    );
}

fn answer_cli(f: &Fixture, run: &WorkerRun, request_type: &str, response: &str) -> Output {
    f.cli()
        .args([
            "answer-worker",
            "--map",
            MAP,
            "--run",
            &run.id,
            "--request-id",
            run.human_request_id.as_deref().unwrap(),
            "--request-type",
            request_type,
            "--response",
            response,
        ])
        .output()
        .unwrap()
}

#[test]
fn blocked_herdr_ui_requires_direct_human_interaction_and_never_sends_raw_input() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    f.herdr_state.lock().unwrap().agent_status = "blocked".into();
    success(f.once());
    let pending = f.state().workers.runs[0].clone();
    assert_eq!(pending.status, WorkerStatus::NeedsHuman);
    assert_eq!(
        pending.human_request_kind,
        Some(store::HumanRequestKind::HerdrBlockedUi)
    );

    let stale = f
        .cli()
        .args([
            "answer-worker",
            "--map",
            MAP,
            "--run",
            &pending.id,
            "--request-id",
            "old-request",
            "--request-type",
            "herdr_blocked_ui",
            "--response",
            "A",
        ])
        .output()
        .unwrap();
    assert!(!stale.status.success());
    let rejected = answer_cli(&f, &pending, "herdr_blocked_ui", "A");
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("interact with it directly"));
    let retained = f.state().workers.runs[0].clone();
    assert_eq!(retained.status, WorkerStatus::NeedsHuman);
    assert!(retained.status.reserves_capacity());
    assert_eq!(retained.human_response.as_deref(), Some("A"));
    assert_eq!(retained.answer_request_id, pending.human_request_id);
    assert_eq!(retained.answer_request_kind, pending.human_request_kind);
    assert_eq!(retained.answer_history.len(), 1);
    assert_eq!(
        retained.answer_history[0].disposition,
        store::AnswerDisposition::ManualRequired
    );
    assert_eq!(retained.human_request_id, pending.human_request_id);
    assert_eq!(retained.question, pending.question);
    success(f.once());
    assert_eq!(f.state().workers.runs[0].status, WorkerStatus::NeedsHuman);
    let records = f.herdr_state.lock().unwrap();
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.prompt")
            .count(),
        1,
        "only the original worker launch prompt was sent"
    );
    assert!(
        !records
            .requests
            .iter()
            .any(|r| r["method"] == "pane.send_input")
    );
    drop(records);
    f.herdr_state.lock().unwrap().agent_status = "working".into();
    success(f.once());
    let resumed = f.state().workers.runs[0].clone();
    assert_eq!(resumed.status, WorkerStatus::Running);
    assert_eq!(resumed.human_response.as_deref(), Some("A"));
    assert_eq!(
        resumed.answer_history[0].disposition,
        store::AnswerDisposition::ManualRequired
    );
}

#[test]
fn initial_agent_prompt_block_is_manual_then_submitted_once_after_ui_clears() {
    let f = Fixture::new();
    f.herdr_state.lock().unwrap().reject_next_prompt_as_blocked = true;
    f.apply(RequestKind::Start);
    let pending = f.state().workers.runs[0].clone();
    assert_eq!(pending.status, WorkerStatus::NeedsHuman);
    assert_eq!(
        pending.human_request_kind,
        Some(store::HumanRequestKind::HerdrBlockedUi)
    );
    assert!(pending.initial_prompt_pending);
    assert_eq!(pending.agent_session, None);
    assert!(
        pending
            .question
            .as_deref()
            .unwrap()
            .contains("Question: Which option")
    );
    assert!(pending.status.reserves_capacity());

    let manual = answer_cli(&f, &pending, "herdr_blocked_ui", "allow this startup UI");
    assert!(!manual.status.success());
    assert!(
        String::from_utf8_lossy(&manual.stderr).contains("interact with it directly"),
        "{}",
        String::from_utf8_lossy(&manual.stderr)
    );
    let retained = f.state().workers.runs[0].clone();
    assert_eq!(retained.status, WorkerStatus::NeedsHuman);
    assert!(retained.initial_prompt_pending);
    assert_eq!(
        retained.human_response.as_deref(),
        Some("allow this startup UI")
    );
    assert_eq!(
        retained.answer_history[0].disposition,
        store::AnswerDisposition::ManualRequired
    );
    assert!(
        !f.herdr_state
            .lock()
            .unwrap()
            .requests
            .iter()
            .any(|request| request["method"] == "pane.send_input")
    );

    success(f.once());
    assert_eq!(f.state().workers.runs[0].status, WorkerStatus::NeedsHuman);
    assert!(f.state().workers.runs[0].initial_prompt_pending);
    assert_eq!(
        f.herdr_state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.prompt")
            .count(),
        1,
        "the rejected prompt is never retried while Herdr still reports blocked"
    );

    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    success(f.once());
    let started = f.state().workers.runs[0].clone();
    assert_eq!(started.status, WorkerStatus::Running);
    assert!(!started.initial_prompt_pending);
    assert!(started.agent_session.is_some());
    assert_eq!(
        f.herdr_state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.prompt")
            .count(),
        2,
        "the task prompt is submitted once after the human resolves startup UI"
    );
}

#[test]
fn blocked_worker_read_failure_or_empty_text_creates_refreshable_manual_request() {
    for empty in [false, true] {
        let f = Fixture::new();
        f.apply(RequestKind::Start);
        {
            let mut herdr = f.herdr_state.lock().unwrap();
            herdr.agent_status = "blocked".into();
            if empty {
                herdr.empty_next_read = true;
            } else {
                herdr.fail_next_read = true;
            }
        }
        success(f.once());
        let fallback = f.state().workers.runs[0].clone();
        assert_eq!(fallback.status, WorkerStatus::NeedsHuman);
        assert!(fallback.status.reserves_capacity());
        assert_eq!(
            fallback.human_request_kind,
            Some(store::HumanRequestKind::HerdrBlockedUi)
        );
        assert!(
            fallback
                .question
                .as_deref()
                .unwrap()
                .contains("named pane pane-owned")
        );
        assert!(fallback.human_request_id.is_some());

        success(f.once());
        let refreshed = f.state().workers.runs[0].clone();
        assert_eq!(refreshed.status, WorkerStatus::NeedsHuman);
        assert_ne!(refreshed.human_request_id, fallback.human_request_id);
        assert_eq!(
            refreshed.question.as_deref(),
            Some("Question: Which option should I use, A or B?")
        );
    }
}

#[test]
fn blocked_ui_changed_after_human_request_is_not_answered() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    f.herdr_state.lock().unwrap().agent_status = "blocked".into();
    success(f.once());
    let pending = f.state().workers.runs[0].clone();
    // The fixture's visible text is changed after correlation. This must be
    // refreshed into a distinct request before any answer can be delivered.
    f.herdr_state.lock().unwrap().changed_read = true;
    let denied = answer_cli(&f, &pending, "herdr_blocked_ui", "approve");
    assert!(!denied.status.success());
    assert!(
        !f.herdr_state
            .lock()
            .unwrap()
            .requests
            .iter()
            .any(|r| r["method"] == "pane.send_input")
    );
    assert_eq!(f.state().workers.runs[0].status, WorkerStatus::NeedsHuman);
    assert_ne!(
        f.state().workers.runs[0].human_request_id,
        pending.human_request_id
    );
    assert_eq!(
        f.state().workers.runs[0].question.as_deref(),
        Some("Approval required: allow running a shell command?")
    );
}

#[test]
fn blocked_ui_answer_is_retained_without_becoming_uncertain() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    f.herdr_state.lock().unwrap().agent_status = "blocked".into();
    success(f.once());
    let pending = f.state().workers.runs[0].clone();
    let failed = answer_cli(&f, &pending, "herdr_blocked_ui", "A");
    assert!(!failed.status.success());
    let state = f.state();
    assert_eq!(state.workers.runs[0].status, WorkerStatus::NeedsHuman);
    assert!(state.workers.runs[0].status.reserves_capacity());
    assert_eq!(state.workers.runs[0].human_response.as_deref(), Some("A"));
    assert_eq!(
        state.workers.runs[0].answer_history[0].disposition,
        store::AnswerDisposition::ManualRequired
    );
    success(f.once());
    let records = f.herdr_state.lock().unwrap();
    assert!(
        !records
            .requests
            .iter()
            .any(|r| r["method"] == "pane.send_input")
    );
}

#[test]
fn manual_answer_keeps_the_same_pending_request_identity() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    f.herdr_state.lock().unwrap().agent_status = "blocked".into();
    success(f.once());
    let first = f.state().workers.runs[0].clone();
    assert!(
        !answer_cli(&f, &first, "herdr_blocked_ui", "A")
            .status
            .success()
    );
    f.herdr_state.lock().unwrap().agent_status = "blocked".into();
    success(f.once());
    let second = f.state().workers.runs[0].clone();
    assert_eq!(second.question, first.question);
    assert_eq!(second.human_request_id, first.human_request_id);
    let stale = answer_cli(&f, &first, "herdr_blocked_ui", "A");
    assert!(!stale.status.success());
    assert_eq!(f.state().workers.runs[0].answer_history.len(), 2);
    assert!(
        !f.herdr_state
            .lock()
            .unwrap()
            .requests
            .iter()
            .any(|r| r["method"] == "pane.send_input")
    );
}

#[test]
fn agent_prompt_blocked_pre_effect_creates_fresh_request_without_uncertainty() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    let original = f.state().workers.runs[0].clone();
    write_worker_result(
        &original,
        json!({
            "format_version":1,
            "run_id":original.id,
            "ticket":original.ticket,
            "role":original.role,
            "status":"blocked",
            "summary":"needs a human decision",
            "question":"Which option should I use, A or B?"
        }),
    );
    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    success(f.once());
    let pending = f.state().workers.runs[0].clone();
    assert_eq!(
        pending.human_request_kind,
        Some(store::HumanRequestKind::WorkerQuestion)
    );
    f.herdr_state.lock().unwrap().reject_next_prompt_as_blocked = true;
    let rejected = answer_cli(&f, &pending, "worker_question", "A");
    assert!(!rejected.status.success());
    let refreshed = f.state().workers.runs[0].clone();
    assert_eq!(refreshed.status, WorkerStatus::NeedsHuman);
    assert_eq!(
        refreshed.human_request_kind,
        Some(store::HumanRequestKind::HerdrBlockedUi)
    );
    assert_ne!(refreshed.human_request_id, pending.human_request_id);
    assert_eq!(refreshed.human_response.as_deref(), Some("A"));
    assert_eq!(
        refreshed.answer_history[0].disposition,
        store::AnswerDisposition::RejectedBeforeEffect
    );
    let manual = answer_cli(&f, &refreshed, "herdr_blocked_ui", "B");
    assert!(!manual.status.success());
    let retained = f.state().workers.runs[0].clone();
    assert_eq!(retained.status, WorkerStatus::NeedsHuman);
    assert_eq!(retained.answer_history.len(), 2);
    assert_eq!(retained.answer_history[0].response, "A");
    assert_eq!(retained.answer_history[1].response, "B");
    assert_eq!(
        retained.answer_history[1].disposition,
        store::AnswerDisposition::ManualRequired
    );
    assert!(
        !f.herdr_state
            .lock()
            .unwrap()
            .requests
            .iter()
            .any(|r| r["method"] == "pane.send_input")
    );
}

#[test]
fn recorded_worker_question_uses_correlated_agent_prompt() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    let original = f.state().workers.runs[0].clone();
    write_worker_result(
        &original,
        json!({
            "format_version":1,
            "run_id":original.id,
            "ticket":original.ticket,
            "role":original.role,
            "status":"blocked",
            "summary":"needs a human decision",
            "question":"Which option should I use, A or B?"
        }),
    );
    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    success(f.once());
    let pending = f.state().workers.runs[0].clone();
    assert_eq!(
        pending.human_request_kind,
        Some(store::HumanRequestKind::WorkerQuestion)
    );
    success(answer_cli(
        &f,
        &pending,
        "worker_question",
        "Choose option A.",
    ));
    let answered = f.state().workers.runs[0].clone();
    assert_eq!(answered.status, WorkerStatus::Running);
    assert_eq!(
        answered.answer_history[0].disposition,
        store::AnswerDisposition::Submitted
    );
    let records = f.herdr_state.lock().unwrap();
    let prompts = records
        .requests
        .iter()
        .filter(|r| r["method"] == "agent.prompt")
        .collect::<Vec<_>>();
    assert_eq!(prompts.len(), 2);
    assert_eq!(prompts[1]["params"]["text"], "Choose option A.");
    assert!(
        !records
            .requests
            .iter()
            .any(|r| r["method"] == "pane.send_input")
    );
}

#[test]
fn blocked_pre_effect_read_failure_persists_manual_request_across_runtime_restart() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    let original = f.state().workers.runs[0].clone();
    write_worker_result(
        &original,
        json!({
            "format_version":1,
            "run_id":original.id,
            "ticket":original.ticket,
            "role":original.role,
            "status":"blocked",
            "summary":"needs a human decision",
            "question":"Which option should I use, A or B?"
        }),
    );
    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    success(f.once());
    let pending = f.state().workers.runs[0].clone();
    f.herdr_state.lock().unwrap().reject_next_prompt_as_blocked = true;
    f.herdr_state.lock().unwrap().fail_next_read = true;
    let rejected = answer_cli(&f, &pending, "worker_question", "Choose option A.");
    assert!(!rejected.status.success());

    let fallback = f.state().workers.runs[0].clone();
    assert_eq!(fallback.status, WorkerStatus::NeedsHuman);
    assert!(fallback.status.reserves_capacity());
    assert_eq!(
        fallback.human_request_kind,
        Some(store::HumanRequestKind::HerdrBlockedUi)
    );
    assert_ne!(fallback.human_request_id, pending.human_request_id);
    assert!(
        fallback
            .question
            .as_deref()
            .unwrap()
            .contains("Inspect the named pane pane-owned directly")
    );
    assert_eq!(fallback.human_response.as_deref(), Some("Choose option A."));
    assert_eq!(fallback.answer_history.len(), 1);
    assert_eq!(
        fallback.answer_history[0].request_id,
        pending.human_request_id.unwrap()
    );
    assert_eq!(
        fallback.answer_history[0].disposition,
        store::AnswerDisposition::RejectedBeforeEffect
    );

    // A later daemon/runtime process can recover the actual UI snapshot and
    // refresh the manual request without replaying the rejected response.
    success(f.once());
    let recovered = f.state().workers.runs[0].clone();
    assert_eq!(recovered.status, WorkerStatus::NeedsHuman);
    assert!(recovered.status.reserves_capacity());
    assert_eq!(
        recovered.human_request_kind,
        Some(store::HumanRequestKind::HerdrBlockedUi)
    );
    assert_ne!(recovered.human_request_id, fallback.human_request_id);
    assert_eq!(
        recovered.question.as_deref(),
        Some("Question: Which option should I use, A or B?")
    );
    assert_eq!(
        recovered.human_response.as_deref(),
        Some("Choose option A.")
    );
    assert_eq!(recovered.answer_history.len(), 1);
    let records = f.herdr_state.lock().unwrap();
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.prompt")
            .count(),
        2
    );
    assert!(
        !records
            .requests
            .iter()
            .any(|r| r["method"] == "pane.send_input")
    );
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
    assert_eq!(state.workers.runs[0].status, WorkerStatus::NeedsHuman);
    assert!(
        state.workers.runs[0]
            .question
            .as_deref()
            .unwrap()
            .contains("repository trust approval required")
    );
    assert!(
        state.workers.runs[0]
            .question
            .as_deref()
            .unwrap()
            .contains("retry-worker --confirmed-absent-or-stopped")
    );
    let repository = state.binding.repository.clone();
    let worktree = state.workers.runs[0].worktree.clone();
    for _ in 0..3 {
        success(f.once());
    }
    let repeated = f.state();
    assert_eq!(repeated.workers.runs.len(), 1);
    assert_eq!(repeated.workers.runs[0].status, WorkerStatus::NeedsHuman);
    let worktree_list = git(&repository, &["worktree", "list", "--porcelain"]);
    assert_eq!(worktree_list.matches(worktree.to_str().unwrap()).count(), 1);
    let records = f.herdr_state.lock().unwrap();
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "worktree.open")
            .count(),
        1
    );
    assert_eq!(records.opened_worktrees.len(), 0);
    assert!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "worktree.open")
            .all(|r| r["params"]["trust_repository"] != true)
    );
}

#[test]
fn refused_git_detached_checkout_is_held_for_explicit_human_retry() {
    let f = Fixture::new();
    let repository = f.state().binding.repository;
    fs::remove_dir_all(repository.join(".git")).unwrap();
    success(f.once());
    f.apply(RequestKind::Start);
    for _ in 0..3 {
        success(f.once());
    }
    let state = f.state();
    assert_eq!(state.workers.runs.len(), 1);
    assert_eq!(state.workers.runs[0].status, WorkerStatus::NeedsHuman);
    assert!(state.workers.runs[0].status.reserves_capacity());
    assert!(
        state.workers.runs[0]
            .question
            .as_deref()
            .unwrap()
            .contains("retry-worker --confirmed-absent-or-stopped")
    );
    assert!(!state.workers.runs[0].worktree.exists());
    assert!(
        f.herdr_state
            .lock()
            .unwrap()
            .requests
            .iter()
            .all(|r| r["method"] != "worktree.open")
    );
}

#[test]
fn ambiguous_agent_start_is_not_repeated_after_effect_then_lost_response() {
    let f = Fixture::new();
    f.herdr_state.lock().unwrap().ambiguous_start_response = true;
    f.apply(RequestKind::Start);
    let run = f.state().workers.runs[0].clone();
    assert_eq!(run.status, WorkerStatus::Uncertain);
    assert!(run.status.reserves_capacity());
    assert_eq!(
        f.herdr_state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|request| request["method"] == "agent.start")
            .count(),
        1
    );
    success(f.once());
    let records = f.herdr_state.lock().unwrap();
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|request| request["method"] == "agent.start")
            .count(),
        1
    );
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|request| request["method"] == "agent.prompt")
            .count(),
        0
    );
}

#[test]
fn runtime_restart_reconnects_only_the_same_observed_agent_session() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    let run = f.state().workers.runs[0].clone();
    assert_eq!(run.terminal_id.as_deref(), Some("terminal-owned"));
    assert_eq!(
        run.agent_session.as_ref().unwrap().value,
        "conversation-pane-owned"
    );

    // Each `once` is a fresh CLI/runtime process. Exact pane, terminal, provider,
    // and agent-session identity proves it is still this worker.
    success(f.once());
    assert_eq!(f.state().workers.runs[0].status, WorkerStatus::Running);
    let requests = &f.herdr_state.lock().unwrap().requests;
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "agent.start")
            .count(),
        1
    );
}

#[test]
fn cold_restored_or_replaced_agent_identity_becomes_uncertain_without_effects() {
    let scenarios = [
        (
            Some(
                json!({"source":"fixture","agent":"codex","kind":"id","value":"different-conversation"}),
            ),
            false,
            false,
            false,
            false,
        ),
        (Some(Value::Null), false, false, false, false),
        (None, false, true, false, false),
        (Some(Value::Null), true, false, false, false),
        (None, false, false, true, false),
        (None, false, false, false, true),
    ];
    for (replacement, clear_saved_identity, replace_terminal, cold_host_restart, replace_process) in
        scenarios
    {
        let f = Fixture::new();
        f.apply(RequestKind::Start);
        let mut queued = f.state();
        queued.concurrency = 1;
        if clear_saved_identity {
            queued.workers.runs[0].terminal_id = None;
            queued.workers.runs[0].agent_provider = None;
            queued.workers.runs[0].agent_session = None;
        }
        if cold_host_restart {
            queued.workers.runs[0]
                .foreground_process
                .as_mut()
                .unwrap()
                .boot_id = "previous-linux-boot-id".into();
        }
        if replace_process {
            let process = queued.workers.runs[0].foreground_process.as_mut().unwrap();
            process.pid = process.pid.saturating_add(1);
            process.start_time_ticks = process.start_time_ticks.saturating_add(1);
        }
        queued.workers.runs.push(WorkerRun {
            id: "run-00000000000000000099".into(),
            ticket: 14,
            role: "implementer".into(),
            attempt: 1,
            automatic_retries: 0,
            rework_round: 0,
            status: WorkerStatus::Queued,
            worktree: f._temp.path().join("queued-worktree"),
            workspace_id: None,
            tab_id: None,
            pane_id: None,
            base_commit: None,
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
            source_run: None,
            claim_login: None,
            context: None,
            last_activity_ms: None,
            terminal_id: None,
            agent_provider: None,
            agent_session: None,
            foreground_process: None,
            result_evidence: None,
            initial_prompt_pending: false,
        });
        store::atomic_json(&f.dir.join("state.json"), &queued).unwrap();
        {
            let mut herdr = f.herdr_state.lock().unwrap();
            if let Some(replacement) = replacement {
                herdr.sessions.insert("pane-owned".into(), replacement);
            }
            if replace_terminal {
                herdr.agents.get_mut("pane-owned").unwrap()["terminal_id"] =
                    json!("restored-terminal");
            }
        }

        let run_id = queued.workers.runs[0].id.clone();
        let denied = f
            .cli()
            .args(["stop-worker", "--map", MAP, "--run", &run_id])
            .output()
            .unwrap();
        assert!(!denied.status.success());
        assert_eq!(f.state().workers.runs[0].status, WorkerStatus::Uncertain);
        success(f.once());
        let state = f.state();
        assert_eq!(state.workers.runs[0].status, WorkerStatus::Uncertain);
        assert!(state.workers.runs[0].status.reserves_capacity());
        assert_eq!(state.workers.runs[1].status, WorkerStatus::Queued);
        let records = f.herdr_state.lock().unwrap();
        assert_eq!(records.closed_panes.len(), 0);
        assert_eq!(
            records
                .requests
                .iter()
                .filter(|r| r["method"] == "agent.start")
                .count(),
            1
        );
        assert_eq!(
            records
                .requests
                .iter()
                .filter(|r| r["method"] == "worktree.open")
                .count(),
            1
        );
    }
}

#[test]
fn ambiguous_stop_after_effect_is_not_repeated_or_applied_to_replacement() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    let run = f.state().workers.runs[0].clone();
    f.herdr_state.lock().unwrap().ambiguous_close_response = true;
    let stopped = f
        .cli()
        .args(["stop-worker", "--map", MAP, "--run", &run.id])
        .output()
        .unwrap();
    assert!(!stopped.status.success());
    assert_eq!(f.state().workers.runs[0].status, WorkerStatus::Uncertain);
    success(f.once());
    let records = f.herdr_state.lock().unwrap();
    assert_eq!(records.closed_panes, ["pane-owned"]);
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "pane.close")
            .count(),
        1
    );
}

#[test]
fn confirmed_worker_failures_create_exactly_two_automatic_retries() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    for failure_number in 0..=2 {
        let run = f
            .state()
            .workers
            .runs
            .iter()
            .filter(|run| run.role == "implementer")
            .max_by_key(|run| run.attempt)
            .unwrap()
            .clone();
        assert_eq!(run.status, WorkerStatus::Running);
        let result = json!({
            "format_version":1,
            "run_id":run.id,
            "ticket":run.ticket,
            "role":run.role,
            "status":"failed",
            "summary":format!("confirmed failure {failure_number}")
        });
        fs::write(
            run.worktree.join(".wayfinder-result.json"),
            serde_json::to_vec(&result).unwrap(),
        )
        .unwrap();
        f.herdr_state.lock().unwrap().agent_status = "idle".into();
        success(f.once());
        let state = f.state();
        let current = state
            .workers
            .runs
            .iter()
            .find(|candidate| candidate.id == run.id)
            .unwrap();
        assert_eq!(current.status, WorkerStatus::Failed);
        assert!(current.result_evidence.as_ref().unwrap().exists());
        if failure_number < 2 {
            let retry = state
                .workers
                .runs
                .iter()
                .find(|candidate| candidate.attempt == run.attempt + 1)
                .unwrap();
            assert_eq!(retry.automatic_retries, failure_number + 1);
            assert_eq!(retry.status, WorkerStatus::Running);
            f.herdr_state.lock().unwrap().agent_status = "working".into();
        }
    }
    let state = f.state();
    assert_eq!(state.workers.runs.len(), 3);
    assert_eq!(
        state
            .workers
            .runs
            .iter()
            .map(|run| run.automatic_retries)
            .max(),
        Some(2)
    );
    assert!(state.workers.runs.iter().all(|run| run.rework_round == 0));
    assert!(
        state.workers.runs[2]
            .question
            .as_deref()
            .unwrap()
            .contains("Two automatic retries were exhausted")
    );
}

#[test]
fn review_rework_budget_advances_independently_of_failure_retry_budget() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    for expected_round in 1..=2 {
        let implementer = f
            .state()
            .workers
            .runs
            .iter()
            .filter(|run| run.role == "implementer")
            .max_by_key(|run| run.attempt)
            .unwrap()
            .clone();
        let commit = commit_worker_change(&implementer, &format!("iteration-{expected_round}"));
        write_worker_result(
            &implementer,
            json!({
                "format_version":1,
                "run_id":implementer.id,
                "ticket":implementer.ticket,
                "role":implementer.role,
                "status":"completed",
                "summary":"implementation ready for review",
                "commit":commit
            }),
        );
        f.herdr_state.lock().unwrap().agent_status = "idle".into();
        success(f.once());

        let reviewer = f
            .state()
            .workers
            .runs
            .iter()
            .find(|run| run.role == "reviewer" && run.status == WorkerStatus::Running)
            .unwrap()
            .clone();
        write_worker_result(
            &reviewer,
            json!({
                "format_version":1,
                "run_id":reviewer.id,
                "ticket":reviewer.ticket,
                "role":reviewer.role,
                "status":"completed",
                "summary":"changes are required",
                "reviewed_commit":commit,
                "verdict":"changes_requested"
            }),
        );
        f.herdr_state.lock().unwrap().agent_status = "idle".into();
        success(f.once());
        let state = f.state();
        let rework = state
            .workers
            .runs
            .iter()
            .filter(|run| run.role == "implementer")
            .max_by_key(|run| run.attempt)
            .unwrap();
        assert_eq!(rework.status, WorkerStatus::Running);
        assert_eq!(rework.rework_round, expected_round);
        assert_eq!(rework.automatic_retries, 0);
        f.herdr_state.lock().unwrap().agent_status = "working".into();
    }
}

#[test]
fn archived_result_allows_replay_but_unrelated_review_changes_still_block_acceptance() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    let implementer = f.state().workers.runs[0].clone();
    fs::write(
        implementer.worktree.join("implementation.txt"),
        "committed output\n",
    )
    .unwrap();
    git(&implementer.worktree, &["add", "implementation.txt"]);
    git(&implementer.worktree, &["commit", "-m", "implementation"]);
    let commit = git(&implementer.worktree, &["rev-parse", "HEAD"])
        .trim()
        .to_owned();
    let result_path = implementer.worktree.join(".wayfinder-result.json");
    fs::write(
        &result_path,
        serde_json::to_vec(&json!({
            "format_version":1,
            "run_id":implementer.id,
            "ticket":implementer.ticket,
            "role":implementer.role,
            "status":"completed",
            "summary":"implemented the feature",
            "commit":commit
        }))
        .unwrap(),
    )
    .unwrap();
    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    success(f.once());
    assert!(
        !result_path.exists(),
        "the checkout protocol artifact was archived and removed"
    );
    let after_implementation = f.state();
    assert_eq!(
        after_implementation.workers.runs[0].status,
        WorkerStatus::Completed
    );
    let implementation_evidence = after_implementation.workers.runs[0]
        .result_evidence
        .as_ref()
        .unwrap();
    assert!(implementation_evidence.exists());
    assert!(
        git(&implementer.worktree, &["status", "--porcelain"])
            .trim()
            .is_empty()
    );
    let reviewer = after_implementation.workers.runs[1].clone();
    assert_eq!(reviewer.role, "reviewer");
    assert_eq!(reviewer.status, WorkerStatus::Running);
    assert_eq!(reviewer.base_commit.as_deref(), Some(commit.as_str()));

    let reviewer_result = reviewer.worktree.join(".wayfinder-result.json");
    fs::write(
        &reviewer_result,
        serde_json::to_vec(&json!({
            "format_version":1,
            "run_id":reviewer.id,
            "ticket":reviewer.ticket,
            "role":reviewer.role,
            "status":"completed",
            "summary":"reviewed the fixed implementation",
            "reviewed_commit":commit,
            "verdict":"approved"
        }))
        .unwrap(),
    )
    .unwrap();
    fs::write(reviewer.worktree.join("unrelated.txt"), "human edit\n").unwrap();
    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    success(f.once());
    let held = f.state();
    assert_eq!(held.workers.runs[1].status, WorkerStatus::Running);
    let reviewer_evidence = held.workers.runs[1].result_evidence.as_ref().unwrap();
    assert!(reviewer_evidence.exists());
    assert!(!reviewer_result.exists());
    assert!(git(&reviewer.worktree, &["status", "--porcelain"]).contains("unrelated.txt"));

    // A process restart replays from the immutable archive after cleanup of only
    // the owned protocol file; the independent dirty file remains the blocker.
    fs::remove_file(reviewer.worktree.join("unrelated.txt")).unwrap();
    success(f.once());
    let accepted = f.state();
    assert_eq!(accepted.workers.runs[1].status, WorkerStatus::Completed);
    assert_eq!(accepted.workers.runs[0].status, WorkerStatus::Reviewed);
    assert!(reviewer_evidence.exists());
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
fn confirmed_blocked_worker_can_be_stopped_once_with_durable_intent() {
    for ambiguous in [false, true] {
        let f = Fixture::new();
        f.apply(RequestKind::Start);
        f.herdr_state.lock().unwrap().agent_status = "blocked".into();
        success(f.once());
        let blocked = f.state().workers.runs[0].clone();
        assert_eq!(blocked.status, WorkerStatus::NeedsHuman);
        assert_eq!(
            blocked.human_request_kind,
            Some(store::HumanRequestKind::HerdrBlockedUi)
        );
        f.herdr_state.lock().unwrap().ambiguous_close_response = ambiguous;

        let stopped = f
            .cli()
            .args(["stop-worker", "--map", MAP, "--run", &blocked.id])
            .output()
            .unwrap();
        if ambiguous {
            assert!(!stopped.status.success());
            assert_eq!(f.state().workers.runs[0].status, WorkerStatus::Uncertain);
        } else {
            success(stopped);
            assert_eq!(f.state().workers.runs[0].status, WorkerStatus::Stopped);
        }
        success(f.once());
        let records = f.herdr_state.lock().unwrap();
        assert_eq!(records.closed_panes, ["pane-owned"]);
        assert_eq!(
            records
                .requests
                .iter()
                .filter(|request| request["method"] == "pane.close")
                .count(),
            1,
            "an ambiguous stop is never repeated"
        );
    }
}

#[test]
fn blocked_worker_replacement_is_never_closed() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    f.herdr_state.lock().unwrap().agent_status = "blocked".into();
    success(f.once());
    let blocked = f.state().workers.runs[0].clone();
    f.herdr_state
        .lock()
        .unwrap()
        .agents
        .get_mut("pane-owned")
        .unwrap()["terminal_id"] = json!("replacement-terminal");

    let stopped = f
        .cli()
        .args(["stop-worker", "--map", MAP, "--run", &blocked.id])
        .output()
        .unwrap();
    assert!(!stopped.status.success());
    assert_eq!(f.state().workers.runs[0].status, WorkerStatus::Uncertain);
    assert!(f.herdr_state.lock().unwrap().closed_panes.is_empty());
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
        human_request_seq: 0,
        human_request_id: None,
        human_request_kind: None,
        human_request_fingerprint: None,
        answer_request_id: None,
        answer_request_kind: None,
        answer_history: Vec::new(),
        source_run: None,
        claim_login: Some("fixture-user".into()),
        context: None,
        last_activity_ms: None,
        terminal_id: None,
        agent_provider: None,
        agent_session: None,
        foreground_process: None,
        result_evidence: None,
        initial_prompt_pending: false,
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
