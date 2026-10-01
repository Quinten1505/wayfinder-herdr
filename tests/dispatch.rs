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
    ambiguous_input_response: bool,
    reject_next_prompt_as_blocked: bool,
    changed_read: bool,
    sent_inputs: Vec<(String, String, Vec<String>)>,
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
            if let Some(tree) = state.opened_worktrees.iter().find(|tree| tree.pane == pane) {
                let info = json!({"agent":provider,"agent_session":null,"agent_status":"working","name":name,"terminal_id":tree.terminal,"workspace_id":tree.workspace,"tab_id":tree.tab,"pane_id":tree.pane});
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
            let text = if state.changed_read {
                "Approval required: allow running a shell command?"
            } else {
                "Question: Which option should I use, A or B?"
            };
            result = json!({"type":"pane_read","read":{"text":text}})
        }
        "pane.send_input" => {
            let pane = request["params"]["pane_id"].as_str().unwrap().to_owned();
            let text = request["params"]["text"].as_str().unwrap().to_owned();
            let keys = request["params"]["keys"]
                .as_array()
                .unwrap()
                .iter()
                .map(|key| key.as_str().unwrap().to_owned())
                .collect::<Vec<_>>();
            state.sent_inputs.push((pane.clone(), text, keys));
            state.agent_status = "working".into();
            result = json!({"type":"pane_input_sent"});
            if state.ambiguous_input_response {
                state.ambiguous_input_response = false;
                error = Some(
                    json!({"code":"response_lost","message":"simulated lost pane.send_input response"}),
                );
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
fn blocked_herdr_ui_uses_correlated_pane_input_not_rejected_agent_prompt() {
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
    assert!(f.herdr_state.lock().unwrap().sent_inputs.is_empty());

    success(answer_cli(&f, &pending, "herdr_blocked_ui", "A"));
    let answered = f.state().workers.runs[0].clone();
    assert_eq!(answered.status, WorkerStatus::Running);
    assert_eq!(answered.human_response.as_deref(), Some("A"));
    assert_eq!(answered.answer_request_id, pending.human_request_id);
    let records = f.herdr_state.lock().unwrap();
    assert_eq!(
        records.sent_inputs,
        [("pane-owned".into(), "A".into(), vec!["enter".into()])]
    );
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.prompt")
            .count(),
        1,
        "only the original worker launch prompt was sent"
    );
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "pane.send_input")
            .count(),
        1
    );
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
    assert!(f.herdr_state.lock().unwrap().sent_inputs.is_empty());
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
fn blocked_ui_input_lost_response_remains_uncertain_and_is_not_repeated() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    f.herdr_state.lock().unwrap().agent_status = "blocked".into();
    success(f.once());
    let pending = f.state().workers.runs[0].clone();
    f.herdr_state.lock().unwrap().ambiguous_input_response = true;
    let failed = answer_cli(&f, &pending, "herdr_blocked_ui", "A");
    assert!(!failed.status.success());
    assert_eq!(f.state().workers.runs[0].status, WorkerStatus::Uncertain);
    assert!(f.state().workers.runs[0].status.reserves_capacity());
    success(f.once());
    let records = f.herdr_state.lock().unwrap();
    assert_eq!(records.sent_inputs.len(), 1);
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "pane.send_input")
            .count(),
        1
    );
}

#[test]
fn repeated_identical_blocked_prompt_gets_a_new_request_identity() {
    let f = Fixture::new();
    f.apply(RequestKind::Start);
    f.herdr_state.lock().unwrap().agent_status = "blocked".into();
    success(f.once());
    let first = f.state().workers.runs[0].clone();
    success(answer_cli(&f, &first, "herdr_blocked_ui", "A"));
    assert!(f.state().workers.runs[0].human_request_id.is_none());

    f.herdr_state.lock().unwrap().agent_status = "blocked".into();
    success(f.once());
    let second = f.state().workers.runs[0].clone();
    assert_eq!(second.question, first.question);
    assert_ne!(second.human_request_id, first.human_request_id);
    let stale = answer_cli(&f, &first, "herdr_blocked_ui", "A");
    assert!(!stale.status.success());
    assert_eq!(f.herdr_state.lock().unwrap().sent_inputs.len(), 1);
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
        f.herdr_state.lock().unwrap().sent_inputs.len(),
        0,
        "the rejected answer is never replayed into the blocked screen"
    );

    success(answer_cli(&f, &refreshed, "herdr_blocked_ui", "B"));
    assert_eq!(f.state().workers.runs[0].status, WorkerStatus::Running);
    assert_eq!(f.herdr_state.lock().unwrap().sent_inputs.len(), 1);
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
            source_run: None,
            claim_login: None,
            context: None,
            last_activity_ms: None,
            terminal_id: None,
            agent_provider: None,
            agent_session: None,
            foreground_process: None,
            result_evidence: None,
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
        source_run: None,
        claim_login: Some("fixture-user".into()),
        context: None,
        last_activity_ms: None,
        terminal_id: None,
        agent_provider: None,
        agent_session: None,
        foreground_process: None,
        result_evidence: None,
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
