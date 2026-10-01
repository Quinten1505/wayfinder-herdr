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
use wayfinder_herdr::store::{
    self, AnswerDisposition, Authorization, Binding, HumanAnswerEvidence, HumanRequestKind,
    Provider, RequestKind, WorkerRun, WorkerStatus,
};
use wayfinder_herdr::{
    herdr::Client,
    orchestration::{self, SchedulerDecisionNotice},
    tracker::GitHub,
};

const MAP: &str = "example/project#42";
const GH_MOCK: &str = r##"#!/usr/bin/env python3
import json, os, sys
args=sys.argv[1:]
path=args[-1]
state_path=os.environ['MOCK_GH_STATE']
with open(state_path) as f: state=json.load(f)
def dump(value): print(json.dumps(value))
def child(number=13):
    assigned=(state['assigned'] if number == 13 else number in state.get('assigned_tickets', []))
    label='wayfinder:task' if number == 13 else 'wayfinder:research'
    return {'id':number*100,'number':number,'title':f'Implement sample task {number}','html_url':f'https://github.com/example/project/issues/{number}','body':'Implement the requested feature.','state':'open','assignees':([{'login':state['login']}] if assigned else []),'labels':[{'name':label}]}
def map_issue():
    return {'id':4200,'number':42,'title':'Map','body':'## Notes\n\nExecution override: selected by the user for this effort.','state':'open','assignees':[],'labels':[{'name':'wayfinder:map'}]}
if path == 'user': dump({'login':state['login']})
elif '--paginate' in args:
    route=path.split('?')[0]
    if route.endswith('/sub_issues'): page=[child()] + ([child(14)] if state.get('second_ticket') else [])
    elif '/dependencies/blocked_by' in route:
        ticket=int(route.split('/')[-3])
        page=[child(number) for number in state.get('blockers',{}).get(str(ticket),[])]
    else: page=[]
    dump([page])
elif '--method' in args:
    method=args[args.index('--method')+1]
    route=args[args.index('--method')+2]
    body=json.loads(sys.stdin.read() or '{}')
    if route.endswith('/assignees'):
        ticket=int(route.split('/')[-2])
        state.setdefault('claim_attempts', []).append(ticket)
        is_assigned=body['assignees'][0] == state['login']
        if ticket == 13: state['assigned']=is_assigned
        else:
            tickets=state.setdefault('assigned_tickets', [])
            if is_assigned and ticket not in tickets: tickets.append(ticket)
            if not is_assigned and ticket in tickets: tickets.remove(ticket)
        with open(state_path,'w') as f: json.dump(state,f)
        if ticket in state.get('fail_claim_after_effect_tickets', []):
            state['fail_issue_read_after_claim_ticket']=ticket
            with open(state_path,'w') as f: json.dump(state,f)
            sys.stderr.write('simulated lost GitHub claim response\n'); sys.exit(1)
        dump({'assignees':[{'login':state['login']}]})
    else: dump({})
elif path.endswith('/issues/42'): dump(map_issue())
elif path.endswith('/issues/13'):
    if state.get('fail_issue_read_after_claim_ticket') == 13:
        state['fail_issue_read_after_claim_ticket']=None
        with open(state_path,'w') as f: json.dump(state,f)
        sys.stderr.write('simulated unreadable post-claim issue\n'); sys.exit(1)
    dump(child())
elif path.endswith('/issues/14'):
    if state.get('fail_issue_read_after_claim_ticket') == 14:
        state['fail_issue_read_after_claim_ticket']=None
        with open(state_path,'w') as f: json.dump(state,f)
        sys.stderr.write('simulated unreadable post-claim issue\n'); sys.exit(1)
    dump(child(14))
else: sys.stderr.write('unhandled fake gh route: '+path+'\n'); sys.exit(2)
"##;

#[derive(Default)]
struct HerdrState {
    requests: Vec<Value>,
    agent_status: String,
    ambiguous_open_response: bool,
    fail_open_without_resource: bool,
    ambiguous_split_response: bool,
    omit_split_pane_id: bool,
    ambiguous_start_response: bool,
    ambiguous_close_response: bool,
    ambiguous_prompt_response: bool,
    ambiguous_prompt_before_effect: bool,
    reject_next_prompt_as_blocked: bool,
    fail_first_open_without_resource: bool,
    fail_next_read: bool,
    empty_next_read: bool,
    changed_read: bool,
    opened_worktrees: Vec<OpenedWorktree>,
    chat_panes: HashMap<String, OpenedWorktree>,
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
            json!({"login":"fixture-user","assigned":false,"assigned_tickets":[],"second_ticket":false,"blockers":{},"claim_attempts":[],"fail_claim_after_effect_tickets":[]}).to_string(),
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
    fn chat(&self) -> Output {
        self.cli()
            .args(["chat", "--map", MAP])
            .env("HERDR_SOCKET_PATH", self.state().binding.socket)
            .env(
                "HERDR_PLUGIN_CONTEXT_JSON",
                json!({
                    "workspace_cwd":self.state().binding.repository,
                    "workspace_id":"origin-workspace",
                    "tab_id":"origin-tab",
                    "focused_pane_id":"origin-pane"
                })
                .to_string(),
            )
            .output()
            .unwrap()
    }
    fn recover_chat(&self, confirmed: bool) -> Output {
        let state = self.state();
        let mut command = self.cli();
        command.arg("recover-chat").args(["--map", MAP]);
        if confirmed {
            command.arg("--confirm-replacement");
        }
        command
            .env("HERDR_SOCKET_PATH", state.binding.socket)
            .env(
                "HERDR_PLUGIN_CONTEXT_JSON",
                json!({
                    "workspace_cwd":state.binding.repository,
                    "workspace_id":"origin-workspace",
                    "tab_id":"origin-tab",
                    "focused_pane_id":"origin-pane"
                })
                .to_string(),
            )
            .output()
            .unwrap()
    }
    fn recover_interrupted_chat(&self, confirm_replacement: bool, confirm_absent: bool) -> Output {
        let state = self.state();
        let mut command = self.cli();
        command.arg("recover-chat").args(["--map", MAP]);
        if confirm_replacement {
            command.arg("--confirm-replacement");
        }
        if confirm_absent {
            command.arg("--confirm-launch-absent-or-stopped");
        }
        command
            .env("HERDR_SOCKET_PATH", state.binding.socket)
            .env(
                "HERDR_PLUGIN_CONTEXT_JSON",
                json!({
                    "workspace_cwd":state.binding.repository,
                    "workspace_id":"origin-workspace",
                    "tab_id":"origin-tab",
                    "focused_pane_id":"origin-pane"
                })
                .to_string(),
            )
            .output()
            .unwrap()
    }
    fn resolve_chat_message(&self, id: &str, delivered: bool) -> Output {
        let mut command = self.cli();
        command.args(["resolve-chat-delivery", "--map", MAP, "--message", id]);
        command.arg(if delivered {
            "--confirmed-delivered"
        } else {
            "--confirmed-not-delivered"
        });
        command.output().unwrap()
    }
    fn apply(&self, kind: RequestKind) {
        store::enqueue(&self.dir, kind).unwrap();
        success(self.once());
    }
    fn state(&self) -> store::State {
        store::read_state(&self.dir).unwrap()
    }
}

fn record_pending_worker_question(f: &Fixture) -> store::State {
    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    f.apply(RequestKind::Start);
    let mut state = f.state();
    let run = state
        .workers
        .runs
        .iter_mut()
        .find(|run| run.ticket == 13)
        .unwrap();
    run.status = WorkerStatus::NeedsHuman;
    run.question = Some("Which API shape should I use?".into());
    run.human_request_seq = 1;
    run.human_request_id = Some(format!("human-{}-0001", run.id));
    run.human_request_kind = Some(HumanRequestKind::WorkerQuestion);
    run.human_request_fingerprint = Some(store::human_request_fingerprint(
        run.question.as_deref().unwrap(),
    ));
    run.answer_history.push(HumanAnswerEvidence {
        request_id: format!("human-{}-0000", run.id),
        request_kind: HumanRequestKind::WorkerQuestion,
        response: "previous recorded answer".into(),
        disposition: AnswerDisposition::Submitted,
    });
    store::atomic_json(&f.dir.join("state.json"), &state).unwrap();
    state
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
        "pane.get" => {
            let pane = request["params"]["pane_id"].as_str().unwrap();
            if pane == "origin-pane" {
                result = json!({"type":"pane_info","pane":{"pane_id":"origin-pane","workspace_id":"origin-workspace","tab_id":"origin-tab","terminal_id":"origin-terminal","agent_status":"working"}});
            } else if let Some(chat) = state.chat_panes.get(pane) {
                result = json!({"type":"pane_info","pane":{"pane_id":chat.pane,"workspace_id":chat.workspace,"tab_id":chat.tab,"terminal_id":chat.terminal,"agent_status":"idle"}});
            } else {
                error = Some(json!({"code":"pane_not_found","message":"unknown pane"}));
            }
        }
        "pane.split" => {
            let pane_number = state.chat_panes.len() + 1;
            let pane_id = if pane_number == 1 {
                "orchestrator-pane".to_owned()
            } else {
                format!("orchestrator-pane-{pane_number}")
            };
            let pane = OpenedWorktree {
                path: request["params"]["cwd"].as_str().unwrap().to_owned(),
                workspace: request["params"]["workspace_id"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
                tab: "origin-tab".into(),
                pane: pane_id.clone(),
                terminal: format!("orchestrator-terminal-{pane_number}"),
            };
            state.chat_panes.insert(pane_id, pane.clone());
            result = json!({"type":"pane_info","pane":{"pane_id":pane.pane,"workspace_id":pane.workspace,"tab_id":pane.tab,"terminal_id":pane.terminal,"cwd":pane.path}});
            if state.omit_split_pane_id {
                state.omit_split_pane_id = false;
                result["pane"].as_object_mut().unwrap().remove("pane_id");
            } else if state.ambiguous_split_response {
                state.ambiguous_split_response = false;
                error = Some(
                    json!({"code":"response_lost","message":"simulated lost pane.split response"}),
                );
            }
        }
        "worktree.open" => {
            if state.fail_first_open_without_resource {
                state.fail_first_open_without_resource = false;
                error = Some(
                    json!({"code":"untrusted_repository","message":"repository trust approval required"}),
                );
            } else if state.fail_open_without_resource {
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
                .or_else(|| state.chat_panes.get(&pane).cloned())
            {
                state.agent_status = "idle".into();
                let info = json!({"agent":provider,"agent_session":null,"agent_status":"idle","name":name,"terminal_id":tree.terminal,"workspace_id":tree.workspace,"tab_id":tree.tab,"pane_id":tree.pane});
                state.agents.insert(pane.clone(), info.clone());
                result = json!({"type":"agent_started","agent":info,"argv":[]});
                if state.ambiguous_start_response {
                    state.ambiguous_start_response = false;
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
            if state.ambiguous_prompt_before_effect {
                state.ambiguous_prompt_before_effect = false;
                error = Some(
                    json!({"code":"response_lost","message":"simulated lost request before effect"}),
                );
            } else if state.agent_status == "blocked" || state.reject_next_prompt_as_blocked {
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
                if state.ambiguous_prompt_response {
                    state.ambiguous_prompt_response = false;
                    error = Some(
                        json!({"code":"response_lost","message":"simulated lost agent.prompt response"}),
                    );
                }
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
        "agent.focus" => {
            let pane = request["params"]["target"].as_str().unwrap();
            if state.agents.contains_key(pane) {
                result = json!({"type":"agent_focused","pane_id":pane});
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

fn success_status(output: Output) -> bool {
    output.status.success()
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
fn one_orchestrator_chat_uses_configured_provider_and_delivers_grouped_linked_questions_once() {
    let f = Fixture::new();
    let mut github = serde_json::from_slice::<Value>(&fs::read(&f.gh_state).unwrap()).unwrap();
    github["second_ticket"] = json!(true);
    github["blockers"] = json!({"14":[13]});
    fs::write(&f.gh_state, serde_json::to_vec(&github).unwrap()).unwrap();
    let mut state = f.state();
    state.workers.providers.roles.insert(
        "orchestrator".into(),
        Provider {
            kind: "codex".into(),
            model: Some("gpt-6-luna".into()),
            reasoning_effort: Some("high".into()),
            args: vec!["--fast".into()],
        },
    );
    store::atomic_json(&f.dir.join("state.json"), &state).unwrap();

    success(f.chat());
    let bound = f.state().orchestrator.unwrap();
    assert_eq!(bound.status, store::OrchestratorStatus::Running);
    assert_eq!(bound.workspace_id, "origin-workspace");
    assert_eq!(bound.tab_id, "origin-tab");
    assert_eq!(bound.pane_id, "orchestrator-pane");
    assert_eq!(bound.provider, "codex");
    assert_eq!(
        bound.session.as_ref().unwrap().value,
        "conversation-orchestrator-pane"
    );

    let first_calls = f.herdr_state.lock().unwrap().requests.clone();
    let split = first_calls
        .iter()
        .find(|call| call["method"] == "pane.split")
        .unwrap();
    assert_eq!(split["params"]["target_pane_id"], "origin-pane");
    assert_eq!(split["params"]["workspace_id"], "origin-workspace");
    let start = first_calls
        .iter()
        .find(|call| call["method"] == "agent.start")
        .unwrap();
    assert_eq!(start["params"]["pane_id"], "orchestrator-pane");
    assert_eq!(start["params"]["kind"], "codex");
    assert!(
        start["params"]["args"]
            .as_array()
            .unwrap()
            .contains(&json!("gpt-6-luna"))
    );
    let initial = first_calls
        .iter()
        .find(|call| call["method"] == "agent.prompt")
        .unwrap();
    assert!(
        initial["params"]["text"]
            .as_str()
            .unwrap()
            .contains("one human-facing Wayfinder orchestrator chat")
    );
    assert!(
        initial["params"]["text"]
            .as_str()
            .unwrap()
            .contains("Never answer for the human")
    );
    let initial_prompt = initial["params"]["text"].as_str().unwrap();
    assert!(initial_prompt.contains("$HOME/.agents/skills/wayfinder/SKILL.md"));
    assert!(initial_prompt.contains("Wayfinding is planning by default"));
    assert!(initial_prompt.contains("Opening this chat does not grant execution authorization"));
    assert!(!initial_prompt.contains("This map has explicit execution authorization"));
    assert!(initial_prompt.contains("retry-worker --map example/project#42 --run RUN"));
    assert!(initial_prompt.contains("--confirmed-absent-or-stopped"));
    assert!(initial_prompt.contains("abandon-worker --map example/project#42 --run RUN"));
    assert!(initial_prompt.contains(
        "without proving termination, releasing uncertain capacity, or deleting artifacts"
    ));
    assert!(initial_prompt.contains("recover-chat --map example/project#42 --confirm-replacement"));
    assert!(initial_prompt.contains("--confirm-launch-absent-or-stopped"));
    assert!(
        initial_prompt
            .contains("resolve-chat-delivery --map example/project#42 --message MESSAGE_ID")
    );
    assert!(
        initial_prompt.contains("answer-decision --map example/project#42 --request-id REQUEST_ID")
    );
    assert!(initial_prompt.contains("never sends input to a completed or stale worker"));
    assert_eq!(f.state().authorization, Authorization::AwaitingStart);
    assert!(f.state().workers.runs.is_empty());
    assert_eq!(
        first_calls
            .iter()
            .filter(|call| call["method"] == "worktree.open")
            .count(),
        0
    );
    assert_eq!(
        first_calls
            .iter()
            .filter(|call| call["method"] == "agent.start")
            .count(),
        1,
        "chat attachment starts only its configured orchestrator, never delegated workers"
    );

    success(f.chat());
    {
        let calls = f.herdr_state.lock().unwrap();
        assert_eq!(
            calls
                .requests
                .iter()
                .filter(|call| call["method"] == "pane.split")
                .count(),
            1
        );
        assert_eq!(
            calls
                .requests
                .iter()
                .filter(|call| call["method"] == "agent.start")
                .count(),
            1
        );
        assert_eq!(
            calls
                .requests
                .iter()
                .filter(|call| call["method"] == "agent.focus")
                .count(),
            1
        );
    }

    f.apply(RequestKind::Start);
    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    let mut state = f.state();
    let run = state
        .workers
        .runs
        .iter_mut()
        .find(|run| run.ticket == 13)
        .unwrap();
    run.status = WorkerStatus::NeedsHuman;
    run.question = Some("Which API shape should I use?".into());
    run.human_request_seq = 1;
    run.human_request_id = Some(format!("human-{}-0001", run.id));
    run.human_request_kind = Some(HumanRequestKind::WorkerQuestion);
    run.human_request_fingerprint = Some(store::human_request_fingerprint(
        run.question.as_deref().unwrap(),
    ));
    store::atomic_json(&f.dir.join("state.json"), &state).unwrap();

    success(f.once());
    let calls = f.herdr_state.lock().unwrap().requests.clone();
    let questions = calls
        .iter()
        .filter(|call| call["method"] == "agent.prompt")
        .filter_map(|call| call["params"]["text"].as_str())
        .filter(|text| text.contains("Independent human decisions are pending"))
        .collect::<Vec<_>>();
    assert_eq!(questions.len(), 1);
    assert!(
        questions[0]
            .contains("[Implement sample task 13](https://github.com/example/project/issues/13)")
    );
    assert!(
        questions[0]
            .contains("[Implement sample task 14](https://github.com/example/project/issues/14)")
    );
    assert!(questions[0].contains("Which API shape should I use?"));
    assert!(questions[0].contains("Recommendation:"));
    assert!(questions[0].contains(&format!(
        "request ID human-{}-0001",
        state.workers.runs[0].id
    )));
    assert!(!questions[0].starts_with("run-"));

    success(f.once()); // Runtime restart/reconciliation reuses the durable outbox marker.
    let repeated = f
        .herdr_state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|call| call["method"] == "agent.prompt")
        .filter(|call| {
            call["params"]["text"]
                .as_str()
                .is_some_and(|text| text.contains("Independent human decisions are pending"))
        })
        .count();
    assert_eq!(
        repeated, 1,
        "the same durable decision round was not redelivered"
    );
}

#[test]
fn ambiguous_chat_delivery_is_retained_and_never_blindly_repeated() {
    let f = Fixture::new();
    success(f.chat());
    {
        let mut herdr = f.herdr_state.lock().unwrap();
        herdr.agent_status = "idle".into();
        herdr.ambiguous_prompt_response = true;
    }
    success(f.once());
    let outbox: Value =
        serde_json::from_slice(&fs::read(f.dir.join("chat-outbox.json")).unwrap()).unwrap();
    assert!(
        outbox["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["status"] == "uncertain")
    );

    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    success(f.once());
    let prompts = f
        .herdr_state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|request| request["method"] == "agent.prompt")
        .count();
    assert_eq!(
        prompts, 2,
        "the uncertain outbox message was not submitted twice"
    );
    let outbox: Value =
        serde_json::from_slice(&fs::read(f.dir.join("chat-outbox.json")).unwrap()).unwrap();
    let uncertain = outbox["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["status"] == "uncertain")
        .unwrap();
    let id = uncertain["id"].as_str().unwrap();
    success(f.resolve_chat_message(id, true));
    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    success(f.once());
    let marker = format!("[WAYFINDER OUTBOX MESSAGE {id}]");
    let attempts = f
        .herdr_state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|request| {
            request["method"] == "agent.prompt"
                && request["params"]["text"]
                    .as_str()
                    .is_some_and(|text| text.contains(&marker))
        })
        .count();
    assert_eq!(
        attempts, 1,
        "a human-confirmed delivered notice stays delivered"
    );
}

#[test]
fn explicit_chat_recovery_archives_changed_identity_and_preserves_pending_human_state() {
    let f = Fixture::new();
    success(f.chat());
    let original = record_pending_worker_question(&f);
    let old = original.orchestrator.clone().unwrap();
    {
        let mut herdr = f.herdr_state.lock().unwrap();
        herdr.agent_status = "idle".into();
        herdr.agents.insert(
            old.pane_id.clone(),
            json!({
                "agent":"claude",
                "agent_session":{"source":"fixture","agent":"claude","kind":"id","value":"replacement-occupant"},
                "agent_status":"idle",
                "terminal_id":"replacement-terminal",
                "workspace_id":old.workspace_id,
                "tab_id":old.tab_id,
                "pane_id":old.pane_id,
            }),
        );
        herdr.sessions.insert(
            old.pane_id.clone(),
            json!({"source":"fixture","agent":"claude","kind":"id","value":"replacement-occupant"}),
        );
    }

    let before = f.herdr_state.lock().unwrap().requests.len();
    assert!(!success_status(f.recover_chat(false)));
    assert_eq!(
        f.herdr_state.lock().unwrap().requests.len(),
        before,
        "replacement requires an explicit human confirmation before Herdr effects"
    );
    let stale = f.chat();
    assert!(!success_status(stale));

    success(f.recover_chat(true));
    let recovered = f.state();
    let current = recovered.orchestrator.as_ref().unwrap();
    assert_eq!(current.status, store::OrchestratorStatus::Running);
    assert_eq!(current.pane_id, "orchestrator-pane-2");
    assert_eq!(recovered.orchestrator_history.len(), 1);
    assert_eq!(
        recovered.orchestrator_history[0].binding.pane_id,
        old.pane_id
    );
    assert!(
        recovered.orchestrator_history[0]
            .reason
            .contains("human explicitly confirmed")
    );
    assert_eq!(
        recovered.workers.runs[0].answer_history,
        original.workers.runs[0].answer_history
    );
    assert_eq!(
        recovered.workers.runs[0].human_request_id,
        original.workers.runs[0].human_request_id
    );

    {
        let mut herdr = f.herdr_state.lock().unwrap();
        let occupant = herdr.agents[&old.pane_id].clone();
        assert_eq!(occupant["agent"], "claude");
        assert_eq!(occupant["terminal_id"], "replacement-terminal");
        herdr.agent_status = "idle".into();
    }
    success(f.once());
    success(f.once());
    let herdr = f.herdr_state.lock().unwrap();
    assert_eq!(herdr.agents[&old.pane_id]["agent"], "claude");
    let after_initial_chat = &herdr.requests[before..];
    assert_eq!(
        after_initial_chat
            .iter()
            .filter(|request| request["method"] == "agent.start"
                && request["params"]["pane_id"] == old.pane_id)
            .count(),
        0,
        "replacement never starts over the changed pane occupant"
    );
    assert_eq!(
        after_initial_chat
            .iter()
            .filter(|request| request["method"] == "agent.prompt"
                && request["params"]["target"] == old.pane_id)
            .count(),
        0,
        "replacement never sends input to the old pane occupant"
    );
    assert_eq!(
        after_initial_chat
            .iter()
            .filter(|request| request["method"] == "agent.focus"
                && request["params"]["target"] == old.pane_id)
            .count(),
        0
    );
    assert_eq!(
        after_initial_chat
            .iter()
            .filter(|request| request["method"] == "agent.prompt"
                && request["params"]["target"] == current.pane_id
                && request["params"]["text"]
                    .as_str()
                    .is_some_and(|text| text.contains("Independent human decisions are pending")))
            .count(),
        1,
        "retained question reaches the verified replacement once and is not replayed on restart"
    );
}

#[test]
fn explicit_chat_recovery_can_replace_a_missing_agent_only_after_confirmation() {
    let f = Fixture::new();
    success(f.chat());
    let old = f.state().orchestrator.unwrap();
    {
        let mut herdr = f.herdr_state.lock().unwrap();
        herdr.agents.remove(&old.pane_id);
        herdr.sessions.remove(&old.pane_id);
    }
    let missing = f.chat();
    assert!(!success_status(missing));
    let before_recovery = f.herdr_state.lock().unwrap().requests.len();
    success(f.recover_chat(true));

    let state = f.state();
    assert_eq!(state.orchestrator_history.len(), 1);
    assert_eq!(state.orchestrator_history[0].binding.pane_id, old.pane_id);
    assert_eq!(
        state.orchestrator.as_ref().unwrap().pane_id,
        "orchestrator-pane-2"
    );
    let herdr = f.herdr_state.lock().unwrap();
    assert_eq!(
        herdr
            .requests
            .iter()
            .filter(|request| request["method"] == "pane.split")
            .count(),
        2,
        "explicit recovery creates exactly one replacement chat"
    );
    let recovery_calls = &herdr.requests[before_recovery..];
    assert!(
        !recovery_calls
            .iter()
            .any(|request| request["method"] == "pane.close")
    );
    assert!(!recovery_calls.iter().any(|request| {
        request["method"] == "agent.prompt" && request["params"]["target"] == old.pane_id
    }));
    assert!(!recovery_calls.iter().any(|request| {
        request["method"] == "agent.focus" && request["params"]["target"] == old.pane_id
    }));
}

#[test]
fn interrupted_chat_launch_stages_require_explicit_absence_and_preserve_pending_questions() {
    for stage in [
        store::OrchestratorStatus::PaneIntent,
        store::OrchestratorStatus::AgentIntent,
        store::OrchestratorStatus::PromptIntent,
        store::OrchestratorStatus::Uncertain,
    ] {
        let f = Fixture::new();
        let before = record_pending_worker_question(&f);
        success(f.chat());
        let mut state = f.state();
        let saved = state.orchestrator.as_mut().unwrap();
        saved.status = stage;
        if stage == store::OrchestratorStatus::PaneIntent {
            saved.pane_id.clear();
            saved.terminal_id = None;
            saved.session = None;
        } else if stage == store::OrchestratorStatus::AgentIntent {
            saved.session = None;
        }
        let old = saved.clone();
        store::atomic_json(&f.dir.join("state.json"), &state).unwrap();
        let held_outbox = br#"{"format_version":1,"messages":[{"id":"held-message","text":"retain delivery evidence","status":"uncertain","resolution_history":[]}]}"#;
        fs::write(f.dir.join("chat-outbox.json"), held_outbox).unwrap();
        {
            let mut herdr = f.herdr_state.lock().unwrap();
            herdr.requests.clear();
            if old.pane_id.is_empty() {
                let panes: Vec<_> = herdr.chat_panes.keys().cloned().collect();
                for pane in panes {
                    herdr.agents.remove(&pane);
                    herdr.sessions.remove(&pane);
                }
            } else {
                herdr.agents.remove(&old.pane_id);
                herdr.sessions.remove(&old.pane_id);
            }
        }

        let held = f.chat();
        assert!(!success_status(held), "Chat held {stage:?}");
        assert_eq!(
            f.herdr_state.lock().unwrap().requests.len(),
            0,
            "Chat does not automatically repeat any {stage:?} effect"
        );
        assert!(!success_status(f.recover_interrupted_chat(true, false)));
        assert_eq!(f.herdr_state.lock().unwrap().requests.len(), 0);

        success(f.recover_interrupted_chat(true, true));
        let recovered = f.state();
        assert_eq!(recovered.orchestrator_history.len(), 1);
        assert_eq!(recovered.orchestrator_history[0].binding.status, stage);
        assert_eq!(
            recovered.workers.runs[0].human_request_id, before.workers.runs[0].human_request_id,
            "pending question survives {stage:?} replacement"
        );
        assert_eq!(
            recovered.workers.runs[0].answer_history,
            before.workers.runs[0].answer_history
        );
        assert_eq!(
            fs::read(f.dir.join("chat-outbox.json")).unwrap(),
            held_outbox,
            "chat recovery preserves delivery evidence for {stage:?}"
        );
        assert_eq!(
            recovered.orchestrator.as_ref().unwrap().status,
            store::OrchestratorStatus::Running
        );
        let herdr = f.herdr_state.lock().unwrap();
        assert_eq!(
            herdr
                .requests
                .iter()
                .filter(|r| r["method"] == "pane.split")
                .count(),
            1,
            "recovery creates one fresh pane after confirming {stage:?} stopped"
        );
        assert_eq!(
            herdr
                .requests
                .iter()
                .filter(|r| r["method"] == "agent.start")
                .count(),
            1
        );
        assert_eq!(
            herdr
                .requests
                .iter()
                .filter(|r| r["method"] == "agent.prompt")
                .count(),
            1
        );
        assert!(herdr.requests.iter().all(|r| r["method"] != "pane.close"));
        assert!(herdr.requests.iter().all(|r| {
            !matches!(
                r["method"].as_str(),
                Some("agent.prompt" | "agent.start" | "agent.focus")
            ) || r["params"]["target"] != old.pane_id && r["params"]["pane_id"] != old.pane_id
        }));
    }
}

#[test]
fn acknowledged_agent_intent_resumes_only_the_not_yet_attempted_prompt() {
    let f = Fixture::new();
    success(f.chat());
    let before = record_pending_worker_question(&f);
    let workspace = "origin-workspace";
    let tab = "origin-tab";
    let pane = "acknowledged-start-pane";
    let terminal = "acknowledged-start-terminal";
    let session = json!({"source":"fixture","agent":"codex","kind":"id","value":"acknowledged-before-prompt"});
    {
        let mut herdr = f.herdr_state.lock().unwrap();
        herdr.chat_panes.insert(
            pane.into(),
            OpenedWorktree {
                path: f.state().binding.repository.to_string_lossy().into_owned(),
                workspace: workspace.into(),
                tab: tab.into(),
                pane: pane.into(),
                terminal: terminal.into(),
            },
        );
        herdr.agents.insert(
            pane.into(),
            json!({"agent":"codex","agent_session":session,"agent_status":"idle","terminal_id":terminal,"workspace_id":workspace,"tab_id":tab,"pane_id":pane}),
        );
        herdr.sessions.insert(pane.into(), session.clone());
        herdr.requests.clear();
    }
    let mut state = f.state();
    let binding = state.orchestrator.as_mut().unwrap();
    binding.status = store::OrchestratorStatus::AgentIntent;
    binding.workspace_id = workspace.into();
    binding.tab_id = tab.into();
    binding.pane_id = pane.into();
    binding.terminal_id = Some(terminal.into());
    binding.session = Some(store::AgentSessionIdentity {
        source: "fixture".into(),
        agent: "codex".into(),
        kind: "id".into(),
        value: "acknowledged-before-prompt".into(),
    });
    store::atomic_json(&f.dir.join("state.json"), &state).unwrap();

    success(f.chat());
    let current = f.state();
    assert_eq!(
        current.orchestrator.unwrap().status,
        store::OrchestratorStatus::Running
    );
    assert_eq!(
        current.workers.runs[0].human_request_id,
        before.workers.runs[0].human_request_id
    );
    let herdr = f.herdr_state.lock().unwrap();
    assert_eq!(
        herdr
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.start")
            .count(),
        0
    );
    assert_eq!(
        herdr
            .requests
            .iter()
            .filter(|r| r["method"] == "pane.split")
            .count(),
        0
    );
    assert_eq!(
        herdr
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.prompt")
            .count(),
        1
    );
}

#[test]
fn lost_chat_launch_responses_never_repeat_split_start_or_prompt() {
    for failure in ["split", "start", "prompt"] {
        let f = Fixture::new();
        let before = record_pending_worker_question(&f);
        {
            let mut herdr = f.herdr_state.lock().unwrap();
            match failure {
                "split" => herdr.ambiguous_split_response = true,
                "start" => herdr.ambiguous_start_response = true,
                "prompt" => herdr.ambiguous_prompt_response = true,
                _ => unreachable!(),
            }
        }
        assert!(
            !success_status(f.chat()),
            "simulated lost {failure} response"
        );
        let saved = f.state();
        let old = saved.orchestrator.as_ref().unwrap().clone();
        assert_eq!(
            saved.workers.runs[0].human_request_id,
            before.workers.runs[0].human_request_id
        );
        let initial_counts = {
            let herdr = f.herdr_state.lock().unwrap();
            ["pane.split", "agent.start", "agent.prompt"].map(|method| {
                herdr
                    .requests
                    .iter()
                    .filter(|r| r["method"] == method)
                    .count()
            })
        };
        assert!(!success_status(f.chat()));
        assert_eq!(
            ["pane.split", "agent.start", "agent.prompt"].map(|method| {
                f.herdr_state
                    .lock()
                    .unwrap()
                    .requests
                    .iter()
                    .filter(|r| r["method"] == method)
                    .count()
            }),
            initial_counts,
            "Chat never repeats a launch effect after an ambiguous {failure} response"
        );
        assert!(!success_status(f.recover_interrupted_chat(true, false)));
        if failure != "split" {
            // A matching provider in the recorded terminal is still the possibly-live
            // interrupted launch, so the human must stop it before replacement.
            assert!(!success_status(f.recover_interrupted_chat(true, true)));
            let mut herdr = f.herdr_state.lock().unwrap();
            if !old.pane_id.is_empty() {
                herdr.agents.remove(&old.pane_id);
                herdr.sessions.remove(&old.pane_id);
            }
        }
        success(f.recover_interrupted_chat(true, true));
        let final_state = f.state();
        assert_eq!(final_state.orchestrator_history.len(), 1);
        assert_eq!(
            final_state.orchestrator_history[0].binding.status,
            old.status
        );
        assert_eq!(
            final_state.workers.runs[0].human_request_id,
            before.workers.runs[0].human_request_id
        );
        let herdr = f.herdr_state.lock().unwrap();
        assert_eq!(
            ["pane.split", "agent.start", "agent.prompt"].map(|method| {
                herdr
                    .requests
                    .iter()
                    .filter(|r| r["method"] == method)
                    .count()
            }),
            [
                initial_counts[0] + 1,
                initial_counts[1] + 1,
                initial_counts[2] + 1
            ]
        );
        assert!(herdr.requests.iter().all(|r| r["method"] != "pane.close"));
    }
}

#[test]
fn missing_split_pane_id_is_recoverable_only_after_explicit_confirmation() {
    let f = Fixture::new();
    {
        let mut herdr = f.herdr_state.lock().unwrap();
        herdr.omit_split_pane_id = true;
    }
    assert!(!success_status(f.chat()));
    let state = f.state();
    let old = state.orchestrator.unwrap();
    assert_eq!(old.status, store::OrchestratorStatus::PaneIntent);
    assert!(old.pane_id.is_empty());
    let before = f.herdr_state.lock().unwrap().requests.len();
    assert!(!success_status(f.chat()));
    assert_eq!(f.herdr_state.lock().unwrap().requests.len(), before);
    assert!(!success_status(f.recover_interrupted_chat(true, false)));
    assert_eq!(f.herdr_state.lock().unwrap().requests.len(), before);
    success(f.recover_interrupted_chat(true, true));
    let state = f.state();
    assert_eq!(
        state.orchestrator_history[0].binding.status,
        store::OrchestratorStatus::PaneIntent
    );
    assert!(state.orchestrator_history[0].binding.pane_id.is_empty());
    assert_eq!(
        state.orchestrator.as_ref().unwrap().pane_id,
        "orchestrator-pane-2"
    );
    let herdr = f.herdr_state.lock().unwrap();
    assert_eq!(
        herdr
            .requests
            .iter()
            .filter(|r| r["method"] == "pane.split")
            .count(),
        2
    );
    assert_eq!(
        herdr
            .requests
            .iter()
            .find(|r| r["method"] == "agent.start")
            .unwrap()["params"]["pane_id"],
        "orchestrator-pane-2",
        "recovery never starts an agent in an unrecorded pane"
    );
    assert_eq!(
        herdr
            .requests
            .iter()
            .find(|r| r["method"] == "agent.prompt")
            .unwrap()["params"]["target"],
        "orchestrator-pane-2"
    );
    assert!(herdr.requests.iter().all(|r| r["method"] != "pane.close"));
}

#[test]
fn prompt_accepted_chat_reconnects_without_duplicate_initial_prompt() {
    let f = Fixture::new();
    success(f.chat());
    let before = record_pending_worker_question(&f);
    let mut state = f.state();
    state.orchestrator.as_mut().unwrap().status = store::OrchestratorStatus::PromptAccepted;
    store::atomic_json(&f.dir.join("state.json"), &state).unwrap();
    f.herdr_state.lock().unwrap().requests.clear();

    success(f.chat());
    let recovered = f.state();
    assert_eq!(
        recovered.orchestrator.unwrap().status,
        store::OrchestratorStatus::Running
    );
    assert_eq!(
        recovered.workers.runs[0].human_request_id,
        before.workers.runs[0].human_request_id
    );
    let herdr = f.herdr_state.lock().unwrap();
    assert_eq!(
        herdr
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.prompt")
            .count(),
        0
    );
    assert_eq!(
        herdr
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.start")
            .count(),
        0
    );
    assert_eq!(
        herdr
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.focus")
            .count(),
        1
    );
}

#[test]
fn uncertain_chat_delivery_stays_held_across_recovery_until_human_resolution() {
    let f = Fixture::new();
    success(f.chat());
    {
        let mut herdr = f.herdr_state.lock().unwrap();
        herdr.agent_status = "idle".into();
        herdr.ambiguous_prompt_before_effect = true;
    }
    success(f.once());
    let before: Value =
        serde_json::from_slice(&fs::read(f.dir.join("chat-outbox.json")).unwrap()).unwrap();
    let uncertain = before["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["status"] == "uncertain")
        .unwrap()
        .clone();
    let id = uncertain["id"].as_str().unwrap().to_owned();
    let delivery_marker = format!("[WAYFINDER OUTBOX MESSAGE {id}]");
    let old = f.state().orchestrator.unwrap();
    {
        let mut herdr = f.herdr_state.lock().unwrap();
        herdr.agent_status = "idle".into();
        herdr.agents.insert(
            old.pane_id.clone(),
            json!({"agent":"claude","agent_session":{"source":"fixture","agent":"claude","kind":"id","value":"replacement-occupant"},"agent_status":"idle","terminal_id":"replacement-terminal","workspace_id":old.workspace_id,"tab_id":old.tab_id,"pane_id":old.pane_id}),
        );
        herdr.sessions.insert(
            old.pane_id.clone(),
            json!({"source":"fixture","agent":"claude","kind":"id","value":"replacement-occupant"}),
        );
    }
    success(f.recover_chat(true));
    let marker_prompts = f
        .herdr_state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|request| {
            request["method"] == "agent.prompt"
                && request["params"]["text"]
                    .as_str()
                    .is_some_and(|text| text.contains(&delivery_marker))
        })
        .count();
    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    success(f.once());
    let prompts_after_held_reconcile = f
        .herdr_state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|request| {
            request["method"] == "agent.prompt"
                && request["params"]["text"]
                    .as_str()
                    .is_some_and(|text| text.contains(&delivery_marker))
        })
        .count();
    assert_eq!(
        prompts_after_held_reconcile, marker_prompts,
        "uncertain delivery is never blindly replayed to the replacement chat"
    );
    let held: Value =
        serde_json::from_slice(&fs::read(f.dir.join("chat-outbox.json")).unwrap()).unwrap();
    assert_eq!(held["messages"][0]["status"], "uncertain");

    success(f.resolve_chat_message(&id, false));
    let resolved: Value =
        serde_json::from_slice(&fs::read(f.dir.join("chat-outbox.json")).unwrap()).unwrap();
    assert_eq!(resolved["messages"][0]["status"], "pending");
    assert_eq!(
        resolved["messages"][0]["resolution_history"][0]["choice"],
        "confirmed_not_delivered"
    );
    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    success(f.once());
    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    success(f.once());
    let final_prompts = f
        .herdr_state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|request| {
            request["method"] == "agent.prompt"
                && request["params"]["text"]
                    .as_str()
                    .is_some_and(|text| text.contains(&delivery_marker))
        })
        .count();
    assert_eq!(
        final_prompts,
        marker_prompts + 1,
        "the human's explicit not-delivered decision permits exactly one replay"
    );
    assert_eq!(
        f.herdr_state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|request| request["method"] == "agent.prompt"
                && request["params"]["target"] == "orchestrator-pane-2"
                && request["params"]["text"]
                    .as_str()
                    .is_some_and(|text| text.contains(&delivery_marker)))
            .count(),
        1,
        "confirmed replay is routed once to the verified replacement chat"
    );
}

#[test]
fn scheduler_decision_notice_reaches_chat_without_worker_input() {
    let f = Fixture::new();
    success(f.chat());
    let mut state = f.state();
    let target = state.orchestrator.as_ref().unwrap().pane_id.clone();
    let pending = SchedulerDecisionNotice {
        request_id: "scheduler-0007".into(),
        ticket_link: "[Implement sample task](https://github.com/example/project/issues/13)".into(),
        run_id: "run-completed-0007".into(),
        question: "Review retries are exhausted. Continue, change scope, or abandon?".into(),
        response: None,
    };
    let resolved = SchedulerDecisionNotice {
        response: Some("continue after human review".into()),
        ..pending.clone()
    };
    let herdr = Client::new(&state.binding.socket);
    let github = GitHub::default();
    f.herdr_state.lock().unwrap().agent_status = "idle".into();
    for _ in 0..3 {
        orchestration::reconcile(
            &f.dir,
            &mut state,
            &herdr,
            &github,
            &[],
            &[pending.clone(), resolved.clone()],
        )
        .unwrap();
        f.herdr_state.lock().unwrap().agent_status = "idle".into();
        state = f.state();
    }
    let calls = f.herdr_state.lock().unwrap().requests.clone();
    let decision_prompts = calls
        .iter()
        .filter(|call| call["method"] == "agent.prompt")
        .filter_map(|call| call["params"]["text"].as_str())
        .filter(|text| text.contains("request ID scheduler-0007"))
        .collect::<Vec<_>>();
    assert_eq!(decision_prompts.len(), 1);
    assert!(decision_prompts[0].contains(&pending.ticket_link));
    assert!(decision_prompts[0].contains("choose continuation, scope change, or abandonment"));
    assert!(decision_prompts[0].contains("answer-decision --map example/project#42"));
    assert!(decision_prompts[0].contains("never use answer-worker or send input to its pane"));
    assert!(calls.iter().any(|call| {
        call["method"] == "agent.prompt"
            && call["params"]["target"] == target
            && call["params"]["text"]
                .as_str()
                .is_some_and(|text| text.contains("request ID scheduler-0007"))
    }));
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
        rework_round_limit: store::DEFAULT_REWORK_ROUNDS,
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
        initial_prompt_acknowledged: false,
        initial_prompt_reconnect_pending: false,
        known_prelaunch_failure: false,
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
fn definite_prelaunch_setup_failure_releases_capacity_for_an_independent_ticket() {
    let f = Fixture::new();
    let mut github = serde_json::from_slice::<Value>(&fs::read(&f.gh_state).unwrap()).unwrap();
    github["second_ticket"] = json!(true);
    fs::write(&f.gh_state, serde_json::to_vec(&github).unwrap()).unwrap();
    f.herdr_state
        .lock()
        .unwrap()
        .fail_first_open_without_resource = true;

    f.apply(RequestKind::Start);

    let state = f.state();
    let failed = state
        .workers
        .runs
        .iter()
        .find(|run| run.ticket == 13)
        .unwrap();
    let independent = state
        .workers
        .runs
        .iter()
        .find(|run| run.ticket == 14)
        .unwrap();
    assert_eq!(failed.status, WorkerStatus::NeedsHuman);
    assert!(failed.known_prelaunch_failure);
    assert!(!failed.reserves_capacity());
    assert!(failed.claim_login.is_some(), "the GitHub claim is retained");
    assert!(failed.question.as_deref().unwrap().contains("retry-worker"));
    assert_eq!(independent.status, WorkerStatus::Running);
    assert!(independent.reserves_capacity());

    let records = f.herdr_state.lock().unwrap();
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|request| request["method"] == "worktree.open")
            .count(),
        2,
        "the held setup failure and independent ticket each had one open attempt"
    );
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|request| request["method"] == "agent.start")
            .count(),
        1,
        "only the independent ticket started an agent"
    );
    drop(records);

    success(f.once());
    let after_reconcile = f.state();
    assert_eq!(
        after_reconcile
            .workers
            .runs
            .iter()
            .find(|run| run.ticket == 13)
            .unwrap()
            .status,
        WorkerStatus::NeedsHuman
    );
    let records = f.herdr_state.lock().unwrap();
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|request| request["method"] == "worktree.open")
            .count(),
        2,
        "the claimed setup failure was not automatically retried"
    );
}

#[test]
fn ambiguous_claim_failure_releases_agent_slot_but_keeps_claim_and_intent_held() {
    let f = Fixture::new();
    let mut github = serde_json::from_slice::<Value>(&fs::read(&f.gh_state).unwrap()).unwrap();
    github["second_ticket"] = json!(true);
    github["fail_claim_after_effect_tickets"] = json!([13]);
    fs::write(&f.gh_state, serde_json::to_vec(&github).unwrap()).unwrap();

    f.apply(RequestKind::Start);

    let state = f.state();
    let uncertain = state
        .workers
        .runs
        .iter()
        .find(|run| run.ticket == 13)
        .unwrap();
    let independent = state
        .workers
        .runs
        .iter()
        .find(|run| run.ticket == 14)
        .unwrap();
    assert_eq!(uncertain.status, WorkerStatus::Uncertain);
    assert!(uncertain.known_prelaunch_failure);
    assert!(!uncertain.reserves_capacity());
    assert!(
        uncertain
            .question
            .as_deref()
            .unwrap()
            .contains("Claim outcome is uncertain")
    );
    assert!(!uncertain.worktree.exists());
    assert_eq!(independent.status, WorkerStatus::Running);

    let github = serde_json::from_slice::<Value>(&fs::read(&f.gh_state).unwrap()).unwrap();
    assert!(
        github["assigned"].as_bool().unwrap(),
        "the ambiguous GitHub claim is retained"
    );
    assert_eq!(
        github["claim_attempts"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| *t == 13)
            .count(),
        1
    );

    success(f.once());
    let after = f.state();
    assert_eq!(
        after
            .workers
            .runs
            .iter()
            .find(|run| run.ticket == 13)
            .unwrap()
            .status,
        WorkerStatus::Uncertain
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(&f.gh_state).unwrap()).unwrap()["claim_attempts"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| *t == 13)
            .count(),
        1,
        "reconciliation does not retry an ambiguous claim"
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
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.start")
            .count(),
        1
    );
    drop(records);

    // A human can reconcile the retained claim using fresh GitHub reads, then explicitly
    // authorize a new worker after confirming the ambiguous prelaunch run is absent.
    success(
        f.cli()
            .args(["tracker", "reconcile-claim", "--map", MAP, "--ticket", "13"])
            .output()
            .unwrap(),
    );
    success(
        f.cli()
            .args(["configure-worker", "--map", MAP, "--concurrency", "2"])
            .output()
            .unwrap(),
    );
    success(
        f.cli()
            .args([
                "retry-worker",
                "--map",
                MAP,
                "--run",
                &uncertain.id,
                "--confirmed-absent-or-stopped",
            ])
            .output()
            .unwrap(),
    );
    success(f.once());
    let retried = f.state();
    assert_eq!(
        retried
            .workers
            .runs
            .iter()
            .filter(|run| run.ticket == 13 && run.status == WorkerStatus::Running)
            .count(),
        1
    );
    let github = serde_json::from_slice::<Value>(&fs::read(&f.gh_state).unwrap()).unwrap();
    assert_eq!(
        github["claim_attempts"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|ticket| *ticket == 13)
            .count(),
        1,
        "recovered retry reuses the proven assignment without another assignment request"
    );
    let records = f.herdr_state.lock().unwrap();
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|request| request["method"] == "agent.start")
            .count(),
        2,
        "the retry worker is dispatched once alongside the independent worker"
    );
    drop(records);
    success(f.once());
    assert_eq!(
        f.herdr_state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|request| request["method"] == "agent.start")
            .count(),
        2,
        "later reconciliation does not dispatch a duplicate retry"
    );
}

#[test]
fn invalid_preexisting_worktree_releases_slot_without_touching_the_path() {
    let f = Fixture::new();
    let mut github = serde_json::from_slice::<Value>(&fs::read(&f.gh_state).unwrap()).unwrap();
    github["second_ticket"] = json!(true);
    fs::write(&f.gh_state, serde_json::to_vec(&github).unwrap()).unwrap();
    let repository = f.state().binding.repository;
    let invalid = repository.parent().unwrap().join(".wayfinder-42-13-1");
    fs::create_dir_all(&invalid).unwrap();
    fs::write(invalid.join("human-data.txt"), "preserve me\n").unwrap();

    f.apply(RequestKind::Start);

    let state = f.state();
    let held = state
        .workers
        .runs
        .iter()
        .find(|run| run.ticket == 13)
        .unwrap();
    let independent = state
        .workers
        .runs
        .iter()
        .find(|run| run.ticket == 14)
        .unwrap();
    assert_eq!(held.status, WorkerStatus::Uncertain);
    assert!(held.known_prelaunch_failure);
    assert!(!held.reserves_capacity());
    assert!(held.claim_login.is_some());
    assert!(
        held.question
            .as_deref()
            .unwrap()
            .contains("no Herdr workspace or agent was requested")
    );
    assert_eq!(independent.status, WorkerStatus::Running);
    assert_eq!(
        fs::read_to_string(invalid.join("human-data.txt")).unwrap(),
        "preserve me\n"
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
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.start")
            .count(),
        1
    );
    drop(records);

    success(f.once());
    let after = f.state();
    assert_eq!(
        after
            .workers
            .runs
            .iter()
            .find(|run| run.ticket == 13)
            .unwrap()
            .status,
        WorkerStatus::Uncertain
    );
    assert_eq!(
        fs::read_to_string(invalid.join("human-data.txt")).unwrap(),
        "preserve me\n"
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
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.start")
            .count(),
        1
    );
}

#[test]
fn invalid_provider_arguments_are_known_prelaunch_and_release_capacity() {
    let f = Fixture::new();
    let mut github = serde_json::from_slice::<Value>(&fs::read(&f.gh_state).unwrap()).unwrap();
    github["second_ticket"] = json!(true);
    fs::write(&f.gh_state, serde_json::to_vec(&github).unwrap()).unwrap();
    let configured = f
        .cli()
        .args([
            "configure-worker",
            "--map",
            MAP,
            "--role",
            "implementer",
            "--kind",
            "codex",
            "--model",
            "",
            "--concurrency",
            "1",
        ])
        .output()
        .unwrap();
    success(configured);

    f.apply(RequestKind::Start);

    let state = f.state();
    let held = state
        .workers
        .runs
        .iter()
        .find(|run| run.ticket == 13)
        .unwrap();
    let independent = state
        .workers
        .runs
        .iter()
        .find(|run| run.ticket == 14)
        .unwrap();
    assert_eq!(held.status, WorkerStatus::NeedsHuman);
    assert!(held.known_prelaunch_failure);
    assert!(!held.reserves_capacity());
    assert!(held.claim_login.is_some());
    assert!(
        held.pane_id.is_some(),
        "the verified empty pane remains retained"
    );
    assert!(
        held.question
            .as_deref()
            .unwrap()
            .contains("Provider configuration prevented agent.start")
    );
    assert_eq!(independent.status, WorkerStatus::Running);
    let records = f.herdr_state.lock().unwrap();
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.start")
            .count(),
        1
    );
    drop(records);

    success(f.once());
    let records = f.herdr_state.lock().unwrap();
    assert_eq!(
        records
            .requests
            .iter()
            .filter(|r| r["method"] == "agent.start")
            .count(),
        1
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
fn acknowledged_initial_prompt_reconnects_only_the_same_process_without_replay() {
    for replace_process in [false, true] {
        let f = Fixture::new();
        f.apply(RequestKind::Start);
        let mut state = f.state();
        let run = &mut state.workers.runs[0];
        run.initial_prompt_acknowledged = true;
        run.initial_prompt_pending = false;
        run.initial_prompt_reconnect_pending = true;
        run.agent_session = None;
        if replace_process {
            let process = run.foreground_process.as_mut().unwrap();
            process.pid = process.pid.saturating_add(1);
            process.start_time_ticks = process.start_time_ticks.saturating_add(1);
        }
        store::atomic_json(&f.dir.join("state.json"), &state).unwrap();
        store::enqueue(&f.dir, RequestKind::Reconcile).unwrap();

        success(f.once());
        let recovered = f.state().workers.runs[0].clone();
        if replace_process {
            assert_eq!(recovered.status, WorkerStatus::Uncertain);
            assert!(recovered.status.reserves_capacity());
            assert!(recovered.initial_prompt_acknowledged);
            assert!(recovered.initial_prompt_reconnect_pending);
            assert!(recovered.agent_session.is_none());
        } else {
            assert_eq!(recovered.status, WorkerStatus::Running);
            assert!(!recovered.initial_prompt_acknowledged);
            assert!(!recovered.initial_prompt_reconnect_pending);
            assert!(recovered.agent_session.is_some());
        }
        assert_eq!(
            f.herdr_state
                .lock()
                .unwrap()
                .requests
                .iter()
                .filter(|request| request["method"] == "agent.prompt")
                .count(),
            1,
            "an acknowledged initial prompt is never replayed"
        );
    }
}

#[test]
fn acknowledged_prompt_does_not_override_ambiguous_stop_or_answer_holds() {
    let stopping = Fixture::new();
    stopping.apply(RequestKind::Start);
    let mut state = stopping.state();
    state.workers.runs[0].initial_prompt_acknowledged = true;
    state.workers.runs[0].initial_prompt_reconnect_pending = false;
    state.workers.runs[0].agent_session = None;
    store::atomic_json(&stopping.dir.join("state.json"), &state).unwrap();
    stopping
        .herdr_state
        .lock()
        .unwrap()
        .ambiguous_close_response = true;
    let run = state.workers.runs[0].clone();
    let stopped = stopping
        .cli()
        .args(["stop-worker", "--map", MAP, "--run", &run.id])
        .output()
        .unwrap();
    assert!(!stopped.status.success());
    assert_eq!(
        stopping.state().workers.runs[0].status,
        WorkerStatus::Uncertain
    );
    success(stopping.once());
    assert_eq!(
        stopping.state().workers.runs[0].status,
        WorkerStatus::Uncertain
    );
    assert_eq!(
        stopping
            .herdr_state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|request| request["method"] == "pane.close")
            .count(),
        1
    );

    let answering = Fixture::new();
    answering.apply(RequestKind::Start);
    let original = answering.state().workers.runs[0].clone();
    write_worker_result(
        &original,
        json!({
            "format_version":1,
            "run_id":original.id,
            "ticket":original.ticket,
            "role":original.role,
            "status":"blocked",
            "summary":"needs a human decision",
            "question":"Which option should I use?"
        }),
    );
    answering.herdr_state.lock().unwrap().agent_status = "idle".into();
    success(answering.once());
    let mut state = answering.state();
    let pending = state.workers.runs[0].clone();
    assert_eq!(
        pending.human_request_kind,
        Some(store::HumanRequestKind::WorkerQuestion)
    );
    state.workers.runs[0].initial_prompt_acknowledged = true;
    state.workers.runs[0].initial_prompt_reconnect_pending = false;
    state.workers.runs[0].agent_session = None;
    store::atomic_json(&answering.dir.join("state.json"), &state).unwrap();
    answering
        .herdr_state
        .lock()
        .unwrap()
        .ambiguous_prompt_response = true;

    let response = answer_cli(&answering, &pending, "worker_question", "Choose option A.");
    assert!(!response.status.success());
    let uncertain = answering.state().workers.runs[0].clone();
    assert_eq!(uncertain.status, WorkerStatus::Uncertain);
    assert_eq!(
        uncertain.answer_history[0].disposition,
        store::AnswerDisposition::Uncertain
    );
    success(answering.once());
    assert_eq!(
        answering.state().workers.runs[0].status,
        WorkerStatus::Uncertain
    );
    assert_eq!(
        answering
            .herdr_state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|request| request["method"] == "agent.prompt")
            .count(),
        2,
        "the acknowledged launch and ambiguous answer are each sent once"
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
            rework_round_limit: store::DEFAULT_REWORK_ROUNDS,
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
            initial_prompt_acknowledged: false,
            initial_prompt_reconnect_pending: false,
            known_prelaunch_failure: false,
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
    for expected_round in 1..=4 {
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
                "verdict":"changes_requested",
                "unresolved_findings":["Required behavior is incomplete."],
                "known_limitations":[]
            }),
        );
        f.herdr_state.lock().unwrap().agent_status = "idle".into();
        success(f.once());
        let state = f.state();
        if expected_round < 4 {
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
        } else {
            let decision = state.scheduler_decisions.last().unwrap();
            assert_eq!(
                decision.request_kind,
                store::HumanRequestKind::SchedulerDecision
            );
            assert_eq!(decision.ticket, reviewer.ticket);
            assert_eq!(decision.run_id, reviewer.id);
            assert!(decision.response.is_none());
            assert!(
                decision
                    .question
                    .contains("Three automatic review/rework rounds")
            );
            assert_eq!(
                state.workers.runs.last().unwrap().status,
                WorkerStatus::Completed,
                "a proven-completed reviewer does not reserve capacity while its ticket awaits a decision"
            );
            assert!(
                state
                    .workers
                    .runs
                    .last()
                    .unwrap()
                    .question
                    .as_deref()
                    .unwrap()
                    .contains("Choose one explicit action")
            );
        }
    }
    let decision = f.state().scheduler_decisions.last().unwrap().clone();
    let launch_count = |requests: &[Value]| {
        requests
            .iter()
            .filter(|request| {
                matches!(
                    request["method"].as_str(),
                    Some("worktree.open" | "agent.start" | "agent.prompt")
                )
            })
            .count()
    };
    let request_before = launch_count(&f.herdr_state.lock().unwrap().requests);
    let output = f
        .cli()
        .args([
            "answer-decision",
            "--map",
            MAP,
            "--request-id",
            &decision.request_id,
            "--response",
            "Please make one bounded correction for the required finding.",
            "--disposition",
            "continue",
        ])
        .output()
        .unwrap();
    success(output);
    // The command records the actual answer. Runtime reconciliation dispatches
    // the continuation and does not prompt the completed reviewer.
    success(f.once());
    let after_restart = f.state();
    let continued = &after_restart.scheduler_decisions[0];
    assert_eq!(
        continued.application,
        store::SchedulerDecisionApplication::Continued
    );
    let successor_id = continued.successor_run_id.as_deref().unwrap();
    let successor = after_restart
        .workers
        .runs
        .iter()
        .find(|run| run.id == successor_id)
        .unwrap();
    assert_eq!(successor.role, "implementer");
    assert_eq!(successor.rework_round, 4);
    assert_eq!(successor.rework_round_limit, 4);
    assert_eq!(successor.status, WorkerStatus::Running);
    assert!(successor.context.as_deref().unwrap().contains(
        "Human's exact response: Please make one bounded correction for the required finding."
    ));
    assert_eq!(
        successor.source_run.as_deref(),
        Some(decision.run_id.as_str())
    );
    let launches = launch_count(&f.herdr_state.lock().unwrap().requests);
    assert!(launches > request_before);
    success(f.once());
    let replayed = f.state();
    assert_eq!(
        replayed
            .workers
            .runs
            .iter()
            .filter(|run| {
                run.context.as_deref().is_some_and(|context| {
                    context.contains(&format!("scheduler-decision:{}", decision.request_id))
                })
            })
            .count(),
        1,
        "replay after restart must not create another implementation worker"
    );
    assert_eq!(
        launch_count(&f.herdr_state.lock().unwrap().requests),
        launches,
        "replay may inspect the worker but must not launch or prompt a duplicate"
    );
}

#[test]
fn deferred_scheduler_decision_for_finished_worker_does_not_block_capacity() {
    let f = Fixture::new();
    let mut github = serde_json::from_slice::<Value>(&fs::read(&f.gh_state).unwrap()).unwrap();
    github["assigned"] = json!(true);
    github["second_ticket"] = json!(true);
    fs::write(&f.gh_state, serde_json::to_vec(&github).unwrap()).unwrap();

    let completed = insert_uncertain_run(&f);
    let mut state = f.state();
    state.authorization = wayfinder_herdr::store::Authorization::Started;
    state.concurrency = 1;
    state.workers.runs[0].status = WorkerStatus::Completed;
    state.workers.runs[0].rework_round = 3;
    state.workers.runs[0].base_commit = Some("ticket-13-commit".into());
    let request_id = store::create_scheduler_decision(
        &mut state,
        store::NewSchedulerDecision {
            ticket: 13,
            run_id: &completed.id,
            source_run_id: &completed.id,
            kind: store::SchedulerDecisionKind::ConflictExhaustion,
            blocked_status: WorkerStatus::Completed,
            base_commit: Some("feature-target"),
            question: "Choose how to handle the exhausted conflict repair.",
        },
    )
    .unwrap();
    store::record_scheduler_decision_response(
        &mut state,
        &request_id,
        "I need to defer this ticket.",
        store::SchedulerDecisionDisposition::Defer,
    )
    .unwrap();
    store::atomic_json(&f.dir.join("state.json"), &state).unwrap();

    success(f.once());
    let after = f.state();
    assert_eq!(
        after.scheduler_decisions[0].application,
        store::SchedulerDecisionApplication::Deferred
    );
    let blocked = after
        .workers
        .runs
        .iter()
        .find(|run| run.id == completed.id)
        .unwrap();
    assert_eq!(blocked.status, WorkerStatus::Completed);
    assert!(!blocked.reserves_capacity());
    let independent = after
        .workers
        .runs
        .iter()
        .find(|run| run.ticket == 14)
        .unwrap();
    assert_eq!(independent.status, WorkerStatus::Running);
    assert_eq!(
        after
            .workers
            .runs
            .iter()
            .filter(|run| run.reserves_capacity())
            .count(),
        1,
        "the deferred decision holds ticket readiness without reserving an agent slot"
    );
}

#[test]
fn scheduler_decision_response_is_durable_and_never_prompts_a_worker() {
    let f = Fixture::new_without_session();
    let mut state = f.state();
    let request_id = store::create_scheduler_decision(
        &mut state,
        store::NewSchedulerDecision {
            ticket: 13,
            run_id: "run-completed-worker",
            source_run_id: "run-completed-worker",
            kind: store::SchedulerDecisionKind::ConflictExhaustion,
            blocked_status: WorkerStatus::Completed,
            base_commit: Some("feature-target"),
            question: "Choose how to resolve the exhausted review findings.",
        },
    )
    .unwrap();
    store::atomic_json(&f.dir.join("state.json"), &state).unwrap();
    let requests_before = f.herdr_state.lock().unwrap().requests.len();

    let output = f
        .cli()
        .args([
            "answer-decision",
            "--map",
            MAP,
            "--request-id",
            &request_id,
            "--disposition",
            "defer",
            "--response",
            "Keep the limitation and document the fallback.",
        ])
        .output()
        .unwrap();
    success(output);
    let answered = f.state().scheduler_decisions.pop().unwrap();
    assert_eq!(answered.request_id, request_id);
    assert_eq!(
        answered.response.as_deref(),
        Some("Keep the limitation and document the fallback.")
    );
    assert_eq!(
        answered.disposition,
        Some(store::SchedulerDecisionDisposition::Defer)
    );
    assert_eq!(
        answered.application,
        store::SchedulerDecisionApplication::Deferred,
        "defer keeps ticket readiness held until a later explicit decision is reconciled"
    );
    assert_eq!(answered.answers.len(), 1);
    let repeat = f
        .cli()
        .args([
            "answer-decision",
            "--map",
            MAP,
            "--request-id",
            &request_id,
            "--disposition",
            "defer",
            "--response",
            "Keep the limitation and document the fallback.",
        ])
        .output()
        .unwrap();
    success(repeat);
    assert_eq!(f.state().scheduler_decisions[0].answers.len(), 1);
    let continue_answer = f
        .cli()
        .args([
            "answer-decision",
            "--map",
            MAP,
            "--request-id",
            &request_id,
            "--disposition",
            "continue",
            "--response",
            "I choose one bounded continuation.",
        ])
        .output()
        .unwrap();
    success(continue_answer);
    assert_eq!(f.state().scheduler_decisions[0].answers.len(), 2);
    assert_eq!(
        f.state().scheduler_decisions[0].response.as_deref(),
        Some("I choose one bounded continuation.")
    );
    let repeated_continue = f
        .cli()
        .args([
            "answer-decision",
            "--map",
            MAP,
            "--request-id",
            &request_id,
            "--disposition",
            "continue",
            "--response",
            "I choose one bounded continuation.",
        ])
        .output()
        .unwrap();
    success(repeated_continue);
    assert_eq!(f.state().scheduler_decisions[0].answers.len(), 2);
    let invalid_action = f
        .cli()
        .args([
            "answer-decision",
            "--map",
            MAP,
            "--request-id",
            &request_id,
            "--disposition",
            "retry",
            "--response",
            "Retry it.",
        ])
        .output()
        .unwrap();
    assert!(!invalid_action.status.success());
    let old_request = f
        .cli()
        .args([
            "answer-decision",
            "--map",
            MAP,
            "--request-id",
            "scheduler-obsolete-request",
            "--disposition",
            "abandon",
            "--response",
            "Abandon it.",
        ])
        .output()
        .unwrap();
    assert!(!old_request.status.success());
    assert_eq!(
        f.herdr_state.lock().unwrap().requests.len(),
        requests_before
    );
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
            "verdict":"approved",
            "unresolved_findings":[],
            "known_limitations":[]
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
        rework_round_limit: store::DEFAULT_REWORK_ROUNDS,
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
        initial_prompt_acknowledged: false,
        initial_prompt_reconnect_pending: false,
        known_prelaunch_failure: false,
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
