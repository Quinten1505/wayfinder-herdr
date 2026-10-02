use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Command, Output},
};
use tempfile::TempDir;
use wayfinder_herdr::{
    store::{self, Binding},
    tracker::MapRef,
};

const MAP: &str = "Example/Project#1";

struct Fixture {
    _temp: TempDir,
    root: PathBuf,
    dir: PathBuf,
    gh: PathBuf,
    data: PathBuf,
}
impl Fixture {
    fn new(execution_override: bool) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("state");
        fs::create_dir_all(&root).unwrap();
        let gh = temp.path().join("gh");
        fs::write(&gh, FAKE_GH).unwrap();
        fs::set_permissions(&gh, fs::Permissions::from_mode(0o755)).unwrap();
        let data = temp.path().join("github.json");
        let notes = if execution_override {
            "## Notes\n\nExecution override: selected by the user."
        } else {
            "## Notes\n\nPlanning mode."
        };
        let initial = json!({
            "next": 20,
            "etag": 1,
            "me": "quinten",
            "issues": {
                "1": issue(1, 101, "Map", "open", &format!("## Destination\n\nDestination\n\n{notes}\n\n## Decisions so far\n\nexisting decision\n\n## Not yet specified\n\nkeep planning\n\n## Evidence and open design\n\nexisting evidence\n\n## Out of scope\n\nkeep scope"), vec![]),
                "2": issue(2, 102, "Claimed", "open", "", vec!["alice"]),
                "3": issue(3, 103, "Ready", "open", "", vec![]),
                "4": issue(4, 104, "Resolved blocker", "closed", "", vec![]),
                "5": issue(5, 105, "Blocked", "open", "", vec![]),
                "6": issue(6, 106, "Blocker", "open", "", vec![]),
                "7": issue(7, 107, "Externally closed", "closed", "", vec![]),
                "10": issue(10, 110, "Spec", "open", "## Evidence and open design\n\nexisting spec evidence", vec![])
            },
            "subissues": {"1": [2, 3, 4, 5, 6, 7]},
            "blockers": {"3": [4], "5": [6]},
            "comments": {},
            "fail_after_comment_once": false
        });
        fs::write(&data, serde_json::to_vec_pretty(&initial).unwrap()).unwrap();
        let herdr = temp.path().join("herdr");
        fs::write(&herdr, "#!/bin/sh\ncase \"$1\" in\n --version) echo 'herdr 0.9.3';;\n status) echo '{\"running\":true,\"version\":\"0.9.3\",\"protocol\":22,\"compatible\":true,\"endpoint_compatible\":true,\"socket\":\"/tmp/session.sock\"}';;\n plugin) echo '{\"result\":{\"plugins\":[{\"plugin_id\":\"wayfinder.herdr\",\"enabled\":true}]}}';;\n *) exit 9;;\nesac\n").unwrap();
        fs::set_permissions(&herdr, fs::Permissions::from_mode(0o755)).unwrap();
        let repository = temp.path().join("repo");
        fs::create_dir_all(&repository).unwrap();
        let binding = Binding {
            repository,
            socket: "/tmp/session.sock".into(),
            herdr_binary: herdr,
            herdr_config: None,
            source_workspace_id: Some("workspace-parent".into()),
        };
        let (key, _) = store::attach(&root, MAP, binding.clone(), 1).unwrap();
        let dir = store::map_dir(&root, &key).unwrap();
        Self {
            _temp: temp,
            root,
            dir,
            gh,
            data,
        }
    }
    fn cli(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_wayfinder-herdr"));
        command
            .arg("--state-dir")
            .arg(&self.root)
            .env("GH_BIN_PATH", &self.gh)
            .env("WAYFINDER_TEST_GITHUB_DATA", &self.data);
        command
    }
    fn api(&self, args: &[&str]) -> Output {
        self.cli().args(args).output().unwrap()
    }
    fn github(&self) -> Value {
        serde_json::from_slice(&fs::read(&self.data).unwrap()).unwrap()
    }
    fn set_github(&self, data: Value) {
        fs::write(&self.data, serde_json::to_vec_pretty(&data).unwrap()).unwrap();
    }
    fn success(&self, args: &[&str]) -> Output {
        let output = self.api(args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }
}
fn issue(
    number: u64,
    id: u64,
    title: &str,
    state: &str,
    body: &str,
    assignees: Vec<&str>,
) -> Value {
    json!({"number":number,"id":id,"title":title,"state":state,"body":body,"assignees":assignees.into_iter().map(|login| json!({"login":login})).collect::<Vec<_>>()})
}

const FAKE_GH: &str = r##"#!/usr/bin/env python3
import json, os, sys
from urllib.parse import urlsplit
args=sys.argv[1:]
data_path=os.environ['WAYFINDER_TEST_GITHUB_DATA']
with open(data_path) as f: db=json.load(f)
method='GET'
if '--method' in args:
    method=args[args.index('--method')+1].upper()
path=next((a for a in args if a.startswith('repos/') or a == 'user'), None)
if path is None: sys.exit('missing path')
body={}
if '--input' in args:
    body=json.load(sys.stdin)
parts=urlsplit(path)
segments=parts.path.split('/')
result=None
changed=False
if path == 'user':
    result={'login':db['me']}
elif method == 'GET' and len(segments)==4 and segments[3]=='issues':
    result=list(db['issues'].values())
elif method == 'POST' and len(segments)==4 and segments[3]=='issues':
    number=db['next']; db['next']+=1; issue_id=100+number
    result={'number':number,'id':issue_id,'title':body['title'],'state':'open','body':body['body'],'assignees':[]}
    db['issues'][str(number)]=result
    changed=True
elif len(segments)>=5 and segments[3]=='issues':
    parent=int(segments[4])
    if method=='GET' and len(segments)==6 and segments[5]=='sub_issues':
        result=[db['issues'][str(i)] for i in db['subissues'].get(str(parent),[])]
    elif method=='GET' and len(segments)==7 and segments[5]=='dependencies' and segments[6]=='blocked_by':
        result=[db['issues'][str(i)] for i in db['blockers'].get(str(parent),[])]
    elif method=='GET' and len(segments)==6 and segments[5]=='comments':
        result=db['comments'].get(str(parent),[])
    elif method=='POST' and len(segments)==6 and segments[5]=='sub_issues':
        number=next(int(k) for k,v in db['issues'].items() if v['id']==body['sub_issue_id'])
        values=db['subissues'].setdefault(str(parent),[])
        if number not in values: values.append(number)
        result={'number':number}
        changed=True
    elif method=='POST' and len(segments)==7 and segments[5]=='dependencies':
        blocker=next(int(k) for k,v in db['issues'].items() if v['id']==body['issue_id'])
        values=db['blockers'].setdefault(str(parent),[])
        if blocker not in values: values.append(blocker)
        result={'number':blocker}
        changed=True
    elif method=='POST' and len(segments)==6 and segments[5]=='comments':
        comments=db['comments'].setdefault(str(parent),[])
        result={'id':len(comments)+1,'body':body['body']}
        comments.append(result)
        changed=True
        fail_map = db.get('fail_after_comment_issue') == parent
        if fail_map: del db['fail_after_comment_issue']
        if db.get('fail_after_comment_once') or fail_map:
            db['fail_after_comment_once']=False
            with open(data_path,'w') as f: json.dump(db,f)
            sys.stderr.write('simulated lost response after comment write\n'); sys.exit(1)
    elif method=='POST' and len(segments)==6 and segments[5]=='assignees':
        target=db['issues'][str(parent)]
        db['claim_write_count']=db.get('claim_write_count',0)+1
        if db.get('claim_no_persist') or db.get('claim_error_no_persist'):
            fail=db.pop('claim_error_no_persist',False)
            db.pop('claim_no_persist',None)
            result=target
            with open(data_path,'w') as f: json.dump(db,f)
            if fail:
                sys.stderr.write('simulated assignment write error without persistence\n'); sys.exit(1)
            # Simulate a successful API response that did not persist the claim.
            changed=False
            target=None
        else:
            race=db.get('claim_race_to')
            if race:
                target['assignees']=[{'login':race}]
                del db['claim_race_to']
            for login in body['assignees']:
                if not any(a['login']==login for a in target['assignees']): target['assignees'].append({'login':login})
            if db.get('close_race_on_claim'):
                target['state']='closed'
                del db['close_race_on_claim']
            result=target
            changed=True
            if db.get('fail_claim_postcheck_read'):
                db['fail_claim_postcheck_read']=False
                db['fail_issue_read_ticket']=parent
    elif method=='GET' and len(segments)==5:
        if db.get('fail_issue_read_ticket') == parent:
            db['fail_issue_read_ticket']=None
            with open(data_path,'w') as f: json.dump(db,f)
            sys.stderr.write('simulated unreadable issue read\n'); sys.exit(1)
        result=db['issues'][str(parent)]
    elif method=='PATCH' and len(segments)==5:
        current_etag='"%s"' % db['etag']
        match=next((a.split(':',1)[1].strip() for a in args if a.lower().startswith('if-match:')),None)
        if match is not None and match != current_etag:
            sys.stderr.write('HTTP 412 precondition failed\n'); sys.exit(1)
        target=db['issues'][str(parent)]
        for key,value in body.items():
            if key=='body': db['body_patch_count']=db.get('body_patch_count',0)+1; target[key]=value
            elif key=='assignees': target[key]=[{'login':v} for v in value]
            else: target[key]=value
        result=target
        changed=True
else:
    sys.stderr.write('unhandled %s %s %r\n' % (method,path,segments)); sys.exit(2)
if changed: db['etag']+=1
with open(data_path,'w') as f: json.dump(db,f)
encoded=json.dumps(result)
if '--slurp' in args:
    encoded=json.dumps([result])
if '--include' in args:
    sys.stdout.write('HTTP/2 200\r\nETag: "%s"\r\n\r\n%s' % (db['etag'],encoded))
else:
    sys.stdout.write(encoded)
"##;

#[test]
fn frontier_uses_native_order_and_excludes_closed_claimed_and_open_blocked_tickets() {
    let f = Fixture::new(false);
    let output = f.success(&["tracker", "frontier", "--map", MAP]);
    let frontier: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        frontier
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["number"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![3, 6]
    );
}

#[test]
fn planning_operations_work_without_override_and_resolution_replay_is_idempotent() {
    let f = Fixture::new(false);
    f.success(&[
        "tracker",
        "create-ticket",
        "--map",
        MAP,
        "--title",
        "Planning question",
        "--body",
        "Need an answer",
        "--label",
        "wayfinder:research",
    ]);
    let db = f.github();
    assert_eq!(db["subissues"]["1"].as_array().unwrap().last().unwrap(), 20);
    assert_eq!(db["issues"]["20"]["title"], "Planning question");

    f.success(&[
        "tracker", "block", "--map", MAP, "--ticket", "3", "--by", "6",
    ]);
    assert!(
        f.github()["blockers"]["3"]
            .as_array()
            .unwrap()
            .contains(&json!(6))
    );

    let mut db = f.github();
    db["issues"]["1"]["body"] = json!(format!(
        "{}\n\nConcurrent map edit.",
        db["issues"]["1"]["body"].as_str().unwrap()
    ));
    db["issues"]["10"]["body"] = json!(format!(
        "{}\n\nConcurrent spec edit.",
        db["issues"]["10"]["body"].as_str().unwrap()
    ));
    f.set_github(db);
    let map_body_before = f.github()["issues"]["1"]["body"].clone();
    let spec_body_before = f.github()["issues"]["10"]["body"].clone();

    f.success(&[
        "tracker",
        "resolve",
        "--map",
        MAP,
        "--ticket",
        "3",
        "--resolution",
        "The human selected option A.",
        "--spec",
        "10",
    ]);
    f.success(&[
        "tracker",
        "resolve",
        "--map",
        MAP,
        "--ticket",
        "3",
        "--resolution",
        "The human selected option A.",
        "--spec",
        "10",
    ]);
    let db = f.github();
    assert_eq!(db["issues"]["3"]["state"], "closed");
    assert_eq!(db["comments"]["3"].as_array().unwrap().len(), 1);
    assert_eq!(
        db.get("body_patch_count")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        0
    );
    assert_eq!(db["issues"]["1"]["body"], map_body_before);
    assert_eq!(db["issues"]["10"]["body"], spec_body_before);
    assert_eq!(db["comments"]["1"].as_array().unwrap().len(), 1);
    assert_eq!(db["comments"]["10"].as_array().unwrap().len(), 1);
    let map_comment = db["comments"]["1"][0]["body"].as_str().unwrap();
    assert!(map_comment.contains("Decision index pointer"));
    assert!(map_comment.contains("body refresh pending for a human"));
    assert!(map_comment.contains("#3 Ready"));
    let spec_comment = db["comments"]["10"][0]["body"].as_str().unwrap();
    assert!(spec_comment.contains("Proposed specification delta"));
    assert!(spec_comment.contains("body refresh pending for a human"));
    assert!(spec_comment.contains("The human selected option A."));
}

#[test]
fn ambiguous_comment_write_is_found_before_retry_and_external_close_is_not_resolution() {
    let f = Fixture::new(false);
    let mut db = f.github();
    db["fail_after_comment_issue"] = json!(1);
    f.set_github(db);
    f.success(&[
        "tracker",
        "resolve",
        "--map",
        MAP,
        "--ticket",
        "3",
        "--resolution",
        "Human answer",
        "--spec",
        "10",
    ]);
    f.success(&[
        "tracker",
        "resolve",
        "--map",
        MAP,
        "--ticket",
        "3",
        "--resolution",
        "Human answer",
        "--spec",
        "10",
    ]);
    assert_eq!(f.github()["comments"]["1"].as_array().unwrap().len(), 1);
    assert_eq!(f.github()["comments"]["10"].as_array().unwrap().len(), 1);
    assert_eq!(
        f.github()
            .get("body_patch_count")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        0
    );
    assert_eq!(f.github()["comments"]["3"].as_array().unwrap().len(), 1);

    let failed = f.api(&[
        "tracker",
        "resolve",
        "--map",
        MAP,
        "--ticket",
        "7",
        "--resolution",
        "Do not infer",
        "--spec",
        "10",
    ]);
    assert!(!failed.status.success());
    assert!(!f.github()["comments"].get("7").is_some());
}

#[test]
fn execution_override_gates_claims_but_existing_assignment_is_a_claim() {
    let f = Fixture::new(false);
    let denied = f.api(&["tracker", "claim", "--map", MAP, "--ticket", "3"]);
    assert!(!denied.status.success());

    let mut db = f.github();
    db["issues"]["1"]["body"] = json!(
        "## Notes\n\nExecution override: selected by user.\n\n## Decisions so far\n\n## Evidence and open design\n"
    );
    f.set_github(db);
    let conflict = f.api(&[
        "tracker",
        "claim",
        "--map",
        MAP,
        "--ticket",
        "2",
        "--assignee",
        "quinten",
    ]);
    assert!(!conflict.status.success());
    let success = f.success(&["tracker", "claim", "--map", MAP, "--ticket", "3"]);
    assert!(String::from_utf8_lossy(&success.stdout).contains("@quinten"));
    assert_eq!(
        f.github()["issues"]["3"]["assignees"][0]["login"],
        "quinten"
    );
}

#[test]
fn claim_detects_external_reassignment_without_replacing_it() {
    let f = Fixture::new(true);
    let mut db = f.github();
    db["claim_race_to"] = json!("alice");
    f.set_github(db);
    let result = f.api(&["tracker", "claim", "--map", MAP, "--ticket", "3"]);
    assert!(!result.status.success());
    let assignees = f.github()["issues"]["3"]["assignees"]
        .as_array()
        .unwrap()
        .clone();
    assert!(assignees.iter().any(|a| a["login"] == "alice"));
    assert!(assignees.iter().any(|a| a["login"] == "quinten"));
    let intent: Value = serde_json::from_slice(
        &fs::read_dir(f.dir.join("tracker/intents"))
            .unwrap()
            .map(|entry| fs::read(entry.unwrap().path()).unwrap())
            .next()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(intent["stage"], "uncertain-claim");
    let retry = f.api(&["tracker", "claim", "--map", MAP, "--ticket", "3"]);
    assert!(!retry.status.success());
    assert_eq!(f.github()["claim_write_count"], 1);
}

#[test]
fn closed_child_is_rejected_before_claim_and_closure_race_retains_uncertain_intent() {
    let closed = Fixture::new(true);
    let mut db = closed.github();
    db["issues"]["3"]["state"] = json!("closed");
    closed.set_github(db);
    let rejected = closed.api(&["tracker", "claim", "--map", MAP, "--ticket", "3"]);
    assert!(!rejected.status.success());
    assert!(
        closed.github()["issues"]["3"]["assignees"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let intent: Value = serde_json::from_slice(
        &fs::read_dir(closed.dir.join("tracker/intents"))
            .unwrap()
            .map(|entry| fs::read(entry.unwrap().path()).unwrap())
            .next()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(intent["stage"], "conflict-closed");

    let raced = Fixture::new(true);
    let mut db = raced.github();
    db["close_race_on_claim"] = json!(true);
    raced.set_github(db);
    let result = raced.api(&["tracker", "claim", "--map", MAP, "--ticket", "3"]);
    assert!(!result.status.success());
    assert_eq!(raced.github()["issues"]["3"]["state"], "closed");
    assert_eq!(
        raced.github()["issues"]["3"]["assignees"][0]["login"],
        "quinten"
    );
    let intent: Value = serde_json::from_slice(
        &fs::read_dir(raced.dir.join("tracker/intents"))
            .unwrap()
            .map(|entry| fs::read(entry.unwrap().path()).unwrap())
            .next()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(intent["stage"], "uncertain-claim");
}

#[test]
fn claim_without_persisted_assignment_fails_and_keeps_uncertain_intent() {
    for flag in ["claim_no_persist", "claim_error_no_persist"] {
        let f = Fixture::new(true);
        let mut db = f.github();
        db[flag] = json!(true);
        f.set_github(db);

        let result = f.api(&["tracker", "claim", "--map", MAP, "--ticket", "3"]);
        assert!(!result.status.success(), "{flag} was incorrectly accepted");
        assert!(
            f.github()["issues"]["3"]["assignees"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert_eq!(f.github()["claim_write_count"], 1);
        let intent: Value = serde_json::from_slice(
            &fs::read_dir(f.dir.join("tracker/intents"))
                .unwrap()
                .map(|entry| fs::read(entry.unwrap().path()).unwrap())
                .next()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(intent["stage"], "uncertain-claim");
    }
}

#[test]
fn uncertain_claim_can_be_reconciled_from_fresh_owned_github_state_without_reassignment() {
    let f = Fixture::new(true);
    let mut db = f.github();
    db["fail_claim_postcheck_read"] = json!(true);
    f.set_github(db);

    let first = f.api(&["tracker", "claim", "--map", MAP, "--ticket", "3"]);
    assert!(!first.status.success());
    assert_eq!(
        f.github()["issues"]["3"]["assignees"][0]["login"],
        "quinten"
    );
    assert_eq!(f.github()["claim_write_count"], 1);

    let blocked = f.api(&["tracker", "claim", "--map", MAP, "--ticket", "3"]);
    assert!(!blocked.status.success());
    assert_eq!(f.github()["claim_write_count"], 1);

    f.success(&["tracker", "reconcile-claim", "--map", MAP, "--ticket", "3"]);
    let intent: Value = serde_json::from_slice(
        &fs::read_dir(f.dir.join("tracker/intents"))
            .unwrap()
            .map(|entry| fs::read(entry.unwrap().path()).unwrap())
            .next()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(intent["stage"], "reconciled-claim");

    // Runtime's claim path (also used by explicit retry-worker runs) accepts the reconciled
    // ownership proof but never posts another assignment.
    f.success(&["tracker", "claim", "--map", MAP, "--ticket", "3"]);
    f.success(&["tracker", "reconcile-claim", "--map", MAP, "--ticket", "3"]);
    assert_eq!(f.github()["claim_write_count"], 1);
}

#[test]
fn uncertain_claim_reconciliation_keeps_foreign_empty_closed_and_unreadable_states_held() {
    for state in ["foreign", "empty", "closed", "unreadable"] {
        let f = Fixture::new(true);
        let mut db = f.github();
        match state {
            "foreign" => db["claim_race_to"] = json!("alice"),
            "empty" => db["claim_error_no_persist"] = json!(true),
            "closed" => db["close_race_on_claim"] = json!(true),
            "unreadable" => db["fail_claim_postcheck_read"] = json!(true),
            _ => unreachable!(),
        }
        f.set_github(db);
        let first = f.api(&["tracker", "claim", "--map", MAP, "--ticket", "3"]);
        assert!(
            !first.status.success(),
            "{state} setup unexpectedly claimed"
        );
        if state == "foreign" {
            // Model an external reassignment after the ambiguous operation resolved.
            let mut db = f.github();
            db["issues"]["3"]["assignees"] = json!([{"login":"alice"}]);
            f.set_github(db);
        }
        if state == "unreadable" {
            // The first failed read was consumed by the ambiguous postcheck; fail reconciliation.
            let mut db = f.github();
            db["fail_issue_read_ticket"] = json!(3);
            f.set_github(db);
        }
        let before = f.github()["claim_write_count"].as_u64().unwrap();
        let result = f.api(&["tracker", "reconcile-claim", "--map", MAP, "--ticket", "3"]);
        assert!(
            !result.status.success(),
            "{state} was incorrectly reconciled"
        );
        let intent: Value = serde_json::from_slice(
            &fs::read_dir(f.dir.join("tracker/intents"))
                .unwrap()
                .map(|entry| fs::read(entry.unwrap().path()).unwrap())
                .next()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            intent["stage"], "uncertain-claim",
            "{state} cleared uncertainty"
        );
        assert_eq!(
            f.github()["claim_write_count"],
            before,
            "{state} repeated assignment"
        );
    }
}

#[test]
fn planning_map_creation_is_idempotent_and_runtime_persists_frontier() {
    let f = Fixture::new(false);
    for _ in 0..2 {
        f.success(&[
            "tracker",
            "create-map",
            "--repository",
            "Example/Project",
            "--title",
            "Fresh map",
            "--notes",
            "Plan first",
        ]);
    }
    let created = f.github()["issues"]
        .as_object()
        .unwrap()
        .values()
        .filter(|i| i["title"] == "Fresh map")
        .count();
    assert_eq!(created, 1);
    let db = f.github();
    let created_map = db["issues"]
        .as_object()
        .unwrap()
        .values()
        .find(|i| i["title"] == "Fresh map")
        .unwrap();
    assert!(created_map["body"].as_str().unwrap().contains("## Notes"));
    assert!(
        !created_map["body"]
            .as_str()
            .unwrap()
            .contains("Execution override:")
    );

    f.success(&[
        "serve",
        "--key",
        &store::map_identity(MAP).unwrap().1,
        "--once",
    ]);
    let frontier: Value =
        serde_json::from_slice(&fs::read(f.dir.join("tracker/frontier.json")).unwrap()).unwrap();
    assert_eq!(frontier.as_array().unwrap()[0]["number"], 3);
}

#[test]
fn map_and_spec_comments_preserve_existing_issue_bodies() {
    assert_eq!(MapRef::parse(MAP).unwrap().number, 1);
    let f = Fixture::new(false);
    let map_body = f.github()["issues"]["1"]["body"].clone();
    let spec_body = f.github()["issues"]["10"]["body"].clone();
    f.success(&[
        "tracker",
        "resolve",
        "--map",
        MAP,
        "--ticket",
        "3",
        "--resolution",
        "Human chose A",
        "--spec",
        "10",
    ]);
    let db = f.github();
    assert_eq!(db["issues"]["1"]["body"], map_body);
    assert_eq!(db["issues"]["10"]["body"], spec_body);
    assert_eq!(db["comments"]["1"].as_array().unwrap().len(), 1);
    assert_eq!(db["comments"]["10"].as_array().unwrap().len(), 1);
}
