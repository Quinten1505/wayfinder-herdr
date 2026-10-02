use serde_json::json;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};
use tempfile::TempDir;
use wayfinder_herdr::{
    delivery,
    store::{self, Binding},
};

fn git(path: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {}: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

struct Fixture {
    _temp: TempDir,
    root: PathBuf,
    source: PathBuf,
    feature: PathBuf,
    feature_head: String,
    gh_bin: PathBuf,
    pr_json: PathBuf,
    dir: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("state");
        let source = temp.path().join("source");
        fs::create_dir(&source).unwrap();
        git(&source, &["init", "-b", "develop"]);
        git(&source, &["config", "user.name", "Wayfinder Fixture"]);
        git(&source, &["config", "user.email", "test@example.invalid"]);
        fs::write(source.join("tracked.txt"), "base\n").unwrap();
        git(&source, &["add", "tracked.txt"]);
        git(&source, &["commit", "-m", "base"]);
        git(&source, &["branch", "feature/bound"]);
        let origin = temp.path().join("origin.git");
        fs::create_dir(&origin).unwrap();
        git(&origin, &["init", "--bare"]);
        git(
            &source,
            &["remote", "add", "origin", origin.to_str().unwrap()],
        );
        git(&source, &["push", "origin", "develop"]);
        git(&source, &["fetch", "origin", "develop"]);
        let feature = temp.path().join("feature");
        git(
            &source,
            &[
                "worktree",
                "add",
                feature.to_str().unwrap(),
                "feature/bound",
            ],
        );
        git(&feature, &["push", "origin", "feature/bound"]);
        fs::write(source.join("human-note.txt"), "keep me\n").unwrap();
        let feature_head = git(&feature, &["rev-parse", "HEAD"]);
        let gh_bin = temp.path().join("fake-bin");
        fs::create_dir(&gh_bin).unwrap();
        let pr_json = temp.path().join("pr.json");
        fs::write(
            &pr_json,
            serde_json::to_vec(&json!({
                "headRefName":"feature/bound","headRefOid":feature_head,
                "baseRefName":"develop","isDraft":true,
                "body":"Tracks https://github.com/example/project/issues/42"
            }))
            .unwrap(),
        )
        .unwrap();
        let gh = gh_bin.join("gh");
        fs::write(&gh, format!("#!/bin/sh\ncat '{}'\n", pr_json.display())).unwrap();
        let mut permissions = fs::metadata(&gh).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&gh, permissions).unwrap();
        let binding = Binding {
            repository: source.clone(),
            herdr_binary: PathBuf::from("/bin/true"),
            socket: temp.path().join("herdr.sock"),
            herdr_config: None,
            source_workspace_id: Some("w1".into()),
        };
        let (key, _) = store::attach(&root, "example/project#42", binding, 30).unwrap();
        let dir = store::map_dir(&root, &key).unwrap();
        Self {
            _temp: temp,
            root,
            source,
            feature,
            feature_head,
            gh_bin,
            pr_json,
            dir,
        }
    }

    fn bind(&self, checkout: &Path) -> std::process::Output {
        self.bind_with(
            checkout,
            "feature/bound",
            &self.feature_head,
            "refs/remotes/origin/develop",
        )
    }

    fn bind_with(
        &self,
        checkout: &Path,
        branch: &str,
        head: &str,
        base_ref: &str,
    ) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_wayfinder-herdr"))
            .arg("--state-dir")
            .arg(&self.root)
            .args([
                "bind-feature-checkout",
                "--map",
                "example/project#42",
                "--checkout",
            ])
            .arg(checkout)
            .args([
                "--branch",
                branch,
                "--head",
                head,
                "--base-ref",
                base_ref,
                "--source-pr",
                "44",
            ])
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    self.gh_bin.display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            )
            .output()
            .unwrap()
    }
}

#[test]
fn command_holds_dirty_checkout_and_wrong_map_scoped_identity() {
    let f = Fixture::new();
    fs::write(f.feature.join("untracked.txt"), "retain me\n").unwrap();
    let dirty = f.bind(&f.feature);
    assert!(!dirty.status.success());
    assert!(String::from_utf8_lossy(&dirty.stderr).contains("local changes"));
    assert!(delivery::read(&f.dir).unwrap().feature_checkout.is_none());
    assert_eq!(
        fs::read_to_string(f.feature.join("untracked.txt")).unwrap(),
        "retain me\n"
    );
    fs::remove_file(f.feature.join("untracked.txt")).unwrap();

    let wrong_branch = f.bind_with(
        &f.feature,
        "feature/another-map",
        &f.feature_head,
        "refs/remotes/origin/develop",
    );
    assert!(!wrong_branch.status.success());
    assert!(String::from_utf8_lossy(&wrong_branch.stderr).contains("changed branch"));

    let wrong_base = f.bind_with(
        &f.feature,
        "feature/bound",
        &f.feature_head,
        "refs/heads/develop",
    );
    assert!(!wrong_base.status.success());
    assert!(String::from_utf8_lossy(&wrong_base.stderr).contains("origin/develop"));
    assert!(delivery::read(&f.dir).unwrap().feature_checkout.is_none());
}

#[test]
fn command_holds_when_saved_target_ref_is_stale() {
    let f = Fixture::new();
    let target = f._temp.path().join("new-target");
    git(
        &f.source,
        &[
            "worktree",
            "add",
            "--detach",
            target.to_str().unwrap(),
            &f.feature_head,
        ],
    );
    fs::write(target.join("target.txt"), "new target\n").unwrap();
    git(&target, &["add", "target.txt"]);
    git(&target, &["commit", "-m", "advance remote target"]);
    git(&target, &["push", "origin", "HEAD:develop"]);
    git(
        &f.source,
        &["update-ref", "refs/remotes/origin/develop", &f.feature_head],
    );
    let result = f.bind(&f.feature);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("current origin target"));
    assert!(delivery::read(&f.dir).unwrap().feature_checkout.is_none());
}

#[test]
fn command_rejects_draft_pr_for_another_map() {
    let f = Fixture::new();
    fs::write(
        &f.pr_json,
        serde_json::to_vec(&json!({
            "headRefName":"feature/bound","headRefOid":f.feature_head,
            "baseRefName":"develop","isDraft":true,
            "body":"Tracks https://github.com/example/project/issues/420"
        }))
        .unwrap(),
    )
    .unwrap();
    let result = f.bind(&f.feature);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("does not prove this map"));
    assert!(delivery::read(&f.dir).unwrap().feature_checkout.is_none());
}

#[test]
fn command_rejects_conflicting_recorded_pr_identity() {
    let f = Fixture::new();
    let mut delivery = delivery::read(&f.dir).unwrap();
    delivery.draft_pr = Some(delivery::PullRequest {
        number: 99,
        url: "https://github.com/example/project/pull/99".into(),
        head: "feature/bound".into(),
        base: "develop".into(),
        draft: true,
    });
    store::atomic_json(&f.dir.join("delivery.json"), &delivery).unwrap();
    let result = f.bind(&f.feature);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("recorded draft PR"));
    let saved = delivery::read(&f.dir).unwrap();
    assert_eq!(saved.draft_pr.unwrap().number, 99);
    assert!(saved.feature_checkout.is_none());
}

#[test]
fn command_binds_one_exact_checkout_without_touching_dirty_source() {
    let f = Fixture::new();
    let source_head = git(&f.source, &["rev-parse", "HEAD"]);
    let feature_head = git(&f.feature, &["rev-parse", "HEAD"]);
    let first = f.bind(&f.feature);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(f.bind(&f.feature).status.success());
    let saved = delivery::read(&f.dir).unwrap();
    assert_eq!(saved.feature_checkout.as_deref(), Some(f.feature.as_path()));
    assert_eq!(saved.feature_branch.as_deref(), Some("feature/bound"));
    assert_eq!(saved.feature_head.as_deref(), Some(feature_head.as_str()));
    assert_eq!(
        saved.feature_base_commit.as_deref(),
        Some(source_head.as_str())
    );
    assert_eq!(git(&f.source, &["branch", "--show-current"]), "develop");
    assert_eq!(
        fs::read_to_string(f.source.join("human-note.txt")).unwrap(),
        "keep me\n"
    );

    fs::write(f.feature.join("external.txt"), "changed\n").unwrap();
    git(&f.feature, &["add", "external.txt"]);
    git(&f.feature, &["commit", "-m", "external move"]);
    let changed = f.bind(&f.feature);
    assert!(!changed.status.success());
    assert!(String::from_utf8_lossy(&changed.stderr).contains("expected commit"));
    assert_eq!(
        delivery::read(&f.dir).unwrap().feature_head.as_deref(),
        Some(feature_head.as_str())
    );
}

#[test]
fn command_rejects_another_repository_worktree() {
    let f = Fixture::new();
    let other = f._temp.path().join("other");
    fs::create_dir(&other).unwrap();
    git(&other, &["init", "-b", "feature/other"]);
    let result = f.bind(&other);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("another Git repository"));
    assert!(delivery::read(&f.dir).unwrap().feature_checkout.is_none());
}

#[test]
fn command_binding_survives_restart_and_integrates_in_separate_checkout() {
    let f = Fixture::new();
    let source_head = git(&f.source, &["rev-parse", "HEAD"]);
    let implementer = f._temp.path().join("implementer");
    git(
        &f.source,
        &[
            "worktree",
            "add",
            "--detach",
            implementer.to_str().unwrap(),
            &f.feature_head,
        ],
    );
    fs::create_dir(implementer.join("src")).unwrap();
    fs::write(
        implementer.join("Cargo.toml"),
        "[package]\nname = \"binding-fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    fs::write(
        implementer.join("src/lib.rs"),
        "pub fn value() -> u8 {\n    1\n}\n",
    )
    .unwrap();
    fs::write(
        implementer.join("Cargo.lock"),
        "version = 4\n\n[[package]]\nname = \"binding-fixture\"\nversion = \"0.1.0\"\n",
    )
    .unwrap();
    git(&implementer, &["add", "Cargo.toml", "Cargo.lock", "src"]);
    git(
        &implementer,
        &["commit", "-m", "reviewed fixture candidate"],
    );
    let candidate = git(&implementer, &["rev-parse", "HEAD"]);
    let reviewer = f._temp.path().join("reviewer");
    git(
        &f.source,
        &[
            "worktree",
            "add",
            "--detach",
            reviewer.to_str().unwrap(),
            &candidate,
        ],
    );
    let evidence = f._temp.path().join("review.json");
    fs::write(
        &evidence,
        serde_json::to_vec(&json!({
            "format_version":1,"run_id":"run-00000000000000000002","ticket":15,
            "role":"reviewer","status":"completed","summary":"approved",
            "reviewed_commit":candidate,"verdict":"approved",
            "unresolved_findings":[],"known_limitations":[]
        }))
        .unwrap(),
    )
    .unwrap();
    let mut state = store::read_state(&f.dir).unwrap();
    state.workers = serde_json::from_value(json!({
        "next_run":2,"providers":{},"runs":[
            {"id":"run-00000000000000000001","ticket":15,"role":"implementer",
             "attempt":1,"rework_round":0,"status":"reviewed","worktree":implementer,
             "base_commit":f.feature_head,"result_commit":candidate,"summary":"implemented",
             "claim_login":"fixture"},
            {"id":"run-00000000000000000002","ticket":15,"role":"reviewer",
             "attempt":1,"rework_round":0,"status":"completed","worktree":reviewer,
             "base_commit":candidate,"summary":"approved",
             "source_run":"run-00000000000000000001","claim_login":"fixture",
             "result_evidence":evidence}
        ]
    }))
    .unwrap();
    store::atomic_json(&f.dir.join("state.json"), &state).unwrap();

    let bound = f.bind(&f.feature);
    assert!(
        bound.status.success(),
        "{}",
        String::from_utf8_lossy(&bound.stderr)
    );
    let restarted = store::read_state(&f.dir).unwrap();
    let outcome = delivery::reconcile(&f.dir, &restarted, &[]).unwrap();
    assert!(
        matches!(outcome, delivery::Outcome::Integrated { .. }),
        "{outcome:?}"
    );
    assert_eq!(git(&f.feature, &["rev-parse", "HEAD"]), candidate);
    assert_eq!(
        git(&f.feature, &["rev-parse", "refs/heads/feature/bound"]),
        candidate
    );
    assert!(f.feature.join("Cargo.toml").exists());
    assert_eq!(git(&f.source, &["branch", "--show-current"]), "develop");
    assert_eq!(git(&f.source, &["rev-parse", "HEAD"]), source_head);
    assert_eq!(
        fs::read_to_string(f.source.join("human-note.txt")).unwrap(),
        "keep me\n"
    );
}
