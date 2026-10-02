//! GitHub Issues tracker operations for a single locally-owned map runtime.
use crate::store::{self, Lock};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

#[derive(Debug, Clone)]
pub struct MapRef {
    pub owner: String,
    pub repository: String,
    pub number: u64,
}
impl MapRef {
    pub fn parse(value: &str) -> Result<Self> {
        let (repo, number) = value
            .rsplit_once('#')
            .context("map must use OWNER/REPOSITORY#NUMBER")?;
        let (owner, repository) = repo
            .split_once('/')
            .context("map must use OWNER/REPOSITORY#NUMBER")?;
        ensure!(
            !owner.is_empty()
                && !repository.is_empty()
                && !repository.contains('/')
                && [owner, repository].iter().all(|part| {
                    part.bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
                })
                && !number.is_empty(),
            "map must use OWNER/REPOSITORY#NUMBER"
        );
        let number = number.parse().context("map issue number must be numeric")?;
        ensure!(number > 0, "map issue number must be positive");
        Ok(Self {
            owner: owner.to_owned(),
            repository: repository.to_owned(),
            number,
        })
    }
    fn repo(&self) -> String {
        format!("{}/{}", self.owner, self.repository)
    }
    fn issue_path(&self, number: u64) -> String {
        format!("repos/{}/issues/{number}", self.repo())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TicketInput {
    pub title: String,
    pub body: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontierTicket {
    pub number: u64,
    pub title: String,
    pub assignees: Vec<String>,
    pub labels: Vec<String>,
    pub body: String,
}

/// Snapshot of a direct map child used by delivery readiness. Includes non-task
/// children because research and human-decision issues also gate completion.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChildTicket {
    pub number: u64,
    pub title: String,
    pub url: String,
    pub state: String,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Intent {
    format_version: u32,
    id: String,
    kind: String,
    stage: String,
}

#[derive(Clone)]
pub struct GitHub {
    executable: String,
}
impl Default for GitHub {
    fn default() -> Self {
        Self {
            executable: std::env::var("GH_BIN_PATH").unwrap_or_else(|_| "gh".to_owned()),
        }
    }
}
impl GitHub {
    fn call(&self, args: &[&str], input: Option<&[u8]>) -> Result<Vec<u8>> {
        let mut command = Command::new(&self.executable);
        command
            .args(args)
            .stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().context("launch GitHub CLI (gh)")?;
        if let Some(input) = input {
            child
                .stdin
                .take()
                .context("open gh stdin")?
                .write_all(input)
                .context("write request to gh")?;
        }
        let output = child.wait_with_output().context("wait for gh")?;
        ensure!(
            output.status.success(),
            "gh {} failed ({}): {}",
            args.first().copied().unwrap_or("api"),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
        Ok(output.stdout)
    }
    fn get(&self, path: &str) -> Result<Value> {
        let output = self.call(&["api", path], None)?;
        serde_json::from_slice(&output).context("decode GitHub API response")
    }
    fn get_list(&self, path: &str) -> Result<Vec<Value>> {
        let path = format!(
            "{path}{}per_page=100",
            if path.contains('?') { "&" } else { "?" }
        );
        let output = self.call(&["api", "--paginate", "--slurp", &path], None)?;
        let pages: Vec<Vec<Value>> =
            serde_json::from_slice(&output).context("decode paginated GitHub API response")?;
        Ok(pages.into_iter().flatten().collect())
    }
    fn write(&self, method: &str, path: &str, body: &Value) -> Result<Value> {
        let input = serde_json::to_vec(body)?;
        let output = self.call(
            &["api", "--method", method, path, "--input", "-"],
            Some(&input),
        )?;
        if output.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&output).context("decode GitHub mutation response")
    }
    fn issue(&self, map: &MapRef, number: u64) -> Result<Value> {
        self.get(&map.issue_path(number))
    }
    fn subissues(&self, map: &MapRef) -> Result<Vec<Value>> {
        self.get_list(&format!(
            "repos/{}/issues/{}/sub_issues",
            map.repo(),
            map.number
        ))
    }
    fn blockers(&self, map: &MapRef, number: u64) -> Result<Vec<Value>> {
        self.get_list(&format!(
            "repos/{}/issues/{number}/dependencies/blocked_by",
            map.repo()
        ))
    }
    fn comments(&self, map: &MapRef, number: u64) -> Result<Vec<Value>> {
        self.get_list(&format!("repos/{}/issues/{number}/comments", map.repo()))
    }

    fn has_comment_marker(&self, map: &MapRef, number: u64, marker: &str) -> Result<bool> {
        let marker = format!("<!-- {marker} -->");
        Ok(self.comments(map, number)?.iter().any(|comment| {
            comment["body"]
                .as_str()
                .is_some_and(|body| body.contains(&marker))
        }))
    }

    fn post_comment_once(&self, map: &MapRef, number: u64, body: &str, marker: &str) -> Result<()> {
        if self.has_comment_marker(map, number, marker)? {
            return Ok(());
        }
        let write = self.write(
            "POST",
            &format!("repos/{}/issues/{number}/comments", map.repo()),
            &json!({"body":body}),
        );
        if self.has_comment_marker(map, number, marker)? {
            return Ok(());
        }
        match write {
            Ok(_) => bail!(
                "GitHub accepted a comment write but its operation marker is not visible; intent retained for reconciliation"
            ),
            Err(error) => Err(error).context(
                "comment write outcome is ambiguous; intent retained for marker reconciliation",
            ),
        }
    }

    pub fn frontier(&self, map: &MapRef) -> Result<Vec<FrontierTicket>> {
        let mut frontier = Vec::new();
        for issue in self.subissues(map)? {
            if issue["state"] != "open" {
                continue;
            }
            let number = issue["number"]
                .as_u64()
                .context("sub-issue omitted number")?;
            let assignees = issue["assignees"]
                .as_array()
                .context("sub-issue omitted assignees")?;
            // GitHub assignment is a claim, whether or not this runtime created it.
            if !assignees.is_empty()
                || self
                    .blockers(map, number)?
                    .iter()
                    .any(|blocker| blocker["state"] != "closed")
            {
                continue;
            }
            frontier.push(FrontierTicket {
                number,
                title: issue["title"].as_str().unwrap_or_default().to_owned(),
                assignees: vec![],
                labels: issue["labels"]
                    .as_array()
                    .map(|labels| {
                        labels
                            .iter()
                            .filter_map(|label| label["name"].as_str().map(str::to_owned))
                            .collect()
                    })
                    .unwrap_or_default(),
                body: issue["body"].as_str().unwrap_or_default().to_owned(),
            });
        }
        Ok(frontier)
    }

    /// Resolve the map's canonical specification from its body declaration
    /// and specifically named append-only map comments. The newest named
    /// pointer governs while a human-owned body refresh is pending. Other
    /// GitHub links (for example decision tickets) do not count as specification links.
    pub fn canonical_linked_spec_url(&self, map: &MapRef) -> Result<Option<String>> {
        let issue = self.issue(map, map.number)?;
        let body = issue["body"].as_str().context("map issue omitted body")?;
        let mut raw_comments = self.comments(map, map.number)?;
        raw_comments.sort_by(|left, right| {
            left["created_at"]
                .as_str()
                .cmp(&right["created_at"].as_str())
        });
        let comments = raw_comments
            .into_iter()
            .filter_map(|comment| comment["body"].as_str().map(str::to_owned))
            .collect::<Vec<_>>();
        canonical_spec_from_map_text(body, &comments)
    }

    /// Read the named child tickets that currently list `blocker` as a native dependency.
    pub fn ticket_dependents(
        &self,
        map: &MapRef,
        blocker: u64,
    ) -> Result<Vec<(u64, String, String)>> {
        let mut dependents = Vec::new();
        for child in self.subissues(map)? {
            let number = child["number"]
                .as_u64()
                .context("map child omitted ticket number")?;
            if self
                .blockers(map, number)?
                .iter()
                .any(|issue| issue["number"].as_u64() == Some(blocker))
            {
                dependents.push((
                    number,
                    child["title"].as_str().unwrap_or_default().to_owned(),
                    child["html_url"]
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| {
                            format!("https://github.com/{}/issues/{number}", map.repo())
                        }),
                ));
            }
        }
        Ok(dependents)
    }

    /// Resolve a child ticket to a human-facing title/link pair for chat summaries.
    pub fn ticket_link(&self, map: &MapRef, ticket: u64) -> Result<String> {
        let issue = self.issue(map, ticket)?;
        let title = issue["title"]
            .as_str()
            .context("GitHub ticket omitted its title")?;
        let url = issue["html_url"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("https://github.com/{}/issues/{ticket}", map.repo()));
        Ok(format!("[{title}]({url})"))
    }

    pub fn create_map(
        &self,
        owner: &str,
        repository: &str,
        title: &str,
        notes: &str,
        execution_override: bool,
        state_dir: &Path,
    ) -> Result<Value> {
        validate_repo(owner, repository)?;
        ensure!(!title.trim().is_empty(), "map title cannot be empty");
        let repo = format!("{owner}/{repository}");
        store::private_dir(state_dir)?;
        let _lock = Lock::acquire(&state_dir.join("tracker-operations.lock"))?;
        let marker = operation_marker(
            "create-map",
            &json!([repo, title, notes, execution_override]),
        );
        let mut map_notes = notes.to_owned();
        if execution_override {
            if !map_notes.is_empty() {
                map_notes.push_str("\n\n");
            }
            map_notes.push_str("Execution override: selected by the user for this effort.");
        }
        self.with_intent(state_dir, &marker, "create-map", || {
            if let Some(found) = self.find_marker(&repo, &marker)? {
                return Ok(found);
            }
            let body = format!("## Destination\n\n{title}\n\n## Notes\n\n{map_notes}\n\n## Decisions so far\n\n## Not yet specified\n\n## Out of scope\n\n<!-- {marker} -->");
            self.write(
                "POST",
                &format!("repos/{repo}/issues"),
                &json!({"title":title,"body":body,"labels":["wayfinder:map"]}),
            )
        })
    }

    pub fn create_ticket(
        &self,
        map: &MapRef,
        input: &TicketInput,
        state_dir: &Path,
    ) -> Result<Value> {
        ensure!(
            !input.title.trim().is_empty(),
            "ticket title cannot be empty"
        );
        ensure!(
            !input.label.trim().is_empty(),
            "ticket label cannot be empty"
        );
        let _lock = Lock::acquire_wait(&state_dir.join("state.lock"))?;
        let marker = operation_marker("create-ticket", &json!([map.repo(), map.number, input]));
        let issue = self.with_intent(state_dir, &marker, "create-ticket", || {
            if let Some(found) = self.find_marker(&map.repo(), &marker)? {
                return Ok(found);
            }
            let body = format!("{}\n\n<!-- {marker} -->", input.body);
            self.write(
                "POST",
                &format!("repos/{}/issues", map.repo()),
                &json!({"title":input.title,"body":body,"labels":[input.label]}),
            )
        })?;
        let id = issue["id"]
            .as_u64()
            .context("created issue omitted database ID")?;
        self.ensure_subissue(
            map,
            id,
            issue["number"]
                .as_u64()
                .context("created issue omitted number")?,
        )?;
        Ok(issue)
    }

    pub fn add_dependency(
        &self,
        map: &MapRef,
        ticket: u64,
        blocker: u64,
        state_dir: &Path,
    ) -> Result<()> {
        ensure!(ticket != blocker, "a ticket cannot block itself");
        let _lock = Lock::acquire_wait(&state_dir.join("state.lock"))?;
        ensure!(
            self.subissues(map)?.iter().any(|i| i["number"] == ticket),
            "ticket is not a child of this map"
        );
        ensure!(
            self.subissues(map)?.iter().any(|i| i["number"] == blocker),
            "blocker is not a child of this map"
        );
        let blocker_id = self.issue(map, blocker)?["id"]
            .as_u64()
            .context("blocker omitted database ID")?;
        let marker = operation_marker(
            "dependency",
            &json!([map.repo(), map.number, ticket, blocker]),
        );
        self.with_intent(state_dir, &marker, "dependency", || {
            let current = self.blockers(map, ticket)?;
            if current.iter().any(|i| i["number"] == blocker) {
                return Ok(Value::Null);
            }
            self.write(
                "POST",
                &format!(
                    "repos/{}/issues/{ticket}/dependencies/blocked_by",
                    map.repo()
                ),
                &json!({"issue_id":blocker_id}),
            )
        })?;
        Ok(())
    }

    pub fn claim(
        &self,
        map: &MapRef,
        ticket: u64,
        assignee: Option<&str>,
        state_dir: &Path,
    ) -> Result<String> {
        let _lock = Lock::acquire_wait(&state_dir.join("state.lock"))?;
        self.claim_inner(map, ticket, assignee, state_dir)
    }

    /// Runtime variant; the single per-map runtime already owns state.lock.
    pub fn claim_for_runtime(&self, map: &MapRef, ticket: u64, state_dir: &Path) -> Result<String> {
        self.claim_inner(map, ticket, None, state_dir)
    }

    /// Resolve a retained uncertain claim only when fresh GitHub reads prove it is still owned.
    /// This operation never writes an assignment; a separate explicit worker retry remains
    /// necessary when the human has confirmed that no previous worker is active.
    pub fn reconcile_claim(
        &self,
        map: &MapRef,
        ticket: u64,
        assignee: Option<&str>,
        state_dir: &Path,
    ) -> Result<String> {
        let _lock = Lock::acquire_wait(&state_dir.join("state.lock"))?;
        self.require_execution_override(map)?;
        let login = self.resolve_assignee(assignee)?;
        let marker = operation_marker("claim", &json!([map.repo(), ticket, login]));
        let path = state_dir
            .join("tracker/intents")
            .join(format!("{marker}.json"));
        ensure!(
            path.exists(),
            "no retained claim intent exists for this ticket and assignee"
        );
        let intent: Intent = serde_json::from_slice(&fs::read(&path)?)?;
        ensure!(intent.kind == "claim", "retained intent is not a claim");
        if intent.stage != "reconciled-claim" {
            ensure!(
                intent.stage == "uncertain-claim",
                "claim intent is not uncertain (current stage: {})",
                intent.stage
            );
            self.verify_owned_claim(map, ticket, &login)?;
            self.advance_intent_path(&path, "reconciled-claim")?;
        } else {
            // Make the command safely repeatable while ensuring the old proof is not stale.
            self.verify_owned_claim(map, ticket, &login)?;
        }
        Ok(login)
    }

    fn resolve_assignee(&self, assignee: Option<&str>) -> Result<String> {
        let login = match assignee {
            Some("@me") | None => self.get("user")?["login"]
                .as_str()
                .context("gh user response omitted login")?
                .to_owned(),
            Some(login) => login.to_owned(),
        };
        ensure!(!login.is_empty(), "assignee cannot be empty");
        Ok(login)
    }

    fn verify_owned_claim(&self, map: &MapRef, ticket: u64, login: &str) -> Result<()> {
        ensure!(
            self.subissues(map)?
                .iter()
                .any(|child| child["number"] == ticket),
            "claim remains uncertain: ticket is no longer a child of this map"
        );
        let issue = self.issue(map, ticket)?;
        ensure!(
            issue["state"] == "open",
            "claim remains uncertain: ticket is closed"
        );
        let assignees = issue["assignees"]
            .as_array()
            .context("claim remains uncertain: GitHub issue omitted assignees")?;
        ensure!(
            !assignees.is_empty() && assignees.iter().all(|a| a["login"] == login),
            "claim remains uncertain: GitHub does not prove exclusive assignment to @{login}"
        );
        ensure!(
            self.blockers(map, ticket)?
                .iter()
                .all(|blocker| blocker["state"] == "closed"),
            "claim remains uncertain: ticket has an open blocker"
        );
        Ok(())
    }

    fn claim_inner(
        &self,
        map: &MapRef,
        ticket: u64,
        assignee: Option<&str>,
        state_dir: &Path,
    ) -> Result<String> {
        self.require_execution_override(map)?;
        let login = self.resolve_assignee(assignee)?;
        let marker = operation_marker("claim", &json!([map.repo(), ticket, login]));
        let intent_path = state_dir
            .join("tracker/intents")
            .join(format!("{marker}.json"));
        if intent_path.exists() {
            let prior: Intent = serde_json::from_slice(&fs::read(&intent_path)?)?;
            ensure!(
                prior.stage != "uncertain-claim",
                "prior claim outcome is uncertain; reconcile GitHub assignments before retrying"
            );
            if prior.stage == "reconciled-claim" {
                self.verify_owned_claim(map, ticket, &login)?;
                return Ok(login);
            }
        }
        self.with_intent(state_dir, &marker, "claim", || {
            if !self
                .subissues(map)?
                .iter()
                .any(|child| child["number"] == ticket)
            {
                self.advance_intent(state_dir, &marker, "conflict-not-child")?;
                bail!("ticket is no longer a child of this map; claim was not attempted");
            }
            let latest = self.issue(map, ticket)?;
            if latest["state"] != "open" {
                self.advance_intent(state_dir, &marker, "conflict-closed")?;
                bail!("ticket was closed before claim; claim was not attempted");
            }
            let assigned = latest["assignees"]
                .as_array()
                .context("issue omitted assignees")?;
            if !assigned.is_empty() {
                if !assigned.iter().all(|a| a["login"] == login) {
                    self.advance_intent(state_dir, &marker, "conflict-assigned")?;
                    bail!("ticket is claimed by another assignee; claim was not attempted");
                }
                ensure!(
                    self.blockers(map, ticket)?.iter().all(|blocker| blocker["state"] == "closed"),
                    "ticket is already assigned but has an open blocker; reconcile its claim"
                );
                return Ok(latest);
            }
            if !self.frontier(map)?.iter().any(|ready| ready.number == ticket) {
                self.advance_intent(state_dir, &marker, "conflict-not-frontier")?;
                bail!("ticket is no longer in the open unclaimed frontier; claim was not attempted");
            }
            let mutation = self.write(
                "POST",
                &format!("repos/{}/issues/{ticket}/assignees", map.repo()),
                &json!({"assignees":[login]}),
            );
            let after = match self.issue(map, ticket) {
                Ok(after) => after,
                Err(error) => {
                    self.advance_intent(state_dir, &marker, "uncertain-claim")?;
                    bail!("claim outcome is uncertain after assignment request ({mutation:?}); retain intent and reconcile: {error:#}");
                }
            };
            let assigned = after["assignees"]
                .as_array();
            let postcheck = (|| -> Result<bool> {
                let assigned = assigned.context("issue omitted assignees after claim")?;
                let still_child = self
                    .subissues(map)?
                    .iter()
                    .any(|child| child["number"] == ticket);
                let no_open_blockers = self
                    .blockers(map, ticket)?
                    .iter()
                    .all(|blocker| blocker["state"] == "closed");
                Ok(after["state"] == "open"
                    && still_child
                    && no_open_blockers
                    && !assigned.is_empty()
                    && assigned
                        .iter()
                        .all(|assignee| assignee["login"] == login))
            })();
            if !matches!(postcheck, Ok(true)) {
                self.advance_intent(state_dir, &marker, "uncertain-claim")?;
                bail!("ticket state, ownership, blockers, or assignment changed or could not be confirmed after claim ({postcheck:?}); outcome is uncertain, assignments are retained, and manual reconciliation is required");
            }
            if let Err(error) = mutation {
                eprintln!("gh did not confirm the claim, but a fresh GitHub read confirms @{login}: {error:#}");
            }
            Ok(after)
        })?;
        Ok(login)
    }

    /// Read the current map children only when the execution override is explicit.
    pub fn dispatch_frontier(&self, map: &MapRef) -> Result<Vec<FrontierTicket>> {
        self.require_execution_override(map)?;
        self.frontier(map)
    }

    /// Recheck a locally claimed task before treating worker output as evidence.
    pub fn confirm_claim(&self, map: &MapRef, ticket: u64, login: &str) -> Result<()> {
        let children = self.subissues(map)?;
        ensure!(
            children.iter().any(|child| child["number"] == ticket),
            "ticket was removed from the map; worker result requires reconciliation"
        );
        let issue = self.issue(map, ticket)?;
        ensure!(
            issue["state"] == "open",
            "ticket was closed externally; closure does not establish worker success"
        );
        let assignees = issue["assignees"]
            .as_array()
            .context("issue omitted assignees")?;
        ensure!(
            assignees.len() == 1 && assignees[0]["login"] == login,
            "ticket assignment changed; worker result requires reconciliation"
        );
        ensure!(
            self.blockers(map, ticket)?
                .iter()
                .all(|blocker| blocker["state"] == "closed"),
            "ticket now has an open blocker; worker result requires reconciliation"
        );
        Ok(())
    }

    pub fn resolve(
        &self,
        map: &MapRef,
        ticket: u64,
        resolution: &str,
        spec: u64,
        state_dir: &Path,
    ) -> Result<()> {
        ensure!(
            !resolution.trim().is_empty(),
            "resolution text cannot be empty"
        );
        let _lock = Lock::acquire_wait(&state_dir.join("state.lock"))?;
        ensure!(
            self.subissues(map)?
                .iter()
                .any(|child| child["number"] == ticket),
            "ticket is not a child of this map"
        );
        let issue = self.issue(map, ticket)?;
        let marker = operation_marker(
            "resolve",
            &json!([map.repo(), map.number, ticket, spec, resolution]),
        );
        let resolution_marker = format!("{marker}:resolution");
        let comment_marker = format!("<!-- {resolution_marker} -->");
        let body = format!("{resolution}\n\n{comment_marker}");
        self.with_intent(state_dir, &marker, "resolve", || {
            let mut has_comment = self.has_comment_marker(map, ticket, &resolution_marker)?;
            let latest = self.issue(map, ticket)?;
            ensure!(latest["state"] == "open" || has_comment, "ticket was closed externally; external closure does not count as resolution");
            if !has_comment {
                self.post_comment_once(map, ticket, &body, &resolution_marker)?;
                has_comment = true;
            }
            ensure!(has_comment, "resolution comment was not confirmed");
            self.advance_intent(state_dir, &marker, "commented")?;
            let latest = self.issue(map, ticket)?;
            if latest["state"] != "closed" {
                self.write("PATCH", &map.issue_path(ticket), &json!({"state":"closed"}))?;
            }
            self.advance_intent(state_dir, &marker, "ticket-closed")?;
            let title = issue["title"].as_str().unwrap_or("Resolved ticket");
            let ticket_url = format!("https://github.com/{}/issues/{ticket}", map.repo());
            let map_marker = format!("{marker}:map-pointer");
            let map_comment = format!(
                "### Decision index pointer — body refresh pending for a human\n\n- [#{ticket} {title}]({ticket_url}): {}\n\nRefresh the map's `Decisions so far` body section manually.\n\n<!-- {map_marker} -->",
                resolution.lines().next().unwrap_or("Resolution recorded")
            );
            self.advance_intent(state_dir, &marker, "map-pointer-pending")?;
            self.post_comment_once(map, map.number, &map_comment, &map_marker)?;
            self.advance_intent(state_dir, &marker, "map-pointer-posted")?;

            let spec_marker = format!("{marker}:spec-delta");
            let proposed_delta = resolution
                .lines()
                .map(|line| format!("> {line}"))
                .collect::<Vec<_>>()
                .join("\n");
            let spec_comment = format!(
                "### Proposed specification delta — body refresh pending for a human\n\nDecision: [#{ticket} {title}]({ticket_url})\n\nProposed delta:\n{proposed_delta}\n\nReview and apply this proposed delta to the specification body manually.\n\n<!-- {spec_marker} -->"
            );
            self.advance_intent(state_dir, &marker, "spec-delta-pending")?;
            self.post_comment_once(map, spec, &spec_comment, &spec_marker)?;
            self.advance_intent(state_dir, &marker, "spec-delta-posted")?;
            Ok(Value::Null)
        })?;
        Ok(())
    }

    pub fn reconcile(&self, map: &MapRef) -> Result<Vec<FrontierTicket>> {
        self.frontier(map)
    }

    /// Direct map children, including non-task work that gates final readiness.
    pub fn child_tickets(&self, map: &MapRef) -> Result<Vec<ChildTicket>> {
        self.subissues(map)?
            .into_iter()
            .map(|issue| {
                let number = issue["number"]
                    .as_u64()
                    .context("map sub-issue omitted number")?;
                let labels = issue["labels"]
                    .as_array()
                    .context("map sub-issue omitted labels")?
                    .iter()
                    .map(|label| {
                        label["name"]
                            .as_str()
                            .map(str::to_owned)
                            .context("map sub-issue label omitted name")
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(ChildTicket {
                    number,
                    title: issue["title"]
                        .as_str()
                        .context("map sub-issue omitted title")?
                        .to_owned(),
                    url: issue["html_url"]
                        .as_str()
                        .context("map sub-issue omitted URL")?
                        .to_owned(),
                    state: issue["state"]
                        .as_str()
                        .context("map sub-issue omitted state")?
                        .to_owned(),
                    labels,
                })
            })
            .collect()
    }

    fn require_execution_override(&self, map: &MapRef) -> Result<()> {
        let body = self.issue(map, map.number)?["body"]
            .as_str()
            .context("map omitted body")?
            .to_owned();
        let notes = section(&body, "Notes")?;
        ensure!(
            affirmative_execution_override(notes),
            "map Notes do not record an explicit execution override"
        );
        Ok(())
    }

    fn ensure_subissue(&self, map: &MapRef, id: u64, number: u64) -> Result<()> {
        if self
            .subissues(map)?
            .iter()
            .any(|issue| issue["number"] == number)
        {
            return Ok(());
        }
        self.write(
            "POST",
            &format!("repos/{}/issues/{}/sub_issues", map.repo(), map.number),
            &json!({"sub_issue_id":id}),
        )?;
        Ok(())
    }

    fn find_marker(&self, repo: &str, marker: &str) -> Result<Option<Value>> {
        let issues = self.get_list(&format!("repos/{repo}/issues?state=all"))?;
        let found: Vec<_> = issues
            .into_iter()
            .filter(|issue| {
                issue["body"]
                    .as_str()
                    .is_some_and(|body| body.contains(marker))
            })
            .collect();
        ensure!(
            found.len() <= 1,
            "multiple GitHub issues contain the same operation marker; manual reconciliation required"
        );
        Ok(found.into_iter().next())
    }

    fn with_intent<F>(&self, state_dir: &Path, id: &str, kind: &str, operation: F) -> Result<Value>
    where
        F: FnOnce() -> Result<Value>,
    {
        let path = if state_dir == Path::new(".") {
            None
        } else {
            let dir = state_dir.join("tracker/intents");
            store::private_dir(&dir)?;
            Some(dir.join(format!("{id}.json")))
        };
        if let Some(path) = &path {
            let intent = if path.exists() {
                serde_json::from_slice::<Intent>(&fs::read(path)?)?
            } else {
                Intent {
                    format_version: 1,
                    id: id.to_owned(),
                    kind: kind.to_owned(),
                    stage: "pending".to_owned(),
                }
            };
            atomic_json(path, &intent)?;
        }
        let result = operation()?;
        if let Some(path) = path {
            self.advance_intent_path(&path, "complete")?;
        }
        Ok(result)
    }

    fn advance_intent(&self, state_dir: &Path, id: &str, stage: &str) -> Result<()> {
        self.advance_intent_path(
            &state_dir.join("tracker/intents").join(format!("{id}.json")),
            stage,
        )
    }
    fn advance_intent_path(&self, path: &Path, stage: &str) -> Result<()> {
        let mut intent: Intent = serde_json::from_slice(&fs::read(path)?)?;
        intent.stage = stage.to_owned();
        atomic_json(path, &intent)
    }
}

fn specification_declaration(line: &str) -> bool {
    let line = line
        .trim()
        .trim_start_matches(['-', '*', ' '])
        .trim()
        .trim_start_matches('*')
        .trim();
    let Some((label, _)) = line.split_once(':') else {
        return false;
    };
    let label = label.trim().trim_matches('*').trim();
    label.eq_ignore_ascii_case("specification")
        || label.eq_ignore_ascii_case("canonical specification")
}

pub(crate) fn canonical_spec_from_map_text(
    body: &str,
    comments: &[String],
) -> Result<Option<String>> {
    let mut body_links = body
        .lines()
        .filter(|line| specification_declaration(line))
        .map(markdown_issue_links)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    body_links.sort();
    body_links.dedup();
    ensure!(
        body_links.len() <= 1,
        "map body declares more than one canonical linked specification"
    );
    let mut canonical = body_links.into_iter().next();

    // With issue18's append-only policy, a newer named pointer supersedes a
    // stale human-owned body and older pointers. The caller orders comments
    // in creation order; a manual body refresh will agree with the latest
    // accepted pointer.
    for text in comments {
        let is_named_pointer = text.lines().any(|line| {
            line.trim_start()
                .starts_with("### Canonical linked specification")
        });
        if is_named_pointer {
            let mut links = markdown_issue_links(text)?;
            links.sort();
            links.dedup();
            ensure!(
                links.len() <= 1,
                "named map comment declares more than one canonical linked specification"
            );
            if let Some(link) = links.into_iter().next() {
                canonical = Some(link);
            }
        }
    }
    Ok(canonical)
}

fn markdown_issue_links(text: &str) -> Result<Vec<String>> {
    let mut links = Vec::new();
    let mut remainder = text;
    while let Some((_, after_marker)) = remainder.split_once("](") {
        let (url, after_url) = after_marker
            .split_once(')')
            .context("malformed Markdown link in canonical specification declaration")?;
        validate_github_issue_url(url)?;
        links.push(url.to_owned());
        remainder = after_url;
    }
    Ok(links)
}

fn validate_github_issue_url(url: &str) -> Result<()> {
    let path = url
        .strip_prefix("https://github.com/")
        .context("canonical specification URL must use https://github.com")?;
    let parts = path.split('/').collect::<Vec<_>>();
    ensure!(
        parts.len() == 4
            && !parts[0].is_empty()
            && !parts[1].is_empty()
            && parts[2] == "issues"
            && parts[3].parse::<u64>().is_ok_and(|number| number > 0),
        "canonical specification URL must identify one GitHub issue"
    );
    Ok(())
}

fn validate_repo(owner: &str, repository: &str) -> Result<()> {
    ensure!(
        !owner.is_empty()
            && !repository.is_empty()
            && !repository.contains('/')
            && [owner, repository].iter().all(|part| {
                part.bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
            }),
        "repository must use OWNER/REPOSITORY"
    );
    Ok(())
}
fn operation_marker(kind: &str, input: &Value) -> String {
    let digest = Sha256::digest(serde_json::to_vec(input).expect("JSON value serializes"));
    format!("wayfinder-operation:{kind}:{}", hex(&digest[..16]))
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn section<'a>(body: &'a str, heading: &str) -> Result<&'a str> {
    let target = format!("## {heading}");
    let start = body
        .find(&target)
        .context(format!("map body has no '{target}' section"))?
        + target.len();
    let rest = &body[start..];
    Ok(rest
        .split_once("\n## ")
        .map_or(rest, |(section, _)| section))
}
fn affirmative_execution_override(notes: &str) -> bool {
    notes.lines().any(|line| {
        let line = line.trim().trim_start_matches('-').trim().to_ascii_lowercase();
        let Some(value) = line.strip_prefix("execution override:") else {
            return false;
        };
        let value = value.trim().trim_end_matches('.').trim();
        if matches!(
            value,
            "selected"
                | "selected by user"
                | "selected by the user"
                | "selected for this effort"
                | "selected by the user for this effort"
        ) {
            return true;
        }

        // Preserve the canonical execution override wording already recorded on
        // the approved map while rejecting free-form or negated prose.
        let prefix = "this effort includes implementation and review, as explicitly selected by the user on ";
        if let Some(suffix) = value.strip_prefix(prefix) {
            let date = suffix.get(..10).unwrap_or_default();
            let valid_date = date.len() == 10
                && date.as_bytes()[4] == b'-'
                && date.as_bytes()[7] == b'-'
                && date
                    .bytes()
                    .enumerate()
                    .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit());
            return valid_date && suffix.as_bytes().get(10) == Some(&b'.');
        }
        false
    })
}
fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("intent path has no parent")?;
    fs::create_dir_all(parent)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temp, value)?;
    temp.write_all(b"\n")?;
    temp.as_file().sync_all()?;
    temp.persist(path)
        .with_context(|| format!("publish tracker intent {}", path.display()))?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_identity_requires_canonical_repository_and_positive_number() {
        assert!(MapRef::parse("owner/repo#12").is_ok());
        assert!(MapRef::parse("owner/repo/sub#12").is_err());
        assert!(MapRef::parse("owner/repo#0").is_err());
        assert!(MapRef::parse("repo#12").is_err());
    }

    #[test]
    fn execution_override_is_limited_to_notes_and_requires_explicit_selection() {
        let notes = section(
            "## Notes\n\nExecution override: selected by the user.",
            "Notes",
        )
        .unwrap();
        assert!(affirmative_execution_override(notes));
        let approved_map = "- Execution override: this effort includes implementation and review, as explicitly selected by the user on 2026-10-01. Charting remains one session.";
        assert!(affirmative_execution_override(approved_map));
        for rejected in [
            "Execution override: not selected",
            "Execution override: pending",
            "Execution override: the team selected a plan",
            "Execution override: selected, unless planning only",
        ] {
            assert!(
                !affirmative_execution_override(rejected),
                "accepted {rejected}"
            );
        }
        assert!(section("## Destination\n\nExecution override: selected", "Notes").is_err());
        assert!(!affirmative_execution_override("Execution override: TBD"));
    }

    #[test]
    fn canonical_spec_reader_ignores_unrelated_links_and_accepts_named_append_comment() {
        let body = "## Destination\n\n- Map notes.\n\n## Notes\n\n- Decision: [ticket](https://github.com/acme/project/issues/7).";
        let comments = vec!["### Canonical linked specification — body refresh pending for a human\n\n- [Spec](https://github.com/acme/project/issues/10)\n\n<!-- marker -->".to_owned()];
        assert_eq!(
            canonical_spec_from_map_text(body, &comments)
                .unwrap()
                .as_deref(),
            Some("https://github.com/acme/project/issues/10")
        );
        assert_eq!(canonical_spec_from_map_text(body, &[]).unwrap(), None);
        let refreshed_body = "## Destination\n\n- Specification: [Refreshed spec](https://github.com/acme/project/issues/12).";
        let refreshed_comments = vec!["### Canonical linked specification update — body refresh pending for a human\n\n- [Refreshed spec](https://github.com/acme/project/issues/12)\n\n<!-- marker -->".to_owned()];
        assert_eq!(
            canonical_spec_from_map_text(refreshed_body, &refreshed_comments)
                .unwrap()
                .as_deref(),
            Some("https://github.com/acme/project/issues/12")
        );
        let superseding_comments = vec![
            comments[0].clone(),
            "### Canonical linked specification update — body refresh pending for a human\n\n- [New spec](https://github.com/acme/project/issues/11)".to_owned(),
        ];
        assert_eq!(
            canonical_spec_from_map_text("## Destination", &superseding_comments)
                .unwrap()
                .as_deref(),
            Some("https://github.com/acme/project/issues/11")
        );
    }

    #[test]
    fn newest_named_spec_pointer_supersedes_stale_body_and_ordinary_links() {
        let stale_body = "## Destination\n\n**Canonical specification:** [Old spec](https://github.com/acme/project/issues/10)";
        let comments = vec![
            "### Canonical linked specification — body refresh pending for a human\n\n[Previous spec](https://github.com/acme/project/issues/10)\n\n<!-- old-pointer -->".to_owned(),
            "A ticket mentions [an unrelated issue](https://github.com/acme/project/issues/12).".to_owned(),
            "### Canonical linked specification update — body refresh pending for a human\n\n[Accepted new spec](https://github.com/acme/project/issues/11)\n\n<!-- new-pointer -->".to_owned(),
            "A later ordinary comment links [another issue](https://github.com/acme/project/issues/13).".to_owned(),
        ];

        assert_eq!(
            canonical_spec_from_map_text(stale_body, &comments)
                .unwrap()
                .as_deref(),
            Some("https://github.com/acme/project/issues/11")
        );

        let refreshed_body = "## Destination\n\n**Canonical specification:** [Accepted new spec](https://github.com/acme/project/issues/11)";
        assert_eq!(
            canonical_spec_from_map_text(refreshed_body, &comments)
                .unwrap()
                .as_deref(),
            Some("https://github.com/acme/project/issues/11")
        );

        let ambiguous = vec![
            "### Canonical linked specification update — body refresh pending for a human\n\n[First](https://github.com/acme/project/issues/11) [Second](https://github.com/acme/project/issues/14)".to_owned(),
        ];
        assert!(canonical_spec_from_map_text(stale_body, &ambiguous).is_err());
        let ambiguous_body = "**Canonical specification:** [First](https://github.com/acme/project/issues/10) [Second](https://github.com/acme/project/issues/14)";
        assert!(canonical_spec_from_map_text(ambiguous_body, &comments).is_err());
    }

    #[test]
    fn canonical_spec_declaration_rejects_non_issue_or_non_https_urls() {
        for url in [
            "https://github.com/acme/project/pull/10",
            "https://example.com/acme/project/issues/10",
            "https://github.com/acme/project/issues/0",
            "https://github.com/acme/project/issues/10?tab=comments",
        ] {
            let text = format!("### Canonical linked specification\n[Spec]({url})");
            assert!(markdown_issue_links(&text).is_err(), "accepted {url}");
        }
    }

    #[test]
    fn specification_declaration_is_explicit_and_case_insensitive() {
        assert!(specification_declaration(
            "- Specification: [Canonical](https://github.com/acme/project/issues/10)."
        ));
        assert!(specification_declaration(
            "**Canonical specification:** [Canonical](https://github.com/acme/project/issues/10)"
        ));
        let actual_map_body = "## Destination\n\n**Canonical specification:** [Wayfinder herdr plugin specification](https://github.com/Quinten1505/wayfinder-herdr/issues/10).";
        assert_eq!(
            canonical_spec_from_map_text(actual_map_body, &[])
                .unwrap()
                .as_deref(),
            Some("https://github.com/Quinten1505/wayfinder-herdr/issues/10")
        );
        assert!(!specification_declaration(
            "- Decision: [ticket](https://github.com/acme/project/issues/7)"
        ));
    }
}
