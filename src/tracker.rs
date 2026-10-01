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
            });
        }
        Ok(frontier)
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
        let _lock = Lock::acquire(&state_dir.join("state.lock"))?;
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
        let _lock = Lock::acquire(&state_dir.join("state.lock"))?;
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
        let _lock = Lock::acquire(&state_dir.join("state.lock"))?;
        self.require_execution_override(map)?;
        let login = match assignee {
            Some("@me") | None => self.get("user")?["login"]
                .as_str()
                .context("gh user response omitted login")?
                .to_owned(),
            Some(login) => login.to_owned(),
        };
        ensure!(!login.is_empty(), "assignee cannot be empty");
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
        let _lock = Lock::acquire(&state_dir.join("state.lock"))?;
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
        let comment_marker = format!("<!-- {marker} -->");
        let body = format!("{resolution}\n\n{comment_marker}");
        self.with_intent(state_dir, &marker, "resolve", || {
            let mut has_comment = self.comments(map, ticket)?.iter().any(|c| c["body"].as_str().is_some_and(|b| b.contains(&comment_marker)));
            let latest = self.issue(map, ticket)?;
            ensure!(latest["state"] == "open" || has_comment, "ticket was closed externally; external closure does not count as resolution");
            if !has_comment {
                self.write("POST", &format!("repos/{}/issues/{ticket}/comments", map.repo()), &json!({"body":body}))?;
                has_comment = true;
            }
            ensure!(has_comment, "resolution comment was not confirmed");
            self.advance_intent(state_dir, &marker, "commented")?;
            let latest = self.issue(map, ticket)?;
            if latest["state"] != "closed" {
                self.write("PATCH", &map.issue_path(ticket), &json!({"state":"closed"}))?;
            }
            self.advance_intent(state_dir, &marker, "closed")?;
            let link = format!("- [#{} {}](https://github.com/{}/issues/{}) — {}", ticket, issue["title"].as_str().unwrap_or("Resolved ticket"), map.repo(), ticket, resolution.lines().next().unwrap_or("Resolved"));
            self.update_issue_section(map, map.number, "Decisions so far", &link, &marker)?;
            self.advance_intent(state_dir, &marker, "map-updated")?;
            let spec_ref = MapRef { number: spec, ..map.clone() };
            let spec_entry = format!("- Evidence from [#{}](https://github.com/{}/issues/{}) is recorded in its resolution comment.", ticket, map.repo(), ticket);
            self.update_issue_section(map, spec, "Evidence and open design", &spec_entry, &marker)?;
            self.advance_intent(state_dir, &marker, "complete")?;
            let _ = spec_ref;
            Ok(Value::Null)
        })?;
        Ok(())
    }

    pub fn reconcile(&self, map: &MapRef) -> Result<Vec<FrontierTicket>> {
        self.frontier(map)
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

    fn update_issue_section(
        &self,
        map: &MapRef,
        number: u64,
        heading: &str,
        entry: &str,
        marker: &str,
    ) -> Result<()> {
        let path = map.issue_path(number);
        for _ in 0..4 {
            let issue = self.get(&path)?;
            let body = issue["body"].as_str().unwrap_or_default();
            if body.contains(marker) || body.contains(entry) {
                return Ok(());
            }
            let updated = append_section(body, heading, entry, marker)?;
            let _ = self.write("PATCH", &path, &json!({"body":updated}));
            // GitHub issue-body writes do not offer conditional updates. Verify and
            // re-merge if a concurrent edit replaced this write before readback.
            let latest = self.get(&path)?;
            let latest_body = latest["body"].as_str().unwrap_or_default();
            if latest_body.contains(marker) || latest_body.contains(entry) {
                return Ok(());
            }
        }
        bail!("GitHub issue kept changing while updating {heading}; retry after reconciliation")
    }
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
fn append_section(body: &str, heading: &str, entry: &str, marker: &str) -> Result<String> {
    let target = format!("## {heading}");
    let start = body
        .find(&target)
        .context(format!("issue body has no '{target}' section"))?
        + target.len();
    let rest = &body[start..];
    let end = rest
        .find("\n## ")
        .map_or(body.len(), |offset| start + offset);
    let mut updated = body.to_owned();
    updated.insert_str(end, &format!("\n{entry}\n\n<!-- {marker} -->\n"));
    Ok(updated)
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
    fn safe_section_edit_preserves_surrounding_concurrent_content() {
        let original = "## Notes\n\nkeep one\n\n## Decisions so far\n\nexisting decision\n\n## Out of scope\n\nkeep two";
        let updated =
            append_section(original, "Decisions so far", "- [#4](url) — done", "marker").unwrap();
        assert!(updated.starts_with("## Notes\n\nkeep one"));
        assert!(updated.contains("existing decision\n\n- [#4](url) — done"));
        assert!(updated.ends_with("## Out of scope\n\nkeep two"));
    }
}
