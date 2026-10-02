use anyhow::{Context, Result, bail, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

pub const FORMAT: u32 = 1;
pub const DEFAULT_REWORK_ROUNDS: u8 = 3;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub repository: PathBuf,
    pub herdr_binary: PathBuf,
    pub socket: PathBuf,
    pub herdr_config: Option<PathBuf>,
    /// Owning repo workspace used as the explicit source for Herdr worktree.open.
    #[serde(default)]
    pub source_workspace_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Authorization {
    AwaitingStart,
    Started,
    Paused,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    pub format_version: u32,
    pub map: String,
    pub binding: Binding,
    pub authorization: Authorization,
    pub poll_seconds: u64,
    pub concurrency: u32,
    /// Set only after the bound host and GitHub have both reconciled successfully.
    pub reconciled: bool,
    pub suspension: String,
    /// Canonical map-linked spec resolved from its issue body and explicitly
    /// named append-only map comments during the latest GitHub reconciliation.
    #[serde(default)]
    pub canonical_linked_spec_url: Option<String>,
    #[serde(default)]
    pub canonical_linked_spec_resolved: bool,
    /// Request IDs are retained to make replay after commit-before-unlink safe.
    pub history: Vec<Applied>,
    /// Defaults keep state created by the initial runtime foundation readable.
    #[serde(default)]
    pub workers: WorkerState,
    /// Identity of the single Herdr-hosted orchestrator chat, if launched.
    #[serde(default)]
    pub orchestrator: Option<OrchestratorBinding>,
    /// Former chats are evidence only. Recovery never sends commands to these panes.
    #[serde(default)]
    pub orchestrator_history: Vec<OrchestratorArchive>,
    /// Scheduler decisions never target a possibly completed worker pane.
    #[serde(default)]
    pub scheduler_decisions: Vec<SchedulerDecision>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrchestratorBinding {
    pub status: OrchestratorStatus,
    pub workspace_id: String,
    pub tab_id: String,
    pub pane_id: String,
    pub terminal_id: Option<String>,
    pub provider: String,
    #[serde(default)]
    pub session: Option<AgentSessionIdentity>,
    /// Exact provider process identity persisted before the first prompt. Some
    /// providers only expose a session ID after receiving that prompt.
    #[serde(default)]
    pub foreground_process: Option<LinuxProcessIdentity>,
    /// `Some(false)` means the first prompt was definitely never attempted;
    /// `Some(true)` means an effect may have been sent. Missing legacy state is
    /// unknown and must not be replayed automatically.
    #[serde(default)]
    pub initial_prompt_attempted: Option<bool>,
    /// The exact Herdr pane that requested creation, used only to reconcile launch intent.
    pub source_pane_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrchestratorArchive {
    pub binding: OrchestratorBinding,
    pub replaced_at_ms: u128,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrchestratorStatus {
    PaneIntent,
    AgentIntent,
    PromptIntent,
    /// Herdr acknowledged the initial prompt and its resulting agent identity was persisted.
    PromptAccepted,
    Running,
    Uncertain,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SchedulerDecision {
    pub request_id: String,
    pub request_kind: HumanRequestKind,
    pub ticket: u64,
    pub run_id: String,
    /// Implementation run whose work is blocked, even when the request was
    /// raised by a separate reviewer run.
    #[serde(default)]
    pub source_run_id: String,
    #[serde(default)]
    pub kind: SchedulerDecisionKind,
    #[serde(default)]
    pub blocked_status: Option<WorkerStatus>,
    #[serde(default)]
    pub base_commit: Option<String>,
    pub question: String,
    #[serde(default)]
    pub response: Option<String>,
    #[serde(default)]
    pub disposition: Option<SchedulerDecisionDisposition>,
    #[serde(default)]
    pub application: SchedulerDecisionApplication,
    #[serde(default)]
    pub successor_run_id: Option<String>,
    /// Keep every exact human response, including a later change from defer.
    #[serde(default)]
    pub answers: Vec<SchedulerDecisionAnswer>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerDecisionKind {
    #[default]
    ReviewExhaustion,
    ConflictExhaustion,
    RequiredChecksExhaustion,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerDecisionDisposition {
    Continue,
    Defer,
    Abandon,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerDecisionApplication {
    #[default]
    Awaiting,
    Deferred,
    Continued,
    Abandoned,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SchedulerDecisionAnswer {
    pub response: String,
    pub disposition: SchedulerDecisionDisposition,
}

pub struct NewSchedulerDecision<'a> {
    pub ticket: u64,
    pub run_id: &'a str,
    pub source_run_id: &'a str,
    pub kind: SchedulerDecisionKind,
    pub blocked_status: WorkerStatus,
    pub base_commit: Option<&'a str>,
    pub question: &'a str,
}

impl SchedulerDecision {
    /// A response without a typed action (including legacy freeform responses)
    /// still needs an explicit human action. Defer remains pending by design.
    pub fn awaits_human_action(&self) -> bool {
        matches!(
            self.application,
            SchedulerDecisionApplication::Awaiting | SchedulerDecisionApplication::Deferred
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct WorkerState {
    #[serde(default)]
    pub next_run: u64,
    #[serde(default)]
    pub runs: Vec<WorkerRun>,
    #[serde(default)]
    pub providers: ProviderConfiguration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfiguration {
    #[serde(default = "default_provider")]
    pub default: Provider,
    #[serde(default)]
    pub roles: BTreeMap<String, Provider>,
}

impl Default for ProviderConfiguration {
    fn default() -> Self {
        Self {
            default: default_provider(),
            roles: BTreeMap::new(),
        }
    }
}

fn default_provider() -> Provider {
    Provider {
        kind: "codex".into(),
        model: None,
        reasoning_effort: None,
        args: Vec::new(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provider {
    pub kind: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPurpose {
    TicketWork,
    FinalFeatureReview,
}

impl ProviderConfiguration {
    pub fn for_role(&self, role: &str) -> &Provider {
        self.roles.get(role).unwrap_or(&self.default)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerRun {
    pub id: String,
    pub ticket: u64,
    pub role: String,
    pub attempt: u8,
    #[serde(default)]
    pub automatic_retries: u8,
    #[serde(default)]
    pub rework_round: u8,
    /// Explicit per-run cap. A human may grant one additional round only by
    /// choosing the typed Continue disposition for a scheduler decision.
    #[serde(default = "default_rework_round_limit")]
    pub rework_round_limit: u8,
    pub status: WorkerStatus,
    /// Persisted before a worktree or pane is requested from Herdr.
    pub worktree: PathBuf,
    pub workspace_id: Option<String>,
    pub tab_id: Option<String>,
    pub pane_id: Option<String>,
    pub base_commit: Option<String>,
    pub result_commit: Option<String>,
    pub summary: Option<String>,
    pub question: Option<String>,
    #[serde(default)]
    pub human_response: Option<String>,
    #[serde(default)]
    pub human_decision: Option<String>,
    /// Current human request identity. Answers must name both this ID and kind.
    #[serde(default)]
    pub human_request_seq: u32,
    #[serde(default)]
    pub human_request_id: Option<String>,
    #[serde(default)]
    pub human_request_kind: Option<HumanRequestKind>,
    /// Fingerprint of the exact blocked prompt snapshot, when Herdr supplied it.
    #[serde(default)]
    pub human_request_fingerprint: Option<String>,
    /// Correlation retained for an answer attempt, including ambiguous delivery.
    #[serde(default)]
    pub answer_request_id: Option<String>,
    #[serde(default)]
    pub answer_request_kind: Option<HumanRequestKind>,
    /// Append-only local evidence for each human answer attempt.
    #[serde(default)]
    pub answer_history: Vec<HumanAnswerEvidence>,
    #[serde(default)]
    pub source_run: Option<String>,
    #[serde(default)]
    pub claim_login: Option<String>,
    #[serde(default)]
    pub context: Option<String>,
    /// Stable workflow purpose. Legacy final-review records are recognized by
    /// their original marker only until the next supported retry persists this
    /// typed value; new dispatch and retry decisions use this field.
    #[serde(default)]
    pub purpose: Option<RunPurpose>,
    /// Last time Herdr confirmed activity for this submitted prompt. Brief idle
    /// snapshots after submission are not evidence that the launch is absent.
    #[serde(default)]
    pub last_activity_ms: Option<u64>,
    /// Identity observed from Herdr after this prompt entered its first turn.
    /// Missing legacy fields are not proof that a restored pane is this worker.
    #[serde(default)]
    pub terminal_id: Option<String>,
    #[serde(default)]
    pub agent_provider: Option<String>,
    #[serde(default)]
    pub agent_session: Option<AgentSessionIdentity>,
    #[serde(default)]
    pub foreground_process: Option<LinuxProcessIdentity>,
    /// Durable output copy outside the source checkout.
    #[serde(default)]
    pub result_evidence: Option<PathBuf>,
    /// Earlier immutable result artifacts retained when a blocked worker later
    /// updates its result after a correlated human answer.
    #[serde(default)]
    pub result_evidence_history: Vec<PathBuf>,
    /// The initial task prompt was rejected before input and is still pending
    /// human resolution of a Herdr startup UI.
    #[serde(default)]
    pub initial_prompt_pending: bool,
    /// Herdr acknowledged the initial task prompt. Until a session ID is
    /// persisted, pane/terminal/provider and Linux process identity reconnect
    /// this confirmed launch without ever replaying its prompt.
    #[serde(default)]
    pub initial_prompt_acknowledged: bool,
    /// None is legacy/unknown; Some(false) proves the initial prompt call was
    /// never entered, and Some(true) preserves a potentially ambiguous attempt.
    #[serde(default)]
    pub initial_prompt_attempted: Option<bool>,
    /// One-time recovery window after durable acknowledgement but before the
    /// original worker identity has been re-established in this runtime.
    #[serde(default)]
    pub initial_prompt_reconnect_pending: bool,
    /// A definite failure occurred before any agent was launched. Keep the
    /// ticket claim and human retry request, but do not consume an agent slot.
    #[serde(default)]
    pub known_prelaunch_failure: bool,
}

/// Resolve a worker through retry ancestry to the implementation whose commit
/// it ultimately concerns. Reviewer retries retain their immediate
/// `source_run` for auditability, so approval consumers must not assume that
/// their direct parent is an implementer.
pub fn implementation_source<'a>(runs: &'a [WorkerRun], run: &WorkerRun) -> Option<&'a WorkerRun> {
    let mut source_id = run.source_run.as_deref();
    let mut visited = std::collections::BTreeSet::new();
    while let Some(id) = source_id {
        if !visited.insert(id) {
            return None;
        }
        let source = runs.iter().find(|candidate| candidate.id == id)?;
        if source.role == "implementer" {
            return Some(source);
        }
        source_id = source.source_run.as_deref();
    }
    None
}

fn default_rework_round_limit() -> u8 {
    DEFAULT_REWORK_ROUNDS
}

impl WorkerRun {
    pub fn reserves_capacity(&self) -> bool {
        self.status.reserves_capacity() && !self.known_prelaunch_failure
    }

    pub fn is_final_feature_review(&self) -> bool {
        self.purpose == Some(RunPurpose::FinalFeatureReview)
            || (self.purpose.is_none()
                && self
                    .context
                    .as_deref()
                    .is_some_and(|context| context.starts_with("wayfinder-final-feature-review")))
    }

    pub fn effective_purpose(&self) -> RunPurpose {
        if self.is_final_feature_review() {
            RunPurpose::FinalFeatureReview
        } else {
            RunPurpose::TicketWork
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanRequestKind {
    WorkerQuestion,
    HerdrBlockedUi,
    SchedulerDecision,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HumanAnswerEvidence {
    pub request_id: String,
    pub request_kind: HumanRequestKind,
    pub response: String,
    pub disposition: AnswerDisposition,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnswerDisposition {
    Intent,
    ManualRequired,
    Submitted,
    RejectedBeforeEffect,
    Uncertain,
}

impl HumanRequestKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WorkerQuestion => "worker_question",
            Self::HerdrBlockedUi => "herdr_blocked_ui",
            Self::SchedulerDecision => "scheduler_decision",
        }
    }
}

/// Persist one stable scheduler question for issue 14's orchestrating chat.
/// Reconciliation retries with the same ticket, run, and text reuse the ID.
pub fn create_scheduler_decision(
    state: &mut State,
    request: NewSchedulerDecision<'_>,
) -> Result<String> {
    let NewSchedulerDecision {
        ticket,
        run_id,
        source_run_id,
        kind,
        blocked_status,
        base_commit,
        question,
    } = request;
    ensure!(
        !question.trim().is_empty(),
        "scheduler question cannot be empty"
    );
    // Preserve the request ID algorithm shipped with the first scheduler
    // decision record so a restart can enrich that record instead of emitting
    // a second chat request during the schema extension.
    let identity = format!("{}\n{ticket}\n{run_id}\n{question}", state.map);
    let generated_request_id = format!(
        "scheduler-{}",
        &format!("{:x}", Sha256::digest(identity.as_bytes()))[..24]
    );
    let legacy_request_id = state
        .scheduler_decisions
        .iter()
        .find(|decision| {
            decision.ticket == ticket
                && decision.run_id == run_id
                && decision.source_run_id.is_empty()
                && decision.blocked_status.is_none()
                && !matches!(
                    decision.application,
                    SchedulerDecisionApplication::Continued
                        | SchedulerDecisionApplication::Abandoned
                )
        })
        .map(|decision| decision.request_id.clone());
    let request_id = legacy_request_id.unwrap_or(generated_request_id);
    if let Some(existing) = state
        .scheduler_decisions
        .iter_mut()
        .find(|decision| decision.request_id == request_id)
    {
        ensure!(
            existing.ticket == ticket
                && existing.run_id == run_id
                && existing.request_kind == HumanRequestKind::SchedulerDecision,
            "scheduler request ID collision or changed request identity"
        );
        let legacy = existing.source_run_id.is_empty() && existing.blocked_status.is_none();
        ensure!(
            legacy || existing.question == question,
            "scheduler request ID collision or changed question"
        );
        ensure!(
            legacy || (existing.source_run_id == source_run_id && existing.kind == kind),
            "scheduler request ID collision or changed run relationship"
        );
        ensure!(
            existing.base_commit.is_none() || existing.base_commit.as_deref() == base_commit,
            "scheduler request ID collision or changed fixed commit"
        );
        if legacy {
            existing.source_run_id = source_run_id.to_owned();
            existing.kind = kind;
            existing.blocked_status = Some(blocked_status);
            existing.base_commit = base_commit.map(str::to_owned);
            existing.question = question.to_owned();
        }
    } else {
        state.scheduler_decisions.push(SchedulerDecision {
            request_id: request_id.clone(),
            request_kind: HumanRequestKind::SchedulerDecision,
            ticket,
            run_id: run_id.to_owned(),
            source_run_id: source_run_id.to_owned(),
            kind,
            blocked_status: Some(blocked_status),
            base_commit: base_commit.map(str::to_owned),
            question: question.to_owned(),
            response: None,
            disposition: None,
            application: SchedulerDecisionApplication::Awaiting,
            successor_run_id: None,
            answers: Vec::new(),
        });
    }
    Ok(request_id)
}

/// Record the actual human response without contacting a worker or Herdr.
pub fn record_scheduler_decision_response(
    state: &mut State,
    request_id: &str,
    response: &str,
    disposition: SchedulerDecisionDisposition,
) -> Result<()> {
    ensure!(
        !response.trim().is_empty(),
        "human response cannot be empty"
    );
    let decision = state
        .scheduler_decisions
        .iter_mut()
        .find(|decision| decision.request_id == request_id)
        .context("scheduler decision request not found")?;
    ensure!(
        decision.request_kind == HumanRequestKind::SchedulerDecision,
        "request is not a scheduler decision"
    );
    if decision.response.as_deref() == Some(response) && decision.disposition == Some(disposition) {
        return Ok(());
    }
    ensure!(
        !matches!(
            decision.application,
            SchedulerDecisionApplication::Continued | SchedulerDecisionApplication::Abandoned
        ),
        "scheduler decision was already applied; create a new request for another action"
    );
    decision.response = Some(response.to_owned());
    decision.disposition = Some(disposition);
    decision.answers.push(SchedulerDecisionAnswer {
        response: response.to_owned(),
        disposition,
    });
    decision.application = if disposition == SchedulerDecisionDisposition::Defer {
        SchedulerDecisionApplication::Deferred
    } else {
        SchedulerDecisionApplication::Awaiting
    };
    Ok(())
}

impl WorkerRun {
    /// Keep a stable correlation while the same request remains pending; create a
    /// fresh ID when the request source or its exact blocked UI snapshot changes.
    pub fn set_human_request(&mut self, kind: HumanRequestKind, content: &str) {
        let fingerprint = human_request_fingerprint(content);
        if self.human_request_kind == Some(kind)
            && self.human_request_fingerprint.as_deref() == Some(&fingerprint)
            && self.human_request_id.is_some()
        {
            return;
        }
        self.human_request_seq = self.human_request_seq.saturating_add(1);
        self.human_request_id = Some(format!("human-{}-{:04}", self.id, self.human_request_seq));
        self.human_request_kind = Some(kind);
        self.human_request_fingerprint = Some(fingerprint);
    }
}

pub fn human_request_fingerprint(content: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(content.as_bytes()))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionIdentity {
    pub source: String,
    pub agent: String,
    pub kind: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LinuxProcessIdentity {
    pub boot_id: String,
    pub pid: u32,
    pub start_time_ticks: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    Queued,
    LaunchIntent,
    OpenIntent,
    /// Exact worktree pane found; agent.start is known not to have been sent.
    AgentStartReady,
    AgentIntent,
    /// Herdr confirmed agent.start and its terminal/process identity was saved;
    /// the first task prompt has not been submitted.
    InitialPromptReady,
    PromptIntent,
    AnswerIntent,
    Running,
    Uncertain,
    StopRequested,
    Stopped,
    NeedsHuman,
    Failed,
    Completed,
    Reviewed,
}

impl WorkerStatus {
    pub fn reserves_capacity(&self) -> bool {
        matches!(
            self,
            Self::LaunchIntent
                | Self::OpenIntent
                | Self::AgentStartReady
                | Self::AgentIntent
                | Self::InitialPromptReady
                | Self::PromptIntent
                | Self::AnswerIntent
                | Self::Running
                | Self::Uncertain
                | Self::StopRequested
                | Self::NeedsHuman
        )
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Applied {
    pub id: String,
    pub command: RequestKind,
    pub outcome: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RequestKind {
    Start,
    AuthorizedStart,
    Pause,
    Resume,
    Reconcile,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub format_version: u32,
    pub sequence: u64,
    pub id: String,
    pub command: RequestKind,
}

pub fn map_identity(input: &str) -> Result<(String, String)> {
    let (repo, number) = input
        .rsplit_once('#')
        .context("map must be OWNER/REPO#NUMBER")?;
    let parts: Vec<_> = repo.split('/').collect();
    ensure!(
        parts.len() == 2
            && parts.iter().all(|p| !p.is_empty()
                && p.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))),
        "map must be OWNER/REPO#NUMBER"
    );
    let number: u64 = number
        .parse()
        .context("map number must be a positive integer")?;
    ensure!(number > 0, "map number must be positive");
    let map = format!("{}#{number}", repo.to_ascii_lowercase());
    let key = format!("{:x}", Sha256::digest(map.as_bytes()));
    Ok((map, key))
}

pub fn map_dir(root: &Path, key: &str) -> Result<PathBuf> {
    ensure!(
        key.len() == 64
            && key
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "invalid map key"
    );
    Ok(root.join("maps").join(key))
}

pub fn private_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    File::open(path)?.sync_all()?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// Never unlink a lock file: contenders must lock the same inode across restarts.
pub struct Lock {
    _file: File,
}
impl Drop for Lock {
    fn drop(&mut self) {
        // Explicit unlock also releases a descriptor temporarily inherited across a
        // concurrent fork before CLOEXEC runs in another test/host command.
        let _ = FileExt::unlock(&self._file);
    }
}
impl Lock {
    pub fn acquire(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(path)?;
        file.try_lock_exclusive().with_context(|| {
            format!(
                "lock busy: {} (another runtime or command owns this map)",
                path.display()
            )
        })?;
        Ok(Self { _file: file })
    }

    /// Wait for the state transaction to finish instead of making a foreground
    /// control fail merely because the supervised runtime is reconciling. The
    /// runtime and user commands still serialize all reads and mutations on the
    /// same kernel lock; only lifetime/integration locks remain nonblocking.
    pub fn acquire_wait(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(path)?;
        file.lock_exclusive()
            .with_context(|| format!("wait for state lock {}", path.display()))?;
        Ok(Self { _file: file })
    }
}

/// Commit in the same directory: sync data, atomic rename, then sync directory.
pub fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path.parent().context("state path has no parent")?;
    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut tmp, value)?;
    tmp.write_all(b"\n")?;
    tmp.as_file().sync_all()?;
    tmp.persist(path).map_err(|e| e.error)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub fn read_versioned<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).with_context(|| {
        format!(
            "invalid JSON in {}; restore a known-good backup; file was not changed",
            path.display()
        )
    })?;
    if value.get("format_version").and_then(|v| v.as_u64()) != Some(FORMAT as u64) {
        bail!(
            "unsupported state/request format in {}; install a Wayfinder version supporting this format or restore a compatible backup; no state was changed",
            path.display()
        );
    }
    serde_json::from_value(value).with_context(|| format!("unsupported or corrupt fields in {}; keep this file and use a compatible Wayfinder version; no state was changed", path.display()))
}

pub fn read_state(dir: &Path) -> Result<State> {
    let state: State = read_versioned(&dir.join("state.json"))?;
    let (_, expected) = map_identity(&state.map)?;
    ensure!(
        dir.file_name().and_then(|n| n.to_str()) == Some(&expected),
        "map identity does not match state directory"
    );
    ensure!(
        (1..=3600).contains(&state.poll_seconds) && state.concurrency > 0,
        "invalid runtime settings; state unchanged"
    );
    Ok(state)
}

pub fn attach(
    root: &Path,
    map: &str,
    binding: Binding,
    poll_seconds: u64,
) -> Result<(String, State)> {
    ensure!(
        (1..=3600).contains(&poll_seconds),
        "poll seconds must be 1..3600"
    );
    let (map, key) = map_identity(map)?;
    let dir = map_dir(root, &key)?;
    private_dir(root)?;
    private_dir(&root.join("maps"))?;
    private_dir(&dir)?;
    private_dir(&dir.join("inbox"))?;
    let _lock = Lock::acquire_wait(&dir.join("state.lock"))?;
    let path = dir.join("state.json");
    let state = if path.exists() {
        let mut state = read_state(&dir)?;
        if state.binding != binding {
            let only_adds_source_workspace = state.binding.repository == binding.repository
                && state.binding.herdr_binary == binding.herdr_binary
                && state.binding.socket == binding.socket
                && state.binding.herdr_config == binding.herdr_config
                && state.binding.source_workspace_id.is_none()
                && binding.source_workspace_id.is_some();
            ensure!(
                only_adds_source_workspace,
                "map already bound to another repository or herdr endpoint; existing state preserved"
            );
            state.binding.source_workspace_id = binding.source_workspace_id.clone();
            atomic_json(&path, &state)?;
        }
        state
    } else {
        let state = State {
            format_version: FORMAT,
            map,
            binding,
            authorization: Authorization::AwaitingStart,
            poll_seconds,
            concurrency: 3,
            reconciled: false,
            suspension:
                "First attachment: explicit Start required; GitHub tracker reads begin with runtime reconciliation"
                    .into(),
            canonical_linked_spec_url: None,
            canonical_linked_spec_resolved: false,
            history: vec![],
            workers: WorkerState::default(),
            orchestrator: None,
            orchestrator_history: vec![],
            scheduler_decisions: Vec::new(),
        };
        atomic_json(&path, &state)?;
        state
    };
    Ok((key, state))
}

pub fn enqueue(dir: &Path, command: RequestKind) -> Result<String> {
    // Validate before adding requests; unsupported state must remain untouched.
    read_state(dir)?;
    let inbox = dir.join("inbox");
    // Serialize submissions independently from the runtime state transaction. Persist
    // the sequence before publishing a request: crashes may leave gaps, never reuse IDs.
    let _queue_lock = Lock::acquire_wait(&dir.join("inbox.lock"))?;
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Sequence {
        format_version: u32,
        next: u64,
    }
    let sequence_path = dir.join("sequence.json");
    let sequence: Sequence = if sequence_path.exists() {
        read_versioned(&sequence_path)?
    } else {
        Sequence {
            format_version: FORMAT,
            next: 1,
        }
    };
    let next = sequence
        .next
        .checked_add(1)
        .context("request sequence exhausted")?;
    atomic_json(
        &sequence_path,
        &Sequence {
            format_version: FORMAT,
            next,
        },
    )?;
    let id = format!("request-{:020}", sequence.next);
    let request = Request {
        format_version: FORMAT,
        sequence: sequence.next,
        id: id.clone(),
        command,
    };
    atomic_json(&inbox.join(format!("{id}.json")), &request)?;
    Ok(id)
}

/// Called while holding state.lock before an existing-map authorization enqueues
/// its first Start. A lost acknowledgement is found in either applied history or
/// the durable inbox, so retrying the same human instruction cannot add another.
pub fn start_recorded_or_pending(dir: &Path, state: &State) -> Result<bool> {
    if state.history.iter().any(|applied| {
        matches!(
            &applied.command,
            RequestKind::Start | RequestKind::AuthorizedStart
        )
    }) {
        return Ok(true);
    }
    for entry in fs::read_dir(dir.join("inbox"))? {
        let path = entry?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            let request: Request = read_versioned(&path)?;
            if matches!(
                &request.command,
                RequestKind::Start | RequestKind::AuthorizedStart
            ) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub fn process_requests(dir: &Path, state: &mut State) -> Result<()> {
    let mut paths = fs::read_dir(dir.join("inbox"))?
        .map(|r| r.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.retain(|p| p.extension().is_some_and(|e| e == "json"));
    let mut requests = paths
        .into_iter()
        .map(|path| {
            let request: Request = read_versioned(&path)?;
            Ok((request.sequence, path, request))
        })
        .collect::<Result<Vec<_>>>()?;
    requests.sort_by_key(|(sequence, _, _)| *sequence);
    for (_, path, request) in requests {
        ensure!(
            path.file_stem().and_then(|p| p.to_str()) == Some(&request.id),
            "request ID does not match filename; request retained"
        );
        if !state.history.iter().any(|r| r.id == request.id) {
            let outcome = match request.command {
                RequestKind::Start => {
                    if state.authorization == Authorization::AwaitingStart {
                        state.authorization = Authorization::Started;
                    }
                    "Start recorded; dispatch still requires successful reconciliation"
                }
                RequestKind::AuthorizedStart => {
                    if state.authorization == Authorization::AwaitingStart {
                        state.authorization = Authorization::Started;
                    }
                    "Existing-map Start recorded; an explicit Pause remains in effect"
                }
                RequestKind::Pause => {
                    state.authorization = Authorization::Paused;
                    "Dispatch paused"
                }
                RequestKind::Resume => {
                    if state.history.iter().any(|r| {
                        matches!(
                            &r.command,
                            RequestKind::Start | RequestKind::AuthorizedStart
                        )
                    }) {
                        state.authorization = Authorization::Started;
                        "Resume recorded; dispatch still requires successful reconciliation"
                    } else {
                        "Resume rejected: explicit Start required first"
                    }
                }
                RequestKind::Reconcile => "Reconciliation requested",
            };
            // Pause before first Start must not prevent the first explicit Start.
            if request.command == RequestKind::Start
                && !state
                    .history
                    .iter()
                    .any(|r| r.command == RequestKind::Start)
            {
                state.authorization = Authorization::Started;
            }
            state.history.push(Applied {
                id: request.id,
                command: request.command,
                outcome: outcome.into(),
            });
            state.reconciled = false;
            atomic_json(&dir.join("state.json"), state)?;
        }
        fs::remove_file(&path)?;
        File::open(dir.join("inbox"))?.sync_all()?;
    }
    Ok(())
}
