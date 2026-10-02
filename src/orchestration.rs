//! Herdr-hosted human-facing orchestrator and its replay-safe outbound chat outbox.
use crate::{
    herdr::{Client, HerdrApiError},
    host,
    store::{
        self, AnswerDisposition, Authorization, HumanRequestKind, Lock, OrchestratorArchive,
        OrchestratorBinding, OrchestratorStatus, Provider, State, WorkerStatus,
    },
    tracker::{GitHub, MapRef},
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    env, fs,
    path::Path,
    path::PathBuf,
    thread,
    time::{Duration, Instant, SystemTime},
};

const OUTBOX_FILE: &str = "chat-outbox.json";
const OUTBOX_LIMIT: usize = 1000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Outbox {
    format_version: u32,
    messages: Vec<OutboundMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutboundMessage {
    id: String,
    text: String,
    status: MessageStatus,
    /// Stable human-question identities carried by a grouped transport message.
    /// Missing in issue-14 outboxes; those are reconciled through legacy IDs.
    #[serde(default)]
    constituents: Vec<String>,
    #[serde(default)]
    resolution_history: Vec<DeliveryResolution>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliveryResolution {
    choice: DeliveryChoice,
    decided_at_ms: u128,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DeliveryChoice {
    ConfirmedDelivered,
    ConfirmedNotDelivered,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MessageStatus {
    Pending,
    Intent,
    Delivered,
    Uncertain,
}

/// Adapter used by the runtime to expose issue-15 scheduler decisions without
/// coupling this chat transport module to delivery-state ownership.
#[derive(Debug, Clone)]
pub struct SchedulerDecisionNotice {
    pub request_id: String,
    /// Preformatted canonical ticket title/link, supplied by the tracker adapter.
    pub ticket_link: String,
    pub run_id: String,
    pub question: String,
    pub response: Option<String>,
    /// The human still needs to choose an explicit typed disposition. This is
    /// true for deferred and legacy response-only records as well as unanswered
    /// requests, and false only after continue/abandon was applied.
    pub action_required: bool,
}

#[derive(Debug, Clone)]
struct HumanQuestionNotice {
    identity: String,
    legacy_ids: Vec<String>,
    legacy_marker: Option<String>,
    text: String,
}

pub fn open_chat(root: &Path, requested_map: Option<&str>) -> Result<()> {
    let (map, key) = resolve_map(root, requested_map)?;
    let dir = store::map_dir(root, &key)?;
    let _lock = Lock::acquire_wait(&dir.join("state.lock"))?;
    let mut state = store::read_state(&dir)?;
    let binding = &state.binding;
    host::check(binding)?;
    let socket = binding.socket.clone();
    let client = Client::new(&socket);

    if let Some(orchestrator) = state.orchestrator.clone() {
        match orchestrator.status {
            OrchestratorStatus::Running => {
                if let Err(error) = verify_orchestrator(&client, &orchestrator) {
                    anyhow::bail!(
                        "saved orchestrator identity no longer verifies: {error:#}. Inspect the old pane, then invoke Herdr's Recover Wayfinder chat action from the original repository workspace and session; it supplies the verified source-pane context needed to archive the old binding and create a fresh isolated chat. The old pane will not be stopped or reused"
                    );
                }
                client.focus_agent(&orchestrator.pane_id)?;
                println!(
                    "Focused the existing Wayfinder orchestrator chat in pane {} for {}.",
                    orchestrator.pane_id, map
                );
                return Ok(());
            }
            OrchestratorStatus::AgentIntent
                if orchestrator.initial_prompt_attempted == Some(false)
                    && (orchestrator.session.is_some()
                        || orchestrator.foreground_process.is_some()) =>
            {
                resume_acknowledged_agent(&dir, &map, root, &client, &mut state)?;
                let resumed = state.orchestrator.as_ref().unwrap();
                client.focus_agent(&resumed.pane_id)?;
                println!(
                    "Resumed the acknowledged Wayfinder orchestrator launch in pane {} for {}.",
                    resumed.pane_id, map
                );
                return Ok(());
            }
            OrchestratorStatus::PromptAccepted => {
                mark_prompt_accepted_running(&dir, &mut state, &client)?;
                let resumed = state.orchestrator.as_ref().unwrap();
                client.focus_agent(&resumed.pane_id)?;
                println!(
                    "Reconnected to the acknowledged Wayfinder orchestrator chat in pane {} for {}.",
                    resumed.pane_id, map
                );
                return Ok(());
            }
            status => {
                anyhow::bail!(
                    "orchestrator launch state is {status:?}; after confirming that the interrupted launch is absent or stopped, invoke Herdr's Recover Wayfinder chat action from the original repository workspace and session. It supplies the verified source-pane context; unknown panes are left untouched"
                );
            }
        }
    }
    let context = action_context(&client, binding)?;
    launch_chat(
        state,
        ChatLaunch {
            dir: &dir,
            map: &map,
            key: &key,
            root,
            client: &client,
            context,
            recovery_note: None,
        },
    )
}

/// Replace only a positively stale/changed binding after an explicit human action.
/// A fresh pane is split; neither the previous nor the caller pane is reused.
pub fn recover_chat(
    root: &Path,
    requested_map: Option<&str>,
    confirmed: bool,
    confirmed_old_absent_or_stopped: bool,
) -> Result<()> {
    ensure!(
        confirmed,
        "recovery requires an explicit human replacement decision; use the Replace Wayfinder Chat action or --confirm-replacement"
    );
    let (map, key) = resolve_map(root, requested_map)?;
    let dir = store::map_dir(root, &key)?;
    let _lock = Lock::acquire_wait(&dir.join("state.lock"))?;
    let mut state = store::read_state(&dir)?;
    let socket = state.binding.socket.clone();
    host::check(&state.binding)?;
    let client = Client::new(&socket);
    let old = state
        .orchestrator
        .clone()
        .context("there is no saved orchestrator chat to recover")?;

    if matches!(
        old.status,
        OrchestratorStatus::PromptAccepted | OrchestratorStatus::AgentIntent
    ) && (old.status == OrchestratorStatus::PromptAccepted
        || (old.initial_prompt_attempted == Some(false)
            && (old.session.is_some() || old.foreground_process.is_some())))
        && matching_orchestrator_agent(&client, &old)?.is_some()
    {
        let context = action_context(&client, &state.binding)?;
        ensure!(
            context.workspace_id == old.workspace_id,
            "recover the chat from its original Herdr workspace; no mutation was made"
        );
        if old.status == OrchestratorStatus::PromptAccepted {
            mark_prompt_accepted_running(&dir, &mut state, &client)?;
            println!("Reconciled the acknowledged chat prompt without resubmitting it.");
        } else {
            resume_acknowledged_agent(&dir, &map, root, &client, &mut state)?;
            println!("Reconciled the acknowledged launch without repeating agent.start.");
        }
        return Ok(());
    }
    match old.status {
        OrchestratorStatus::Running => {
            match client.agent(&old.pane_id) {
                Ok(observed) => {
                    if verify_agent_record(&observed, &old).is_ok() {
                        anyhow::bail!(
                            "the saved orchestrator chat still has the same verified identity; use Chat to focus it instead of replacing it"
                        );
                    }
                    // A different or incomplete occupant is treated as changed. It is never
                    // focused, prompted, closed, or otherwise touched by recovery.
                }
                Err(error) if is_missing_agent_target(&error) => {}
                Err(error) => return Err(error).context("could not verify the old chat identity"),
            }
        }
        OrchestratorStatus::PaneIntent
        | OrchestratorStatus::AgentIntent
        | OrchestratorStatus::PromptIntent
        | OrchestratorStatus::PromptAccepted
        | OrchestratorStatus::Uncertain => {
            ensure!(
                confirmed_old_absent_or_stopped,
                "launch recovery requires the human to confirm the interrupted launch is absent or stopped; inspect Herdr, stop it manually if still live, then use --confirm-launch-absent-or-stopped"
            );
            confirm_interrupted_launch_not_live(&client, &old)?;
        }
    }
    let context = action_context(&client, &state.binding)?;
    ensure!(
        context.workspace_id == old.workspace_id,
        "recover the chat from its original Herdr workspace; no replacement was made"
    );
    state.orchestrator_history.push(OrchestratorArchive {
        binding: old,
        replaced_at_ms: now_ms(),
        reason:
            "human explicitly confirmed replacement after the prior identity was missing or changed"
                .into(),
    });
    launch_chat(
        state,
        ChatLaunch {
            dir: &dir,
            map: &map,
            key: &key,
            root,
            client: &client,
            context,
            recovery_note: Some(
                "The human explicitly authorized recovery after confirming the prior launch absent or stopped. The archived binding and any panes not selected as the new launch are evidence only: do not focus, stop, reuse, or send input to them. Uncertain outbox messages were not replayed; ask the human to inspect prior chat history and use the explicit delivery-resolution command only after deciding whether each was seen.",
            ),
        },
    )
}

struct ChatLaunch<'a> {
    dir: &'a Path,
    map: &'a str,
    key: &'a str,
    root: &'a Path,
    client: &'a Client,
    context: ActionContext,
    recovery_note: Option<&'a str>,
}

fn launch_chat(mut state: State, launch: ChatLaunch<'_>) -> Result<()> {
    let ChatLaunch {
        dir,
        map,
        key,
        root,
        client,
        context,
        recovery_note,
    } = launch;
    let provider = state.workers.providers.for_role("orchestrator").clone();
    let (args, model, effort) = provider_args(&provider)?;
    state.orchestrator = Some(OrchestratorBinding {
        status: OrchestratorStatus::PaneIntent,
        workspace_id: context.workspace_id.clone(),
        tab_id: context.tab_id.clone(),
        pane_id: String::new(),
        terminal_id: None,
        provider: provider.kind.clone(),
        session: None,
        foreground_process: None,
        initial_prompt_attempted: Some(false),
        source_pane_id: context.source_pane_id.clone(),
    });
    store::atomic_json(&dir.join("state.json"), &state)?;
    let split = match client.split_pane(
        &context.source_pane_id,
        &context.workspace_id,
        &context.cwd,
        true,
    ) {
        Ok(result) => result,
        Err(error) => {
            state.orchestrator.as_mut().unwrap().status = OrchestratorStatus::Uncertain;
            store::atomic_json(&dir.join("state.json"), &state)?;
            anyhow::bail!(
                "orchestrator pane creation is uncertain and will not be repeated: {error:#}"
            );
        }
    };
    let pane = split
        .get("pane")
        .context("Herdr pane.split omitted pane identity; launch remains held")?;
    let pane_id = required(pane, "pane_id")?;
    ensure!(
        required(pane, "workspace_id")? == context.workspace_id
            && required(pane, "tab_id")? == context.tab_id,
        "Herdr created the orchestrator pane outside the caller workspace/tab"
    );
    {
        let chat = state.orchestrator.as_mut().unwrap();
        chat.status = OrchestratorStatus::AgentIntent;
        chat.pane_id = pane_id.to_owned();
        chat.terminal_id = pane["terminal_id"].as_str().map(str::to_owned);
    }
    store::atomic_json(&dir.join("state.json"), &state)?;
    let shell =
        wait_for_empty_orchestrator_shell(client, pane_id, &context, pane["terminal_id"].as_str())?;
    let mut attempts = 0;
    let started = match loop {
        attempts += 1;
        match client.start_agent(
            &orchestrator_agent_name(key, pane_id),
            &provider.kind,
            pane_id,
            &args,
        ) {
            Ok(started) => break Ok(started),
            Err(error) => {
                let explicit_busy = error
                    .downcast_ref::<HerdrApiError>()
                    .is_some_and(HerdrApiError::is_agent_pane_busy);
                let same_shell = empty_orchestrator_shell(
                    client,
                    pane_id,
                    &context,
                    pane["terminal_id"].as_str(),
                )
                .is_ok_and(|observed| observed.as_ref() == Some(&shell));
                if explicit_busy && same_shell && attempts < 10 {
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
                break Err(error);
            }
        }
    } {
        Ok(result) => result,
        Err(error) => {
            state.orchestrator.as_mut().unwrap().status = OrchestratorStatus::Uncertain;
            store::atomic_json(&dir.join("state.json"), &state)?;
            anyhow::bail!("orchestrator start is uncertain and will not be repeated: {error:#}");
        }
    };
    let _started_agent = started
        .get("agent")
        .context("Herdr agent.start omitted identity; launch remains held")?;
    let pane_id = state.orchestrator.as_ref().unwrap().pane_id.clone();
    let terminal_id = state
        .orchestrator
        .as_ref()
        .and_then(|chat| chat.terminal_id.clone())
        .context("orchestrator pane omitted terminal identity; launch remains held")?;
    let (agent, process) = wait_for_orchestrator_identity(
        client,
        &pane_id,
        &context.workspace_id,
        &context.tab_id,
        &terminal_id,
        &provider.kind,
        &context.cwd,
    )?;
    bind_agent(
        &mut state,
        agent
            .get("agent")
            .context("Herdr omitted orchestrator identity after startup")?,
        &provider.kind,
        false,
    )?;
    state.orchestrator.as_mut().unwrap().foreground_process = Some(process);
    store::atomic_json(&dir.join("state.json"), &state)?;
    state.orchestrator.as_mut().unwrap().status = OrchestratorStatus::PromptIntent;
    state
        .orchestrator
        .as_mut()
        .unwrap()
        .initial_prompt_attempted = Some(true);
    store::atomic_json(&dir.join("state.json"), &state)?;
    let mut prompt = initial_prompt(map, root, &provider, model.as_deref(), effort.as_deref());
    if let Some(note) = recovery_note {
        prompt.push_str("\n\n");
        prompt.push_str(note);
    }
    if let Err(error) = client.prompt(&pane_id, &prompt) {
        let chat = state.orchestrator.as_mut().unwrap();
        if error
            .downcast_ref::<HerdrApiError>()
            .is_some_and(HerdrApiError::is_agent_not_ready)
        {
            chat.status = OrchestratorStatus::AgentIntent;
            chat.initial_prompt_attempted = Some(false);
        } else {
            chat.status = OrchestratorStatus::Uncertain;
        }
        store::atomic_json(&dir.join("state.json"), &state)?;
        anyhow::bail!(
            "initial orchestrator prompt outcome is uncertain and will not be repeated: {error:#}"
        );
    }
    let observed = client.agent(&pane_id)?;
    verify_orchestrator_observation(client, state.orchestrator.as_ref().unwrap(), &observed)?;
    bind_agent(
        &mut state,
        observed
            .get("agent")
            .context("Herdr omitted orchestrator identity")?,
        &provider.kind,
        true,
    )?;
    verify_agent_record(&observed, state.orchestrator.as_ref().unwrap())?;
    state.orchestrator.as_mut().unwrap().status = OrchestratorStatus::PromptAccepted;
    store::atomic_json(&dir.join("state.json"), &state)?;
    state.orchestrator.as_mut().unwrap().status = OrchestratorStatus::Running;
    store::atomic_json(&dir.join("state.json"), &state)?;
    println!(
        "Started the Wayfinder orchestrator chat in Herdr pane {pane_id} for {map}. Provider: {}{}{}.",
        provider.kind,
        model
            .map(|value| format!(", model {value}"))
            .unwrap_or_default(),
        effort
            .map(|value| format!(", reasoning {value}"))
            .unwrap_or_default(),
    );
    Ok(())
}

fn orchestrator_agent_name(map_key: &str, pane_id: &str) -> String {
    let digest = Sha256::digest(pane_id.as_bytes());
    format!(
        "wf-orch-{}-{}",
        &map_key[..12],
        digest[..5]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

fn empty_orchestrator_shell(
    client: &Client,
    pane_id: &str,
    context: &ActionContext,
    expected_terminal: Option<&str>,
) -> Result<Option<(String, store::LinuxProcessIdentity)>> {
    match client.agent(pane_id) {
        Err(error)
            if error
                .downcast_ref::<HerdrApiError>()
                .is_some_and(HerdrApiError::is_agent_not_found) => {}
        Ok(_) => return Ok(None),
        Err(error) => return Err(error).context("inspect empty orchestrator pane"),
    }
    let pane = client.pane(pane_id)?;
    let pane = &pane["pane"];
    ensure!(
        pane["pane_id"].as_str() == Some(pane_id)
            && pane["workspace_id"].as_str() == Some(context.workspace_id.as_str())
            && pane["tab_id"].as_str() == Some(context.tab_id.as_str()),
        "orchestrator pane identity changed before agent.start"
    );
    let terminal = required(pane, "terminal_id")?.to_owned();
    ensure!(
        Some(terminal.as_str()) == expected_terminal,
        "orchestrator terminal changed before agent.start"
    );
    let cwd = context
        .cwd
        .to_str()
        .context("orchestrator repository path is not UTF-8")?;
    ensure!(
        pane["cwd"].as_str() == Some(cwd),
        "orchestrator pane changed checkout before agent.start"
    );
    let process =
        crate::herdr::capture_shell_process(&client.pane_process_info(pane_id)?, pane_id, cwd)?;
    Ok(Some((terminal, process)))
}

fn wait_for_empty_orchestrator_shell(
    client: &Client,
    pane_id: &str,
    context: &ActionContext,
    expected_terminal: Option<&str>,
) -> Result<(String, store::LinuxProcessIdentity)> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match empty_orchestrator_shell(client, pane_id, context, expected_terminal) {
            Ok(Some(shell)) => return Ok(shell),
            Ok(None) => bail!("new orchestrator pane acquired an agent before agent.start"),
            Err(error) if Instant::now() >= deadline => {
                return Err(error).context("wait for the new orchestrator shell");
            }
            Err(_) => thread::sleep(Duration::from_millis(50)),
        }
    }
}

fn wait_for_orchestrator_identity(
    client: &Client,
    pane_id: &str,
    workspace_id: &str,
    tab_id: &str,
    terminal_id: &str,
    provider: &str,
    repository: &Path,
) -> Result<(Value, crate::store::LinuxProcessIdentity)> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let observed = client.agent(pane_id)?;
        let info = observed
            .get("agent")
            .context("Herdr agent.get omitted orchestrator identity")?;
        ensure!(
            required(info, "workspace_id")? == workspace_id,
            "orchestrator workspace changed during startup"
        );
        ensure!(
            required(info, "tab_id")? == tab_id,
            "orchestrator tab changed during startup"
        );
        ensure!(
            required(info, "pane_id")? == pane_id,
            "orchestrator pane changed during startup"
        );
        ensure!(
            required(info, "terminal_id")? == terminal_id,
            "orchestrator terminal changed during startup"
        );
        let process_info = client.pane_process_info(pane_id)?;
        let process = process_info
            .get("process_info")
            .context("Herdr pane.process_info omitted process_info")?;
        let group = process["foreground_process_group_id"].as_u64();
        let foreground = process["foreground_processes"]
            .as_array()
            .and_then(|processes| {
                processes
                    .iter()
                    .find(|entry| entry["pid"].as_u64() == group)
            });
        if info["agent"].as_str() == Some(provider)
            && foreground.is_some_and(|entry| {
                entry["name"].as_str() == Some(provider)
                    && entry["argv"]
                        .as_array()
                        .and_then(|args| args.first())
                        .and_then(Value::as_str)
                        .and_then(|arg| Path::new(arg).file_name())
                        .and_then(|name| name.to_str())
                        == Some(provider)
                    && entry["cwd"].as_str() == repository.to_str()
            })
        {
            return Ok((
                observed,
                crate::herdr::capture_foreground_process(&process_info, pane_id)?,
            ));
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "Herdr did not establish the configured orchestrator provider and exact foreground process within five seconds; launch remains held"
            )
        }
        thread::sleep(Duration::from_millis(100));
    }
}

/// Complete only the start stage whose acknowledgement and complete process identity
/// were durably saved. AgentIntent is written before any prompt request, so one
/// initial prompt is safe to submit from this exact state.
fn resume_acknowledged_agent(
    dir: &Path,
    map: &str,
    root: &Path,
    client: &Client,
    state: &mut State,
) -> Result<()> {
    let binding = state
        .orchestrator
        .as_ref()
        .context("orchestrator intent missing")?;
    ensure!(
        binding.status == OrchestratorStatus::AgentIntent
            && binding.initial_prompt_attempted == Some(false)
            && (binding.session.is_some() || binding.foreground_process.is_some()),
        "only an acknowledged agent start with a saved session or exact foreground process identity can resume automatically"
    );
    verify_orchestrator(client, binding)?;
    let provider = state.workers.providers.for_role("orchestrator").clone();
    ensure!(
        provider.kind == binding.provider,
        "configured orchestrator provider changed during launch recovery"
    );
    let (_, model, effort) = provider_args(&provider)?;
    state.orchestrator.as_mut().unwrap().status = OrchestratorStatus::PromptIntent;
    state
        .orchestrator
        .as_mut()
        .unwrap()
        .initial_prompt_attempted = Some(true);
    store::atomic_json(&dir.join("state.json"), state)?;
    let prompt = initial_prompt(map, root, &provider, model.as_deref(), effort.as_deref());
    let pane_id = state.orchestrator.as_ref().unwrap().pane_id.clone();
    if let Err(error) = client.prompt(&pane_id, &prompt) {
        let chat = state.orchestrator.as_mut().unwrap();
        if error
            .downcast_ref::<HerdrApiError>()
            .is_some_and(HerdrApiError::is_agent_not_ready)
        {
            chat.status = OrchestratorStatus::AgentIntent;
            chat.initial_prompt_attempted = Some(false);
        } else {
            chat.status = OrchestratorStatus::Uncertain;
        }
        store::atomic_json(&dir.join("state.json"), state)?;
        anyhow::bail!(
            "recovered initial prompt outcome is uncertain and will not be repeated: {error:#}"
        );
    }
    persist_prompt_acceptance(dir, state, client)
}

/// Record Herdr's acknowledged prompt only after the resulting agent identity is
/// observed and saved. A crash before this durable marker is treated as ambiguous.
fn persist_prompt_acceptance(dir: &Path, state: &mut State, client: &Client) -> Result<()> {
    let pane_id = state
        .orchestrator
        .as_ref()
        .context("orchestrator intent missing")?
        .pane_id
        .clone();
    let observed = client.agent(&pane_id)?;
    let provider = state.orchestrator.as_ref().unwrap().provider.clone();
    // The initial prompt is the bootstrap boundary for providers such as
    // Codex whose session ID is unavailable until the first prompt. Keep the
    // already persisted terminal/provider/process proof authoritative before
    // learning any session the provider now reports.
    verify_orchestrator(client, state.orchestrator.as_ref().unwrap())?;
    bind_agent(
        state,
        observed
            .get("agent")
            .context("Herdr omitted orchestrator identity")?,
        &provider,
        true,
    )?;
    verify_agent_record(&observed, state.orchestrator.as_ref().unwrap())?;
    state.orchestrator.as_mut().unwrap().status = OrchestratorStatus::PromptAccepted;
    store::atomic_json(&dir.join("state.json"), state)?;
    mark_prompt_accepted_running(dir, state, client)
}

fn mark_prompt_accepted_running(dir: &Path, state: &mut State, client: &Client) -> Result<()> {
    let binding = state
        .orchestrator
        .as_ref()
        .context("orchestrator intent missing")?;
    ensure!(
        binding.status == OrchestratorStatus::PromptAccepted,
        "orchestrator prompt has not been durably acknowledged"
    );
    verify_orchestrator(client, binding)?;
    state.orchestrator.as_mut().unwrap().status = OrchestratorStatus::Running;
    store::atomic_json(&dir.join("state.json"), state)
}

/// A confirmed replacement can proceed only if Herdr no longer reports the
/// interrupted process in its original terminal. Any different pane occupant is
/// left alone; a possibly live original launch must be stopped by the human first.
fn confirm_interrupted_launch_not_live(client: &Client, old: &OrchestratorBinding) -> Result<()> {
    if old.pane_id.is_empty() {
        // The split may have succeeded before its pane ID was persisted. There is
        // no owned identifier to query or reuse, so require the explicit human gate
        // and leave all unrecorded panes untouched.
        return Ok(());
    }
    match client.agent(&old.pane_id) {
        Err(error) if is_missing_agent_target(&error) => Ok(()),
        Err(error) => Err(error).context("could not inspect interrupted chat before replacement"),
        Ok(observed) => {
            let info = observed
                .get("agent")
                .context("Herdr omitted agent identity while checking interrupted launch")?;
            let same_terminal = info["workspace_id"].as_str() == Some(old.workspace_id.as_str())
                && info["tab_id"].as_str() == Some(old.tab_id.as_str())
                && info["pane_id"].as_str() == Some(old.pane_id.as_str())
                && old
                    .terminal_id
                    .as_deref()
                    .is_some_and(|terminal| info["terminal_id"].as_str() == Some(terminal));
            let same_provider = info["agent"].as_str() == Some(old.provider.as_str());
            let has_observed_session = info
                .get("agent_session")
                .is_some_and(|session| !session.is_null());
            let same_session = old.session.as_ref().is_some_and(|session| {
                info["agent_session"]["source"].as_str() == Some(session.source.as_str())
                    && info["agent_session"]["agent"].as_str() == Some(session.agent.as_str())
                    && info["agent_session"]["kind"].as_str() == Some(session.kind.as_str())
                    && info["agent_session"]["value"].as_str() == Some(session.value.as_str())
            });
            let session_could_match =
                old.session.is_none() || !has_observed_session || same_session;
            ensure!(
                !(same_terminal && same_provider && session_could_match),
                "the interrupted orchestrator may still be live in pane {}; stop it manually and verify it is absent before replacement",
                old.pane_id
            );
            // A different identity is not modified or reused. The human's explicit
            // absence/stopped confirmation applies to the saved launch identity.
            Ok(())
        }
    }
}

#[derive(Debug)]
struct ActionContext {
    workspace_id: String,
    tab_id: String,
    source_pane_id: String,
    cwd: PathBuf,
}

fn action_context(client: &Client, binding: &crate::store::Binding) -> Result<ActionContext> {
    let context: Value = serde_json::from_str(
        &env::var("HERDR_PLUGIN_CONTEXT_JSON")
            .context("open chat recovery from a Herdr workspace action")?,
    )
    .context("decode Herdr action context")?;
    let socket = env::var_os("HERDR_SOCKET_PATH")
        .map(PathBuf::from)
        .context("missing Herdr action socket context")?;
    ensure!(
        socket == binding.socket,
        "action came from a different Herdr session than the attached map"
    );
    let workspace_id = context_id(&context, "workspace_id")?;
    let tab_id = context_id(&context, "tab_id")?;
    let source_pane_id = context_id(&context, "focused_pane_id")?;
    let cwd = fs::canonicalize(
        context["workspace_cwd"]
            .as_str()
            .context("Herdr action omitted workspace cwd")?,
    )?;
    ensure!(
        cwd == binding.repository,
        "open chat from the map repository workspace"
    );
    let source = client.request("pane.get", json!({"pane_id": source_pane_id}))?;
    ensure!(
        source["pane"]["workspace_id"].as_str() == Some(workspace_id.as_str())
            && source["pane"]["tab_id"].as_str() == Some(tab_id.as_str())
            && source["pane"]["pane_id"].as_str() == Some(source_pane_id.as_str()),
        "Herdr action context no longer identifies the source pane"
    );
    Ok(ActionContext {
        workspace_id,
        tab_id,
        source_pane_id,
        cwd,
    })
}

pub fn reconcile(
    dir: &Path,
    state: &mut State,
    herdr: &Client,
    github: &GitHub,
    ticket_milestones: &[String],
    scheduler_decisions: &[SchedulerDecisionNotice],
) -> Result<()> {
    let Some(binding) = state.orchestrator.as_ref() else {
        return Ok(());
    };
    if binding.status == OrchestratorStatus::PromptAccepted {
        mark_prompt_accepted_running(dir, state, herdr)?;
    } else if binding.status == OrchestratorStatus::AgentIntent
        && binding.initial_prompt_attempted == Some(false)
        && (binding.session.is_some() || binding.foreground_process.is_some())
    {
        let root = dir
            .parent()
            .and_then(Path::parent)
            .context("map state directory is not below a state root")?;
        let map = state.map.clone();
        resume_acknowledged_agent(dir, &map, root, herdr, state)?;
    }
    let Some(binding) = state.orchestrator.clone() else {
        return Ok(());
    };
    ensure!(
        binding.status == OrchestratorStatus::Running,
        "orchestrator is {:?}; chat delivery is held for explicit reconciliation",
        binding.status
    );
    let agent = verify_orchestrator(herdr, &binding)?;
    pin_observed_session(dir, state, &agent)?;
    if !matches!(
        agent["agent"]["agent_status"].as_str(),
        Some("idle" | "done")
    ) {
        // Keep notices queued while the orchestrator is active or blocked. This
        // avoids interrupting its turn and avoids input to a blocked Herdr UI.
        return Ok(());
    }
    let map = MapRef::parse(&state.map)?;
    let mut questions = build_worker_question_notices(state, github, &map)?;
    questions.extend(build_scheduler_decision_notices(&map, scheduler_decisions)?);
    let mut notices = build_notices(state, github, &map)?;
    if !ticket_milestones.is_empty() {
        let text = format!(
            "Ticket delivery milestones (durable integration/review state):\n{}",
            ticket_milestones.join("\n")
        );
        notices.push((message_id(&text), text));
    }
    let mut outbox = read_outbox(dir)?;
    let new_questions = uncovered_questions(questions, &outbox);
    if !new_questions.is_empty() {
        let mut identities = new_questions
            .iter()
            .map(|question| question.identity.clone())
            .collect::<Vec<_>>();
        identities.sort();
        identities.dedup();
        let joined = identities.join("\n");
        let text = format!(
            "Independent human decisions are pending. Present these independent questions together as one grouped round. Preserve each request's title/link, exact request ID and type, and its own answer/action command. Give a recommendation grounded in the relevant ticket/spec evidence and identify the work each answer unblocks. Never answer for the human. Wait for real human responses and record each exact response only against its matching request.\n\n{}",
            new_questions
                .iter()
                .map(|question| question.text.as_str())
                .collect::<Vec<_>>()
                .join("\n\n")
        );
        outbox.messages.push(OutboundMessage {
            id: message_id(&format!("human-question-round:{joined}")),
            text,
            status: MessageStatus::Pending,
            constituents: identities,
            resolution_history: Vec::new(),
        });
    }
    for (id, text) in notices {
        if !outbox.messages.iter().any(|message| message.id == id) {
            outbox.messages.push(OutboundMessage {
                id,
                text,
                status: MessageStatus::Pending,
                constituents: Vec::new(),
                resolution_history: Vec::new(),
            });
        }
    }
    if outbox.messages.len() > OUTBOX_LIMIT {
        let excess = outbox.messages.len() - OUTBOX_LIMIT;
        ensure!(
            outbox.messages[..excess]
                .iter()
                .all(|message| matches!(message.status, MessageStatus::Delivered)),
            "chat outbox is full of unresolved delivery intents; human reconciliation is required"
        );
        outbox.messages.drain(..excess);
    }
    store::atomic_json(&dir.join(OUTBOX_FILE), &outbox)?;

    for index in 0..outbox.messages.len() {
        match outbox.messages[index].status {
            MessageStatus::Delivered => continue,
            MessageStatus::Intent | MessageStatus::Uncertain => {
                anyhow::bail!(
                    "chat message {} has an uncertain delivery outcome; it was not repeated",
                    outbox.messages[index].id
                );
            }
            MessageStatus::Pending => {}
        }
        let pane_id = &binding.pane_id;
        let prompt = format!(
            "[WAYFINDER OUTBOX MESSAGE {}]\n{}\n\nYou are the human-facing Wayfinder orchestrator. Present the notice to the human in this chat. Never answer a human decision yourself. Wait for a real human response before recording it.",
            outbox.messages[index].id, outbox.messages[index].text
        );
        outbox.messages[index].status = MessageStatus::Intent;
        store::atomic_json(&dir.join(OUTBOX_FILE), &outbox)?;
        match herdr.prompt(pane_id, &prompt) {
            Ok(_) => {
                outbox.messages[index].status = MessageStatus::Delivered;
                store::atomic_json(&dir.join(OUTBOX_FILE), &outbox)?;
                // Let the orchestrator present one durable notice and return to
                // the human before another update is submitted.
                break;
            }
            Err(error) => {
                outbox.messages[index].status = MessageStatus::Uncertain;
                store::atomic_json(&dir.join(OUTBOX_FILE), &outbox)?;
                anyhow::bail!(
                    "chat message {} delivery is uncertain and was retained without retry: {error:#}",
                    outbox.messages[index].id
                );
            }
        }
    }
    Ok(())
}

fn build_scheduler_decision_notices(
    map: &MapRef,
    decisions: &[SchedulerDecisionNotice],
) -> Result<Vec<HumanQuestionNotice>> {
    decisions
        .iter()
        .filter(|decision| decision.action_required)
        .map(|decision| {
            ensure!(
                decision.ticket_link.starts_with('[')
                    && decision.ticket_link.contains("]("),
                "scheduler decisions require a linked ticket title"
            );
            let question = brief_summary(&decision.question, 360);
            let previous_response = decision
                .response
                .as_deref()
                .map(|response| {
                    format!(
                        "Previously recorded human response (verbatim; no disposition inferred): {response}\n\n"
                    )
                })
                .unwrap_or_default();
            let text = format!(
                "Scheduler decision for {} (request ID {}; type scheduler_decision; related run {}). {}\n{}Ask the human to choose one explicit disposition: continue for one bounded implementation rework, defer to keep this ticket held while releasing only proven-completed worker capacity, or abandon to retain artifacts without claiming unimplemented work is integrated or ready. Work unblocked: the linked ticket's explicitly selected follow-up; abandonment does not satisfy integration/readiness. Recommend using retained review/conflict evidence; keep details in the worker/reviewer pane. Preserve any human response exactly and record it separately from the disposition; never infer an action from response text. This may outlive the worker. Never use answer-worker or send input to its pane. After a genuine human response, record it locally with `{}` `answer-decision --map {}/{}#{} --request-id {} --response RESPONSE --disposition continue|defer|abandon`; this does not contact Herdr.",
                decision.ticket_link,
                decision.request_id,
                decision.run_id,
                question,
                previous_response,
                env::current_exe()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|_| "wayfinder-herdr".into()),
                map.owner,
                map.repository,
                map.number,
                decision.request_id,
            );
            let (identity, legacy_ids) = scheduler_question_identity(decision);
            Ok(HumanQuestionNotice {
                identity,
                legacy_ids,
                legacy_marker: None,
                text,
            })
        })
        .collect()
}

fn scheduler_question_identity(decision: &SchedulerDecisionNotice) -> (String, Vec<String>) {
    let (identity, legacy_identity) = match decision.response.as_deref() {
        None => (
            format!("scheduler:{}:unanswered", decision.request_id),
            vec![
                format!("scheduler-decision:{}", decision.request_id),
                format!("scheduler-decision:{}:unanswered", decision.request_id),
            ],
        ),
        Some(response) => {
            let digest = Sha256::digest(response.as_bytes());
            let fingerprint = digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            (
                format!("scheduler:{}:response:{fingerprint}", decision.request_id),
                vec![format!(
                    "scheduler-decision:{}:{response}",
                    decision.request_id
                )],
            )
        }
    };
    (
        identity,
        legacy_identity
            .into_iter()
            .map(|identity| message_id(&identity))
            .collect(),
    )
}

fn message_covers_question(message: &OutboundMessage, question: &HumanQuestionNotice) -> bool {
    message.constituents.contains(&question.identity)
        || question.legacy_ids.contains(&message.id)
        || question
            .legacy_marker
            .as_ref()
            .is_some_and(|marker| message.text.contains(marker))
}

fn uncovered_questions(
    questions: Vec<HumanQuestionNotice>,
    outbox: &Outbox,
) -> Vec<HumanQuestionNotice> {
    questions
        .into_iter()
        .filter(|question| {
            !outbox
                .messages
                .iter()
                .any(|message| message_covers_question(message, question))
        })
        .collect()
}

fn build_worker_question_notices(
    state: &State,
    github: &GitHub,
    map: &MapRef,
) -> Result<Vec<HumanQuestionNotice>> {
    state
        .workers
        .runs
        .iter()
        .filter(|run| run.status == WorkerStatus::NeedsHuman && run.human_request_id.is_some())
        .map(|run| -> Result<HumanQuestionNotice> {
            let ticket = github.ticket_link(map, run.ticket)?;
            let dependents = github.ticket_dependents(map, run.ticket)?;
            let unblocks = if dependents.is_empty() {
                format!("completion and human resolution of {ticket}")
            } else {
                dependents
                    .iter()
                    .map(|(number, title, url)| format!("[{title}]({url}) (#{number})"))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let request_id = run.human_request_id.as_deref().unwrap_or_default();
            let request_type = run
                .human_request_kind
                .map(HumanRequestKind::as_str)
                .unwrap_or("unknown");
            let action = match run.human_request_kind {
                Some(HumanRequestKind::WorkerQuestion) => format!(
                    "After the genuine human responds, preserve the exact text and use `answer-worker --map {}/{}#{} --run {} --request-id {} --request-type worker_question --response RESPONSE`.",
                    map.owner, map.repository, map.number, run.id, request_id
                ),
                Some(HumanRequestKind::HerdrBlockedUi) => format!(
                    "Have the human inspect and interact directly with the linked worker pane; never send raw pane input. After the human reports the actual answer, preserve it exactly and use `answer-worker --map {}/{}#{} --run {} --request-id {} --request-type herdr_blocked_ui --response RESPONSE`.",
                    map.owner, map.repository, map.number, run.id, request_id
                ),
                _ => "Request type is unknown; inspect retained state and do not route an answer until its request type is verified.".into(),
            };
            let text = format!(
                "Worker question for {ticket} (request ID {request_id}; type {request_type}; run {}): {}\nRecommendation: inspect the ticket/spec and worker context, then give a grounded recommendation to the human.\nWork this answer unblocks: {unblocks}; dependent tickets remain blocked until the answer is recorded and the blocker is resolved. {action}",
                run.id,
                run.question
                    .as_deref()
                    .unwrap_or("Question text unavailable; inspect the named worker pane."),
            );
            Ok(HumanQuestionNotice {
                identity: format!("worker:{}:{request_id}:{request_type}", run.id),
                legacy_ids: Vec::new(),
                legacy_marker: Some(format!("request ID {request_id} / {request_type}")),
                text,
            })
        })
        .collect()
}

pub fn list_deliveries(root: &Path, map: &str) -> Result<()> {
    let (_, key) = store::map_identity(map)?;
    let dir = store::map_dir(root, &key)?;
    let _lock = Lock::acquire_wait(&dir.join("state.lock"))?;
    println!("{}", serde_json::to_string_pretty(&read_outbox(&dir)?)?);
    Ok(())
}

pub fn resolve_uncertain_delivery(
    root: &Path,
    map: &str,
    id: &str,
    confirmed_delivered: bool,
    confirmed_not_delivered: bool,
) -> Result<()> {
    ensure!(
        confirmed_delivered ^ confirmed_not_delivered,
        "choose exactly one human decision: --confirmed-delivered or --confirmed-not-delivered"
    );
    let (_, key) = store::map_identity(map)?;
    let dir = store::map_dir(root, &key)?;
    let _lock = Lock::acquire_wait(&dir.join("state.lock"))?;
    let mut outbox = read_outbox(&dir)?;
    let message = outbox
        .messages
        .iter_mut()
        .find(|message| message.id == id)
        .context("chat message ID not found")?;
    ensure!(
        matches!(
            message.status,
            MessageStatus::Intent | MessageStatus::Uncertain
        ),
        "only a chat delivery with an ambiguous outcome can be reconciled"
    );
    let (choice, next_status) = if confirmed_delivered {
        (DeliveryChoice::ConfirmedDelivered, MessageStatus::Delivered)
    } else {
        (
            DeliveryChoice::ConfirmedNotDelivered,
            MessageStatus::Pending,
        )
    };
    message.resolution_history.push(DeliveryResolution {
        choice,
        decided_at_ms: now_ms(),
    });
    message.status = next_status;
    store::atomic_json(&dir.join(OUTBOX_FILE), &outbox)?;
    println!(
        "Chat message {id} reconciled from the human's explicit delivery decision; state is {next_status:?}."
    );
    Ok(())
}

fn is_missing_agent_target(error: &anyhow::Error) -> bool {
    error.downcast_ref::<HerdrApiError>().is_some_and(|error| {
        error.method == "agent.get"
            && (error.code == "agent_not_found" || error.code == "pane_not_found")
    })
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn resolve_map(root: &Path, requested: Option<&str>) -> Result<(String, String)> {
    if let Some(map) = requested {
        let (canonical, key) = store::map_identity(map)?;
        let state = store::read_state(&store::map_dir(root, &key)?)?;
        ensure!(
            state.map == canonical,
            "map is not attached to this state directory"
        );
        return Ok((canonical, key));
    }
    let socket = env::var_os("HERDR_SOCKET_PATH")
        .map(PathBuf::from)
        .context("chat needs --map or Herdr workspace/session context")?;
    let context: Value = serde_json::from_str(
        &env::var("HERDR_PLUGIN_CONTEXT_JSON")
            .context("chat needs --map or Herdr workspace/session context")?,
    )?;
    let cwd = fs::canonicalize(
        context["workspace_cwd"]
            .as_str()
            .context("chat needs workspace cwd; pass --map explicitly")?,
    )?;
    let mut matches = Vec::new();
    if root.join("maps").exists() {
        for entry in fs::read_dir(root.join("maps"))? {
            let entry = entry?;
            let state = store::read_state(&entry.path())?;
            if state.binding.repository == cwd && state.binding.socket == socket {
                let (_, key) = store::map_identity(&state.map)?;
                matches.push((state.map, key));
            }
        }
    }
    ensure!(
        matches.len() == 1,
        "chat requires exactly one attached map in this workspace/session; pass --map explicitly"
    );
    Ok(matches.remove(0))
}

fn build_notices(state: &State, github: &GitHub, map: &MapRef) -> Result<Vec<(String, String)>> {
    let mut notices = Vec::new();
    for run in &state.workers.runs {
        if let Some(answer) = run.answer_history.last() {
            if answer.disposition == AnswerDisposition::Submitted {
                let ticket = github.ticket_link(map, run.ticket)?;
                let text = format!(
                    "Human decision recorded for {ticket} (request {}, {:?}). Exact response: {:?}. The answer is durably correlated in Wayfinder state; continue the worker workflow without changing the recorded response or asking this answered request again.",
                    answer.request_id, answer.request_kind, answer.response,
                );
                notices.push((message_id(&text), text));
            }
        }
        if matches!(
            run.status,
            WorkerStatus::Running
                | WorkerStatus::Completed
                | WorkerStatus::Reviewed
                | WorkerStatus::Failed
                | WorkerStatus::Uncertain
                | WorkerStatus::Stopped
        ) {
            let ticket = github.ticket_link(map, run.ticket)?;
            let summary = run
                .summary
                .as_deref()
                .map(|value| brief_summary(value, 240))
                .unwrap_or_default();
            let text = format!(
                "Milestone: {ticket} is {:?}. {}{}",
                run.status,
                summary,
                run.question
                    .as_deref()
                    .map(|question| format!(" Detail: {question}"))
                    .unwrap_or_default()
            );
            notices.push((message_id(&text), text));
        }
    }

    let gate = match state.authorization {
        Authorization::AwaitingStart => {
            "First use is reconciled; explicit Start is still required before worker dispatch."
        }
        Authorization::Paused => {
            "Dispatch is paused. Existing workers may continue; no new workers or reviewers start."
        }
        Authorization::Started if !state.reconciled => {
            "Dispatch is authorized and runtime reconciliation is pending."
        }
        Authorization::Started => "Dispatch is active after successful reconciliation.",
    };
    let mut status = format!("Wayfinder milestone: {gate} {}", state.suspension);
    for run in &state.workers.runs {
        let ticket = github.ticket_link(map, run.ticket)?;
        status.push_str(&format!("\n- {ticket}: {:?}", run.status));
    }
    notices.push((message_id(&status), status));
    Ok(notices)
}

fn brief_summary(value: &str, limit: usize) -> String {
    let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if value.chars().count() <= limit {
        value
    } else {
        let shortened = value
            .chars()
            .take(limit.saturating_sub(1))
            .collect::<String>();
        format!("{shortened}…")
    }
}

fn read_outbox(dir: &Path) -> Result<Outbox> {
    let path = dir.join(OUTBOX_FILE);
    if !path.exists() {
        return Ok(Outbox {
            format_version: 1,
            messages: Vec::new(),
        });
    }
    let outbox: Outbox = serde_json::from_slice(&fs::read(path)?)?;
    ensure!(outbox.format_version == 1, "unsupported chat outbox format");
    Ok(outbox)
}

fn message_id(text: &str) -> String {
    let digest = Sha256::digest(text.as_bytes());
    let hash = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("chat-{hash}")
}

fn context_id(context: &Value, key: &str) -> Result<String> {
    context[key]
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .with_context(|| {
            format!("Herdr action context omitted {key}; open chat from a workspace action")
        })
}

fn required<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .filter(|value| !value.is_empty())
        .with_context(|| format!("Herdr identity omitted {key}"))
}

fn bind_agent(
    state: &mut State,
    agent: &Value,
    provider: &str,
    allow_first_session_pin: bool,
) -> Result<()> {
    let binding = state
        .orchestrator
        .as_mut()
        .context("orchestrator intent missing")?;
    ensure!(
        required(agent, "workspace_id")? == binding.workspace_id,
        "orchestrator workspace identity changed"
    );
    ensure!(
        required(agent, "tab_id")? == binding.tab_id,
        "orchestrator tab identity changed"
    );
    ensure!(
        required(agent, "pane_id")? == binding.pane_id,
        "orchestrator pane identity changed"
    );
    let observed_provider = agent["agent"]
        .as_str()
        .context("Herdr agent omitted provider identity")?;
    ensure!(
        observed_provider == provider,
        "Herdr started an unexpected orchestrator provider"
    );
    binding.terminal_id = Some(required(agent, "terminal_id")?.to_owned());
    binding.provider = observed_provider.to_owned();
    let observed_session = agent
        .get("agent_session")
        .filter(|value| !value.is_null())
        .map(|session| -> Result<crate::store::AgentSessionIdentity> {
            Ok(crate::store::AgentSessionIdentity {
                source: required(session, "source")?.to_owned(),
                agent: required(session, "agent")?.to_owned(),
                kind: required(session, "kind")?.to_owned(),
                value: required(session, "value")?.to_owned(),
            })
        })
        .transpose()?;
    match (&binding.session, observed_session, allow_first_session_pin) {
        (Some(pinned), Some(observed), _) => ensure!(
            pinned == &observed,
            "orchestrator session identity changed; the pinned session was preserved"
        ),
        (Some(_), None, _) => {
            bail!("Herdr omitted the pinned orchestrator session; the saved identity was preserved")
        }
        (None, Some(observed), true) => binding.session = Some(observed),
        (None, _, _) => {}
    }
    Ok(())
}

/// Pin a session that became observable after the initial prompt. The caller
/// must first verify the same terminal/provider and foreground process saved
/// before bootstrap; a session alone never authorizes adopting another PID.
fn pin_observed_session(dir: &Path, state: &mut State, observed: &Value) -> Result<()> {
    let binding = state
        .orchestrator
        .as_ref()
        .context("orchestrator binding disappeared during session capture")?;
    if binding.session.is_some() {
        return Ok(());
    }
    let agent = observed
        .get("agent")
        .context("Herdr omitted orchestrator agent identity")?;
    if agent.get("agent_session").is_none_or(Value::is_null) {
        return Ok(());
    }
    ensure!(
        binding.status == OrchestratorStatus::Running
            && binding.initial_prompt_attempted == Some(true),
        "cannot pin an orchestrator session before initial-prompt bootstrap is acknowledged"
    );
    let provider = state
        .workers
        .providers
        .for_role("orchestrator")
        .kind
        .clone();
    bind_agent(state, agent, &provider, true)?;
    store::atomic_json(&dir.join("state.json"), state)
        .context("persist verified orchestrator session before chat delivery")?;
    Ok(())
}

fn verify_orchestrator(client: &Client, binding: &OrchestratorBinding) -> Result<Value> {
    let observed = client.agent(&binding.pane_id)?;
    verify_orchestrator_observation(client, binding, &observed)?;
    Ok(observed)
}

/// Pin a human instruction relayed by this exact, currently running chat.
/// The agent still has to preserve what the human said; Herdr proves which
/// orchestrator relayed it, not the semantics of the words.
pub fn verified_caller_session(state: &State) -> Result<store::AgentSessionIdentity> {
    let binding = state
        .orchestrator
        .as_ref()
        .context("this map has no attached orchestrator chat")?;
    ensure!(
        binding.status == OrchestratorStatus::Running,
        "the orchestrator chat is not running"
    );
    ensure!(
        env::var("HERDR_ENV").as_deref() == Ok("1")
            && env::var("HERDR_PANE_ID").as_deref() == Ok(binding.pane_id.as_str())
            && env::var_os("HERDR_SOCKET_PATH").as_deref()
                == Some(state.binding.socket.as_os_str()),
        "authorization must be relayed from this map's verified orchestrator pane"
    );
    verify_orchestrator(&Client::new(&state.binding.socket), binding)?;
    binding
        .session
        .clone()
        .context("the orchestrator has no pinned agent session identity")
}

fn verify_orchestrator_observation(
    client: &Client,
    binding: &OrchestratorBinding,
    observed: &Value,
) -> Result<()> {
    verify_agent_record(observed, binding)?;
    let expected = binding
        .foreground_process
        .as_ref()
        .context("orchestrator has no saved foreground process identity")?;
    let process = client.pane_process_info(&binding.pane_id)?;
    ensure!(
        crate::herdr::capture_foreground_process(&process, &binding.pane_id)? == *expected,
        "orchestrator foreground process identity changed"
    );
    Ok(())
}

/// Return an observation only when it proves the exact persisted session identity.
/// Missing or changed identities are recoverable by the separate human-gated path.
fn matching_orchestrator_agent(
    client: &Client,
    binding: &OrchestratorBinding,
) -> Result<Option<Value>> {
    match client.agent(&binding.pane_id) {
        Ok(observed) if verify_orchestrator(client, binding).is_ok() => Ok(Some(observed)),
        Ok(_) => Ok(None),
        Err(error) if is_missing_agent_target(&error) => Ok(None),
        Err(error) => Err(error).context("could not inspect saved orchestrator identity"),
    }
}

fn verify_agent_record(agent: &Value, binding: &OrchestratorBinding) -> Result<()> {
    let info = agent
        .get("agent")
        .context("Herdr omitted orchestrator agent identity")?;
    ensure!(
        required(info, "workspace_id")? == binding.workspace_id,
        "orchestrator workspace identity changed"
    );
    ensure!(
        required(info, "tab_id")? == binding.tab_id,
        "orchestrator tab identity changed"
    );
    ensure!(
        required(info, "pane_id")? == binding.pane_id,
        "orchestrator pane identity changed"
    );
    ensure!(
        Some(required(info, "terminal_id")?) == binding.terminal_id.as_deref(),
        "orchestrator terminal identity changed"
    );
    ensure!(
        info["agent"].as_str() == Some(binding.provider.as_str()),
        "orchestrator provider identity changed"
    );
    if let Some(expected) = binding.session.as_ref() {
        let session = info
            .get("agent_session")
            .filter(|value| !value.is_null())
            .context("Herdr omitted orchestrator session identity")?;
        ensure!(
            session["source"].as_str() == Some(expected.source.as_str())
                && session["agent"].as_str() == Some(expected.agent.as_str())
                && session["kind"].as_str() == Some(expected.kind.as_str())
                && session["value"].as_str() == Some(expected.value.as_str()),
            "orchestrator session identity changed"
        );
    }
    Ok(())
}

fn provider_args(provider: &Provider) -> Result<(Vec<String>, Option<String>, Option<String>)> {
    ensure!(
        !provider.kind.trim().is_empty(),
        "orchestrator provider kind cannot be empty"
    );
    let mut args = provider.args.clone();
    if let Some(model) = &provider.model {
        ensure!(
            !model.trim().is_empty(),
            "orchestrator model cannot be empty"
        );
        args.extend(["--model".into(), model.clone()]);
    }
    if let Some(effort) = &provider.reasoning_effort {
        ensure!(
            !effort.trim().is_empty(),
            "orchestrator reasoning effort cannot be empty"
        );
        if provider.kind == "codex" {
            args.extend(["-c".into(), format!("model_reasoning_effort={effort}")]);
        } else {
            args.extend(["--reasoning-effort".into(), effort.clone()]);
        }
    }
    Ok((
        args,
        provider.model.clone(),
        provider.reasoning_effort.clone(),
    ))
}

fn initial_prompt(
    map: &str,
    root: &Path,
    provider: &Provider,
    model: Option<&str>,
    effort: Option<&str>,
) -> String {
    format!(
        "You are the one human-facing Wayfinder orchestrator chat for map {map} at https://github.com/{}/issues/{}.\n\nFirst load and follow the Wayfinder skill at `$HOME/.agents/skills/wayfinder/SKILL.md`. Read AGENTS.md and docs/agents/issue-tracker.md. Inspect the canonical map and linked spec with `gh issue view NUMBER --repo OWNER/REPO --json title,body,comments`; include comments and use them to determine accepted scope and decisions. GitHub Issues is canonical. Never use a worker response as a human answer.\n\nWayfinding is planning by default. Opening this chat does not grant execution authorization. Inspect the accepted map Notes for an explicit execution override, and follow its actual value; do not claim or assume that it exists. For an already attached map without that Notes override, a genuine explicit instruction from the human for this named map may be recorded with `{binary} --state-dir {state_root} authorize-existing --map {map} --instruction RESPONSE`, preserving the exact response. This command verifies this chat, records a durable map-scoped receipt, reconciles its named GitHub comment, and queues one Start. Never derive that instruction from attachment, chat opening, a worker answer, or your own inference. Worker dispatch also requires Wayfinder's durable explicit Start and an unpaused runtime. Never start workers or claim execution is authorized unless the accepted map authorization and runtime state both permit it.\n\nFollow the map and specification. Give brief milestone summaries in this chat while detailed worker/reviewer output stays in their Herdr panes. Group independent pending human questions into a round, provide grounded recommendations, name the linked work each answer unblocks, and continue unaffected work. Never answer for the human. Wait for the human to respond naturally in this chat. For a `worker_question`, only after a genuine human response, invoke the attached Wayfinder binary with `--state-dir {state_root} answer-worker --map {map} --run RUN --request-id REQUEST_ID --request-type worker_question --response RESPONSE`, preserving the exact response and request correlation. The durable state root is `{state_root}`; this Wayfinder executable is `{binary}`. For `herdr_blocked_ui`, preserve the accepted manual path: have the human inspect and interact directly with the named pane, and never send raw pane input.\n\nWorker controls require actual human intent. `pause --map {map}` prevents future dispatch but does not stop active workers. Use `stop-worker --map {map} --run RUN` only after a clear human stop request; claims and artifacts remain retained. `retry-worker --map {map} --run RUN` resumes a confirmed stopped or failed attempt; for uncertain or human-blocked work, first inspect status and the named pane, then require the human to confirm the prior worker is absent or stopped and pass `--confirmed-absent-or-stopped`. Never retry a running or stop-requested worker. `abandon-worker --map {map} --run RUN` requires a clear human decision and applies only to retained or settled work; it records abandonment without proving termination, releasing uncertain capacity, or deleting artifacts. The human controls merges. To operate controls, use the same executable: `start --map {map}` only after explicit human authorization, `resume --map {map}`, and `status --map {map}`.\n\nIf an interrupted orchestrator launch is in PaneIntent, AgentIntent without saved session identity, PromptIntent, or Uncertain, never repeat pane creation, agent start, or prompt automatically. Tell the human to inspect Herdr and confirm the old launch is absent or stop it manually. Only after both that confirmation and the human replacement decision, invoke Herdr's Recover Wayfinder chat action from the original repository workspace and session; the action carries the verified socket and source-pane context required by recovery. Recovery archives its prior binding and leaves all unknown/replacement panes untouched. For an acknowledged AgentIntent with a persisted session, Chat safely submits its not-yet-attempted initial prompt once. For PromptAccepted, Chat verifies the saved identity and reconnects without resubmitting it. A missing or changed Running identity still requires the human replacement decision; do not focus, stop, reuse, or send input to the old pane. For delivery uncertainty, `chat-outbox --map {map}` lists durable messages. After checking the old chat history, the human may run `resolve-chat-delivery --map {map} --message MESSAGE_ID --confirmed-delivered` or `--confirmed-not-delivered`; the latter permits one replay. Never replay an uncertain message without that explicit human decision.\n\nA scheduler-decision notice is not a worker question. Briefly frame the linked ticket title and the exhausted review/conflict choice. Ask the human to choose an explicit disposition: `continue` for one bounded rework, `defer` to keep the ticket held while freeing only proven-completed worker capacity, or `abandon` to retain artifacts without claiming unimplemented work is integrated or ready. Recommend using retained evidence, while detailed evidence stays in reviewer/worker panes. Wait for a real human response, preserve its exact text verbatim, and record that text separately from the human-selected disposition with `answer-decision --map {map} --request-id REQUEST_ID --response RESPONSE --disposition continue|defer|abandon`. Never infer an action from response text. Deferred and legacy response-only decisions remain visibly actionable until a typed action is applied. This command records locally and never sends input to a completed or stale worker.\n\nOrchestrator provider: {}{}{}. Use the existing attached runtime for status and controls. Do not create a second orchestrating chat.",
        MapRef::parse(map)
            .map(|reference| format!("{}/{}", reference.owner, reference.repository))
            .unwrap_or_else(|_| "OWNER/REPO".into()),
        MapRef::parse(map)
            .map(|reference| reference.number)
            .unwrap_or_default(),
        provider.kind,
        model
            .map(|value| format!(", model {value}"))
            .unwrap_or_default(),
        effort
            .map(|value| format!(", reasoning effort {value}"))
            .unwrap_or_default(),
        state_root = root.display(),
        binary = env::current_exe()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|_| "wayfinder-herdr".into()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orchestrator_agent_name_fits_herdr_limit() {
        let name = orchestrator_agent_name(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "w1:p3",
        );
        assert!(name.starts_with("wf-orch-0123456789ab-"));
        assert_ne!(
            name,
            orchestrator_agent_name(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                "w1:p4"
            )
        );
        assert!(name.len() <= 32);
        assert!(
            name.bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        );
    }

    #[test]
    fn scheduler_decisions_are_named_human_choices_not_worker_answers() {
        let map = MapRef::parse("example/project#42").unwrap();
        let pending = SchedulerDecisionNotice {
            request_id: "scheduler-0007".into(),
            ticket_link: "[Implement sample task](https://github.com/example/project/issues/13)"
                .into(),
            run_id: "run-0000000000000007".into(),
            question: "Review retries are exhausted. Continue, change scope, or abandon?".into(),
            response: None,
            action_required: true,
        };
        let deferred = SchedulerDecisionNotice {
            request_id: "scheduler-deferred".into(),
            response: Some("we should return to this later".into()),
            action_required: true,
            ..pending.clone()
        };
        let legacy_response_only = SchedulerDecisionNotice {
            request_id: "scheduler-legacy".into(),
            response: Some("please continue".into()),
            action_required: true,
            ..pending.clone()
        };
        let applied = SchedulerDecisionNotice {
            request_id: "scheduler-applied".into(),
            response: Some("please continue".into()),
            action_required: false,
            ..pending.clone()
        };
        let notices = build_scheduler_decision_notices(
            &map,
            &[pending.clone(), deferred, legacy_response_only, applied],
        )
        .unwrap();
        assert_eq!(
            notices.len(),
            3,
            "deferred and legacy records remain actionable"
        );
        assert_eq!(notices[0].identity, "scheduler:scheduler-0007:unanswered");
        assert!(notices[0].text.contains(&pending.ticket_link));
        assert!(notices[0].text.contains("choose one explicit disposition"));
        assert!(notices[0].text.contains("request ID scheduler-0007"));
        assert!(
            notices[0]
                .text
                .contains("answer-decision --map example/project#42")
        );
        assert!(
            notices[0]
                .text
                .contains("--disposition continue|defer|abandon")
        );
        assert!(
            notices[0]
                .text
                .contains("never infer an action from response text")
        );
        assert!(notices[1].text.contains("we should return to this later"));
        assert!(notices[1].text.contains("no disposition inferred"));
        let deferred_update = build_scheduler_decision_notices(
            &map,
            &[SchedulerDecisionNotice {
                response: Some("we should return to this later".into()),
                action_required: true,
                ..pending.clone()
            }],
        )
        .unwrap();
        assert_ne!(
            notices[0].identity, deferred_update[0].identity,
            "recording a response must produce one new visible decision notice"
        );
        assert!(
            notices[0]
                .text
                .contains("Never use answer-worker or send input")
        );
        assert!(notices[0].text.contains("does not contact Herdr"));
    }

    #[test]
    fn accepted_issue_14_scheduler_outbox_ids_cover_the_same_request_after_upgrade() {
        let map = MapRef::parse("example/project#42").unwrap();
        let scheduler = build_scheduler_decision_notices(
            &map,
            &[SchedulerDecisionNotice {
                request_id: "scheduler-upgrade".into(),
                ticket_link: "[Upgrade task](https://github.com/example/project/issues/13)".into(),
                run_id: "run-old".into(),
                question: "Choose what happens next".into(),
                response: None,
                action_required: true,
            }],
        )
        .unwrap()
        .remove(0);
        let legacy_id = message_id("scheduler-decision:scheduler-upgrade");
        let interim_id = message_id("scheduler-decision:scheduler-upgrade:unanswered");
        assert!(scheduler.legacy_ids.contains(&legacy_id));
        assert!(scheduler.legacy_ids.contains(&interim_id));
        let worker = HumanQuestionNotice {
            identity: "worker:run-new:human-new-1:worker_question".into(),
            legacy_ids: Vec::new(),
            legacy_marker: Some("request ID human-new-1 / worker_question".into()),
            text: "A new worker question".into(),
        };

        for status in [MessageStatus::Delivered, MessageStatus::Uncertain] {
            let outbox = Outbox {
                format_version: 1,
                messages: vec![OutboundMessage {
                    id: legacy_id.clone(),
                    text: "legacy scheduler notice".into(),
                    status,
                    constituents: Vec::new(),
                    resolution_history: Vec::new(),
                }],
            };
            let fresh = uncovered_questions(vec![scheduler.clone(), worker.clone()], &outbox);
            assert_eq!(fresh.len(), 1);
            assert_eq!(fresh[0].identity, worker.identity);
            assert_eq!(outbox.messages[0].status, status);
        }
    }
}
