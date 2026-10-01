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
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{env, fs, path::Path, path::PathBuf, time::SystemTime};

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
}

pub fn open_chat(root: &Path, requested_map: Option<&str>) -> Result<()> {
    let (map, key) = resolve_map(root, requested_map)?;
    let dir = store::map_dir(root, &key)?;
    let _lock = Lock::acquire(&dir.join("state.lock"))?;
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
                        "saved orchestrator identity no longer verifies: {error:#}. Inspect the old pane, then use the explicit Herdr action or `wayfinder-herdr recover-chat --map {map} --confirm-replacement` to archive it and create a fresh isolated chat; the old pane will not be stopped or reused"
                    );
                }
                client.focus_agent(&orchestrator.pane_id)?;
                println!(
                    "Focused the existing Wayfinder orchestrator chat in pane {} for {}.",
                    orchestrator.pane_id, map
                );
                return Ok(());
            }
            OrchestratorStatus::AgentIntent if orchestrator.session.is_some() => {
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
                    "orchestrator launch state is {status:?}; use `recover-chat --map {map} --confirm-replacement --confirm-launch-absent-or-stopped` only after confirming that the interrupted launch is absent or stopped. Unknown panes are left untouched"
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
    let _lock = Lock::acquire(&dir.join("state.lock"))?;
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
    ) && (old.status == OrchestratorStatus::PromptAccepted || old.session.is_some())
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
    let started = match client.start_agent(
        &format!("wayfinder-orchestrator-{}", &key[..12]),
        &provider.kind,
        pane_id,
        &args,
    ) {
        Ok(result) => result,
        Err(error) => {
            state.orchestrator.as_mut().unwrap().status = OrchestratorStatus::Uncertain;
            store::atomic_json(&dir.join("state.json"), &state)?;
            anyhow::bail!("orchestrator start is uncertain and will not be repeated: {error:#}");
        }
    };
    let agent = started
        .get("agent")
        .context("Herdr agent.start omitted identity; launch remains held")?;
    bind_agent(&mut state, agent, &provider.kind)?;
    store::atomic_json(&dir.join("state.json"), &state)?;
    state.orchestrator.as_mut().unwrap().status = OrchestratorStatus::PromptIntent;
    store::atomic_json(&dir.join("state.json"), &state)?;
    let mut prompt = initial_prompt(map, root, &provider, model.as_deref(), effort.as_deref());
    if let Some(note) = recovery_note {
        prompt.push_str("\n\n");
        prompt.push_str(note);
    }
    if let Err(error) = client.prompt(pane_id, &prompt) {
        state.orchestrator.as_mut().unwrap().status = OrchestratorStatus::Uncertain;
        store::atomic_json(&dir.join("state.json"), &state)?;
        anyhow::bail!(
            "initial orchestrator prompt outcome is uncertain and will not be repeated: {error:#}"
        );
    }
    let observed = client.agent(pane_id)?;
    bind_agent(
        &mut state,
        observed
            .get("agent")
            .context("Herdr omitted orchestrator identity")?,
        &provider.kind,
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
        binding.status == OrchestratorStatus::AgentIntent && binding.session.is_some(),
        "only an acknowledged agent start with a saved session identity can resume automatically"
    );
    verify_orchestrator(client, binding)?;
    let provider = state.workers.providers.for_role("orchestrator").clone();
    ensure!(
        provider.kind == binding.provider,
        "configured orchestrator provider changed during launch recovery"
    );
    let (_, model, effort) = provider_args(&provider)?;
    state.orchestrator.as_mut().unwrap().status = OrchestratorStatus::PromptIntent;
    store::atomic_json(&dir.join("state.json"), state)?;
    let prompt = initial_prompt(map, root, &provider, model.as_deref(), effort.as_deref());
    let pane_id = state.orchestrator.as_ref().unwrap().pane_id.clone();
    if let Err(error) = client.prompt(&pane_id, &prompt) {
        state.orchestrator.as_mut().unwrap().status = OrchestratorStatus::Uncertain;
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
    bind_agent(
        state,
        observed
            .get("agent")
            .context("Herdr omitted orchestrator identity")?,
        &provider,
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
    } else if binding.status == OrchestratorStatus::AgentIntent && binding.session.is_some() {
        let root = dir
            .parent()
            .and_then(Path::parent)
            .context("map state directory is not below a state root")?;
        let map = state.map.clone();
        resume_acknowledged_agent(dir, &map, root, herdr, state)?;
    }
    let Some(binding) = state.orchestrator.as_ref() else {
        return Ok(());
    };
    ensure!(
        binding.status == OrchestratorStatus::Running,
        "orchestrator is {:?}; chat delivery is held for explicit reconciliation",
        binding.status
    );
    let agent = verify_orchestrator(herdr, binding)?;
    if !matches!(
        agent["agent"]["agent_status"].as_str(),
        Some("idle" | "done")
    ) {
        // Keep notices queued while the orchestrator is active or blocked. This
        // avoids interrupting its turn and avoids input to a blocked Herdr UI.
        return Ok(());
    }
    let map = MapRef::parse(&state.map)?;
    let mut notices = build_notices(state, github, &map)?;
    notices.extend(build_scheduler_decision_notices(&map, scheduler_decisions)?);
    if !ticket_milestones.is_empty() {
        let text = format!(
            "Ticket delivery milestones (durable integration/review state):\n{}",
            ticket_milestones.join("\n")
        );
        notices.push((message_id(&text), text));
    }
    let mut outbox = read_outbox(dir)?;
    for (id, text) in notices {
        if !outbox.messages.iter().any(|message| message.id == id) {
            outbox.messages.push(OutboundMessage {
                id,
                text,
                status: MessageStatus::Pending,
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
) -> Result<Vec<(String, String)>> {
    decisions
        .iter()
        .filter(|decision| decision.response.is_none())
        .map(|decision| {
            ensure!(
                decision.ticket_link.starts_with('[')
                    && decision.ticket_link.contains("]("),
                "scheduler decisions require a linked ticket title"
            );
            let question = brief_summary(&decision.question, 360);
            let text = format!(
                "A scheduler decision is needed for {}. The review or conflict retry budget is exhausted (associated run {}; request ID {}). {}\n\nAsk the human to choose continuation, scope change, or abandonment, and give a recommendation grounded in the retained review/conflict evidence. Keep evidence details in worker/reviewer panes. This decision may outlive the worker; never use answer-worker or send input to its pane. After a genuine human response in this chat, record that exact text locally with `{}` `answer-decision --map {}/{}#{} --request-id {} --response RESPONSE`; this does not contact Herdr.",
                decision.ticket_link,
                decision.run_id,
                decision.request_id,
                question,
                env::current_exe()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|_| "wayfinder-herdr".into()),
                map.owner,
                map.repository,
                map.number,
                decision.request_id,
            );
            Ok((
                message_id(&format!("scheduler-decision:{}", decision.request_id)),
                text,
            ))
        })
        .collect()
}

pub fn list_deliveries(root: &Path, map: &str) -> Result<()> {
    let (_, key) = store::map_identity(map)?;
    let dir = store::map_dir(root, &key)?;
    let _lock = Lock::acquire(&dir.join("state.lock"))?;
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
    let _lock = Lock::acquire(&dir.join("state.lock"))?;
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
    let pending: Vec<_> = state
        .workers
        .runs
        .iter()
        .filter(|run| run.status == WorkerStatus::NeedsHuman && run.human_request_id.is_some())
        .collect();
    if !pending.is_empty() {
        let mut questions: Vec<_> = pending
            .iter()
            .map(|run| -> Result<String> {
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
                Ok(format!(
                    "- {ticket} (run {}, request ID {} / {}): {}\n  Recommendation: inspect the ticket and spec with the worker context, then offer a grounded recommendation to the human.\n  Work this answer unblocks: {unblocks}; dependent tickets remain blocked until the answer is recorded and the blocker is resolved.",
                    run.id,
                    run.human_request_id.as_deref().unwrap_or_default(),
                    run.human_request_kind.map(HumanRequestKind::as_str).unwrap_or("unknown"),
                    run.question.as_deref().unwrap_or("Question text unavailable; inspect the named worker pane."),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        questions.sort();
        let text = format!(
            "Independent human decisions are pending. Present this as one grouped round. For each request, show its exact ID and type, the worker's question, a recommendation grounded in the ticket/spec, and the named work it unblocks. Do not answer any question. Ask the human to respond naturally in this chat. For a worker_question, after an actual human response, record that response exactly with the attached Wayfinder answer-worker command and its run/request/type tuple. For herdr_blocked_ui, preserve the accepted manual path: ask the human to inspect and interact directly with the named worker pane; never send raw pane input.\n\n{}",
            questions.join("\n")
        );
        notices.push((message_id(&text), text));
    }

    for run in &state.workers.runs {
        if let Some(answer) = run.answer_history.last() {
            if answer.disposition == AnswerDisposition::Submitted {
                let ticket = github.ticket_link(map, run.ticket)?;
                let text = format!(
                    "Human decision recorded for {ticket} (request {}). The answer is durably correlated in Wayfinder state; continue the worker workflow without changing the recorded response.",
                    answer.request_id
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

fn bind_agent(state: &mut State, agent: &Value, provider: &str) -> Result<()> {
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
    binding.session = agent
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
    Ok(())
}

fn verify_orchestrator(client: &Client, binding: &OrchestratorBinding) -> Result<Value> {
    let observed = client.agent(&binding.pane_id)?;
    verify_agent_record(&observed, binding)?;
    Ok(observed)
}

/// Return an observation only when it proves the exact persisted session identity.
/// Missing or changed identities are recoverable by the separate human-gated path.
fn matching_orchestrator_agent(
    client: &Client,
    binding: &OrchestratorBinding,
) -> Result<Option<Value>> {
    match client.agent(&binding.pane_id) {
        Ok(observed) if verify_agent_record(&observed, binding).is_ok() => Ok(Some(observed)),
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
    let session = info
        .get("agent_session")
        .filter(|value| !value.is_null())
        .context("Herdr omitted orchestrator session identity")?;
    ensure!(
        session["source"].as_str() == binding.session.as_ref().map(|value| value.source.as_str())
            && session["agent"].as_str()
                == binding.session.as_ref().map(|value| value.agent.as_str())
            && session["kind"].as_str()
                == binding.session.as_ref().map(|value| value.kind.as_str())
            && session["value"].as_str()
                == binding.session.as_ref().map(|value| value.value.as_str()),
        "orchestrator session identity changed"
    );
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
        "You are the one human-facing Wayfinder orchestrator chat for map {map} at https://github.com/{}/issues/{}.\n\nFirst load and follow the Wayfinder skill at `$HOME/.agents/skills/wayfinder/SKILL.md`. Read AGENTS.md and docs/agents/issue-tracker.md. Inspect the canonical map and linked spec with `gh issue view NUMBER --repo OWNER/REPO --json title,body,comments`; include comments and use them to determine accepted scope and decisions. GitHub Issues is canonical. Never use a worker response as a human answer.\n\nWayfinding is planning by default. Opening this chat does not grant execution authorization. Inspect the accepted map Notes for an explicit execution override, and follow its actual value; do not claim or assume that it exists. Even when a map has an execution override, worker dispatch also requires Wayfinder's durable explicit Start and an unpaused runtime. Never start workers or claim execution is authorized unless the accepted map authorization and runtime state both permit it.\n\nFollow the map and specification. Give brief milestone summaries in this chat while detailed worker/reviewer output stays in their Herdr panes. Group independent pending human questions into a round, provide grounded recommendations, name the linked work each answer unblocks, and continue unaffected work. Never answer for the human. Wait for the human to respond naturally in this chat. For a `worker_question`, only after a genuine human response, invoke the attached Wayfinder binary with `--state-dir {state_root} answer-worker --map {map} --run RUN --request-id REQUEST_ID --request-type worker_question --response RESPONSE`, preserving the exact response and request correlation. The durable state root is `{state_root}`; this Wayfinder executable is `{binary}`. For `herdr_blocked_ui`, preserve the accepted manual path: have the human inspect and interact directly with the named pane, and never send raw pane input.\n\nWorker controls require actual human intent. `pause --map {map}` prevents future dispatch but does not stop active workers. Use `stop-worker --map {map} --run RUN` only after a clear human stop request; claims and artifacts remain retained. `retry-worker --map {map} --run RUN` resumes a confirmed stopped or failed attempt; for uncertain or human-blocked work, first inspect status and the named pane, then require the human to confirm the prior worker is absent or stopped and pass `--confirmed-absent-or-stopped`. Never retry a running or stop-requested worker. `abandon-worker --map {map} --run RUN` requires a clear human decision and applies only to retained or settled work; it records abandonment without proving termination, releasing uncertain capacity, or deleting artifacts. The human controls merges. To operate controls, use the same executable: `start --map {map}` only after explicit human authorization, `resume --map {map}`, and `status --map {map}`.\n\nIf an interrupted orchestrator launch is in PaneIntent, AgentIntent without saved session identity, PromptIntent, or Uncertain, never repeat pane creation, agent start, or prompt automatically. Tell the human to inspect Herdr and confirm the old launch is absent or stop it manually. Only after both that confirmation and the human replacement decision, use `recover-chat --map {map} --confirm-replacement --confirm-launch-absent-or-stopped` from the original workspace; recovery archives its prior binding and leaves all unknown/replacement panes untouched. For an acknowledged AgentIntent with a persisted session, Chat safely submits its not-yet-attempted initial prompt once. For PromptAccepted, Chat verifies the saved identity and reconnects without resubmitting it. A missing or changed Running identity still requires the human replacement decision; do not focus, stop, reuse, or send input to the old pane. For delivery uncertainty, `chat-outbox --map {map}` lists durable messages. After checking the old chat history, the human may run `resolve-chat-delivery --map {map} --message MESSAGE_ID --confirmed-delivered` or `--confirmed-not-delivered`; the latter permits one replay. Never replay an uncertain message without that explicit human decision.\n\nA scheduler-decision notice is not a worker question. Briefly frame the linked ticket title and the exhausted review/conflict choice; ask the human to choose continuation, scope change, or abandonment. Use retained review/conflict evidence for a recommendation, while detailed evidence stays in reviewer/worker panes. Wait for a real human response, then record that exact response with the issue's `answer-decision --map {map} --request-id REQUEST_ID --response RESPONSE` command. This records the scheduler decision locally and never sends input to a completed or stale worker.\n\nOrchestrator provider: {}{}{}. Use the existing attached runtime for status and controls. Do not create a second orchestrating chat.",
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
    fn scheduler_decisions_are_named_human_choices_not_worker_answers() {
        let map = MapRef::parse("example/project#42").unwrap();
        let pending = SchedulerDecisionNotice {
            request_id: "scheduler-0007".into(),
            ticket_link: "[Implement sample task](https://github.com/example/project/issues/13)"
                .into(),
            run_id: "run-0000000000000007".into(),
            question: "Review retries are exhausted. Continue, change scope, or abandon?".into(),
            response: None,
        };
        let resolved = SchedulerDecisionNotice {
            response: Some("continue after human review".into()),
            ..pending.clone()
        };
        let notices = build_scheduler_decision_notices(&map, &[pending.clone(), resolved]).unwrap();
        assert_eq!(notices.len(), 1, "resolved decisions are not asked again");
        assert_eq!(
            notices[0].0,
            message_id("scheduler-decision:scheduler-0007")
        );
        assert!(notices[0].1.contains(&pending.ticket_link));
        assert!(
            notices[0]
                .1
                .contains("choose continuation, scope change, or abandonment")
        );
        assert!(notices[0].1.contains("request ID scheduler-0007"));
        assert!(
            notices[0]
                .1
                .contains("answer-decision --map example/project#42")
        );
        assert!(
            notices[0]
                .1
                .contains("never use answer-worker or send input")
        );
        assert!(notices[0].1.contains("does not contact Herdr"));
    }
}
