//! Herdr-hosted human-facing orchestrator and its replay-safe outbound chat outbox.
use crate::{
    herdr::Client,
    host,
    store::{
        self, AnswerDisposition, Authorization, HumanRequestKind, Lock, OrchestratorBinding,
        OrchestratorStatus, Provider, State, WorkerStatus,
    },
    tracker::{GitHub, MapRef},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{env, fs, path::Path, path::PathBuf};

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
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MessageStatus {
    Pending,
    Intent,
    Delivered,
    Uncertain,
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

    if let Some(orchestrator) = state.orchestrator.as_ref() {
        if orchestrator.status == OrchestratorStatus::Running {
            verify_orchestrator(&client, orchestrator)?;
            client.focus_agent(&orchestrator.pane_id)?;
            println!(
                "Focused the existing Wayfinder orchestrator chat in pane {} for {}.",
                orchestrator.pane_id, map
            );
            return Ok(());
        }
        anyhow::bail!(
            "orchestrator launch state is {:?}; inspect map {} and pane {:?} before any retry. Ambiguous chat launches are never duplicated",
            orchestrator.status,
            map,
            orchestrator.pane_id
        );
    }

    let context: Value = serde_json::from_str(&env::var("HERDR_PLUGIN_CONTEXT_JSON").context(
        "open the orchestrator from a Herdr workspace action or pass --map with Herdr context",
    )?)
    .context("decode Herdr action context")?;
    let socket_context = env::var_os("HERDR_SOCKET_PATH")
        .map(PathBuf::from)
        .context("missing Herdr action socket context")?;
    ensure!(
        socket_context == socket,
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
        "open the orchestrator from the map repository workspace"
    );
    let source = client.request("pane.get", json!({"pane_id": source_pane_id}))?;
    ensure!(
        source["pane"]["workspace_id"].as_str() == Some(workspace_id.as_str())
            && source["pane"]["tab_id"].as_str() == Some(tab_id.as_str())
            && source["pane"]["pane_id"].as_str() == Some(source_pane_id.as_str()),
        "Herdr action context no longer identifies the source pane"
    );
    let provider = state.workers.providers.for_role("orchestrator").clone();
    let (args, model, effort) = provider_args(&provider)?;

    // Persist launch intent before the pane split; an ambiguous response must not
    // cause another pane or chat to be created on restart.
    state.orchestrator = Some(OrchestratorBinding {
        status: OrchestratorStatus::PaneIntent,
        workspace_id: workspace_id.clone(),
        tab_id: tab_id.clone(),
        pane_id: String::new(),
        terminal_id: None,
        provider: provider.kind.clone(),
        session: None,
        source_pane_id: source_pane_id.clone(),
    });
    store::atomic_json(&dir.join("state.json"), &state)?;
    let split = match client.split_pane(&source_pane_id, &workspace_id, &cwd, true) {
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
    let observed_workspace = required(pane, "workspace_id")?;
    let observed_tab = required(pane, "tab_id")?;
    ensure!(
        observed_workspace == workspace_id && observed_tab == tab_id,
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
    let prompt = initial_prompt(&map, root, &provider, model.as_deref(), effort.as_deref());
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

pub fn reconcile(
    dir: &Path,
    state: &State,
    herdr: &Client,
    github: &GitHub,
    ticket_milestones: &[String],
) -> Result<()> {
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
        "You are the one human-facing Wayfinder orchestrator chat for map {map} at https://github.com/{}/issues/{}.\n\nFirst load and follow the Wayfinder skill at `$HOME/.agents/skills/wayfinder/SKILL.md`. Read AGENTS.md and docs/agents/issue-tracker.md. Inspect the canonical map and linked spec with `gh issue view NUMBER --repo OWNER/REPO --json title,body,comments`; include comments and use them to determine accepted scope and decisions. GitHub Issues is canonical. Never use a worker response as a human answer.\n\nWayfinding is planning by default. Opening this chat does not grant execution authorization. Inspect the accepted map Notes for an explicit execution override, and follow its actual value; do not claim or assume that it exists. Even when a map has an execution override, worker dispatch also requires Wayfinder's durable explicit Start and an unpaused runtime. Never start workers or claim execution is authorized unless the accepted map authorization and runtime state both permit it.\n\nFollow the map and specification. Give brief milestone summaries in this chat while detailed worker/reviewer output stays in their Herdr panes. Group independent pending human questions into a round, provide grounded recommendations, name the linked work each answer unblocks, and continue unaffected work. Never answer for the human. Wait for the human to respond naturally in this chat. For a `worker_question`, only after a genuine human response, invoke the attached Wayfinder binary with `--state-dir {state_root} answer-worker --map {map} --run RUN --request-id REQUEST_ID --request-type worker_question --response RESPONSE`, preserving the exact response and request correlation. The durable state root is `{state_root}`; this Wayfinder executable is `{binary}`. For `herdr_blocked_ui`, preserve the accepted manual path: have the human inspect and interact directly with the named pane, and never send raw pane input. Pause affects new dispatch only; it does not stop active workers. Worker stop is a separate explicit action that retains claim and artifacts. The human controls merges. To operate controls, use the same executable: `start --map {map}` only after explicit human authorization, `pause --map {map}`, `resume --map {map}`, and `stop-worker --map {map} --run RUN` only after an explicit stop request. The status command is `status --map {map}`.\n\nOrchestrator provider: {}{}{}. Use the existing attached runtime for status and controls. Do not create a second orchestrating chat.",
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
