use crate::{
    agent::AgentContext,
    config::Config,
    jobs::Jobs,
    mcp::{Pool as McpPool, Servers as McpServers},
    models::{ModelCatalog, ModelInfo},
    provider::Provider,
    session::{Session, Store, Turn},
    tools::string,
    workspace::{Workspace, read_bounded},
};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::{Mutex, Semaphore, mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const MAX_FRAME: usize = 1024 * 1024;

#[derive(Debug)]
struct InvalidParams(&'static str);

impl std::fmt::Display for InvalidParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for InvalidParams {}

fn selection<'a>(params: &'a Value, key: &str) -> Result<&'a str> {
    params[key]
        .as_str()
        .filter(|value| {
            !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
        })
        .ok_or_else(|| InvalidParams("Missing or invalid selection parameter").into())
}

const WIRE_QUEUE_BYTES: usize = 4 * 1024 * 1024;
const WIRE_BATCH_BYTES: usize = 64 * 1024;
const WIRE_BATCH_FRAMES: usize = 64;

type PendingReplies = std::sync::Mutex<HashMap<String, oneshot::Sender<Value>>>;

#[derive(Clone)]
enum Outbound {
    Values(mpsc::Sender<Value>),
    Wire {
        tx: mpsc::Sender<WireFrame>,
        bytes: Arc<Semaphore>,
        byte_limit: usize,
    },
}

struct WireFrame {
    bytes: Vec<u8>,
    // Includes the frame currently being written, not only channel storage.
    _permit: tokio::sync::OwnedSemaphorePermit,
}

struct PendingPermission<'a> {
    pending: &'a PendingReplies,
    id: &'a str,
}

impl Drop for PendingPermission<'_> {
    fn drop(&mut self) {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(self.id);
    }
}

#[derive(Clone)]
pub struct Peer {
    outbound: Outbound,
    pending: Arc<PendingReplies>,
}

impl Peer {
    /// In-process clients can continue consuming structured values without a
    /// serialize/parse round trip. The stdio path uses compact wire frames.
    pub fn channel(capacity: usize) -> (Self, mpsc::Receiver<Value>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self {
                outbound: Outbound::Values(tx),
                pending: Arc::new(PendingReplies::default()),
            },
            rx,
        )
    }

    fn wire_channel(capacity: usize, byte_limit: usize) -> (Self, mpsc::Receiver<WireFrame>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self {
                outbound: Outbound::Wire {
                    tx,
                    bytes: Arc::new(Semaphore::new(byte_limit)),
                    byte_limit,
                },
                pending: Arc::new(PendingReplies::default()),
            },
            rx,
        )
    }

    async fn closed(&self) {
        match &self.outbound {
            Outbound::Values(tx) => tx.closed().await,
            Outbound::Wire { tx, .. } => tx.closed().await,
        }
    }

    pub async fn send(&self, value: Value) -> Result<()> {
        match &self.outbound {
            Outbound::Values(tx) => tx.send(value).await.context("ACP client disconnected"),
            Outbound::Wire {
                tx,
                bytes,
                byte_limit,
            } => {
                // Reserve before encoding: tasks waiting for a slow client never
                // accumulate their own second, encoded copy of a large event.
                let slot = tx.reserve().await.context("ACP client disconnected")?;
                let length = crate::session::json_bytes(&value)?.saturating_add(1);
                // Existing valid task/model snapshots can exceed the queue budget.
                // Such a frame owns the whole budget exclusively until written.
                let permits = length.min(*byte_limit) as u32;
                let permit = bytes.clone().acquire_many_owned(permits).await?;
                let mut encoded = Vec::with_capacity(length);
                serde_json::to_writer(&mut encoded, &value)?;
                encoded.push(b'\n');
                drop(value);
                slot.send(WireFrame {
                    bytes: encoded,
                    _permit: permit,
                });
                Ok(())
            }
        }
    }
    pub async fn update(&self, session: &str, update: Value) -> Result<()> {
        let mut envelope =
            json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":session}});
        envelope["params"]["update"] = update;
        self.send(envelope).await
    }
    pub async fn task_event(&self, session: &str, id: &str, event: Value) -> Result<()> {
        let mut envelope = json!({"jsonrpc":"2.0","method":"_harness/task","params":{"sessionId":session,"taskId":id}});
        envelope["params"]["event"] = event;
        self.send(envelope).await
    }

    pub async fn permission(
        &self,
        session: &str,
        call: &str,
        title: &str,
        kind: &str,
        input: Value,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let id = format!("permission-{}", Uuid::new_v4());
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(id.clone(), tx);
        // Future cancellation, including an outer task abort while send is
        // backpressured, must retire the pending reply registration too.
        let _registration = PendingPermission {
            pending: &self.pending,
            id: &id,
        };
        let exchange = async {
            let mut envelope = json!({"jsonrpc":"2.0","id":id,"method":"session/request_permission","params":{
                "sessionId":session,"toolCall":{"toolCallId":call,"title":title,"kind":kind,"status":"pending"},
                "options":[{"optionId":"allow-once","name":"Allow once","kind":"allow_once"},{"optionId":"reject-once","name":"Reject","kind":"reject_once"}]
            }});
            envelope["params"]["toolCall"]["rawInput"] = input;
            self.send(envelope).await?;
            rx.await.context("Permission response lost")
        };
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => bail!("Permission request cancelled"),
            result = tokio::time::timeout(Duration::from_secs(300), exchange) => result.context("Permission request timed out")??,
        };
        ensure!(
            result["result"]["outcome"]["outcome"] == "selected"
                && result["result"]["outcome"]["optionId"] == "allow-once",
            "Permission denied; do not retry this action through another tool"
        );
        Ok(())
    }

    pub async fn resolve(&self, value: Value) {
        if let Some(id) = value["id"].as_str()
            && let Some(sender) = self
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .remove(id)
        {
            let _ = sender.send(value);
        }
    }
}

async fn write_wire(
    writer: impl tokio::io::AsyncWrite + Unpin,
    mut outbound: mpsc::Receiver<WireFrame>,
) -> Result<()> {
    let mut writer = BufWriter::new(writer);
    while let Some(frame) = outbound.recv().await {
        let mut bytes = frame.bytes.len();
        writer.write_all(&frame.bytes).await?;
        drop(frame);
        // Drain only ready frames and flush before waiting for more work. This
        // batches bursts without adding a timer to permission/interactive output.
        for _ in 1..WIRE_BATCH_FRAMES {
            if bytes >= WIRE_BATCH_BYTES {
                break;
            }
            let Ok(frame) = outbound.try_recv() else {
                break;
            };
            bytes += frame.bytes.len();
            writer.write_all(&frame.bytes).await?;
            drop(frame);
        }
        writer.flush().await?;
    }
    Ok(())
}

#[cfg(test)]
mod transport_tests {
    use super::*;

    fn pending_count(peer: &Peer) -> usize {
        peer.pending.lock().unwrap().len()
    }

    #[tokio::test]
    async fn structured_envelopes_move_large_values_without_cloning() {
        let (peer, mut receiver) = Peer::channel(2);
        let value = json!({"text":"x".repeat(65536)});
        let pointer = value["text"].as_str().unwrap().as_ptr();
        peer.update("session", value).await.unwrap();
        let message = receiver.recv().await.unwrap();
        assert_eq!(
            message["params"]["update"]["text"]
                .as_str()
                .unwrap()
                .as_ptr(),
            pointer
        );
        let value = json!({"output":"y".repeat(65536)});
        let pointer = value["output"].as_str().unwrap().as_ptr();
        peer.task_event("session", "task", value).await.unwrap();
        let message = receiver.recv().await.unwrap();
        assert_eq!(
            message["params"]["event"]["output"]
                .as_str()
                .unwrap()
                .as_ptr(),
            pointer
        );
    }

    #[tokio::test]
    async fn wire_byte_budget_waits_before_encoding_and_releases_on_frame_drop() {
        let (peer, mut receiver) = Peer::wire_channel(4, 64);
        peer.send(json!("x".repeat(40))).await.unwrap();
        let sending = peer.clone();
        let waiting = tokio::spawn(async move { sending.send(json!("y".repeat(40))).await });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        let first = receiver.recv().await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&first.bytes).unwrap(),
            json!("x".repeat(40))
        );
        // Receiving alone does not release bytes while the writer owns a frame.
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(first);
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let second = receiver.recv().await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&second.bytes).unwrap(),
            json!("y".repeat(40))
        );
    }

    #[tokio::test]
    async fn permission_cancel_and_abort_cleanup_when_output_channel_is_full() {
        for abort in [false, true] {
            let (peer, mut receiver) = Peer::channel(1);
            peer.send(json!("occupied")).await.unwrap();
            let cancel = CancellationToken::new();
            let token = cancel.clone();
            let requesting = peer.clone();
            let task = tokio::spawn(async move {
                requesting
                    .permission("session", "call", "title", "execute", json!({}), &token)
                    .await
            });
            tokio::task::yield_now().await;
            assert_eq!(pending_count(&peer), 1);
            if abort {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                cancel.cancel();
                assert!(
                    tokio::time::timeout(Duration::from_secs(1), task)
                        .await
                        .unwrap()
                        .unwrap()
                        .is_err()
                );
            }
            assert_eq!(pending_count(&peer), 0);
            assert_eq!(receiver.recv().await.unwrap(), json!("occupied"));
            assert!(receiver.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn cancelled_wire_permission_releases_reserved_slot_and_pending_reply() {
        let (peer, mut receiver) = Peer::wire_channel(2, 32);
        // An existing large message is permitted exclusively, without truncation.
        peer.send(json!({"output":"x".repeat(1024)})).await.unwrap();
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let requesting = peer.clone();
        let task = tokio::spawn(async move {
            requesting
                .permission("session", "call", "title", "execute", json!({}), &token)
                .await
        });
        tokio::task::yield_now().await;
        assert_eq!(pending_count(&peer), 1);
        cancel.cancel();
        assert!(task.await.unwrap().is_err());
        assert_eq!(pending_count(&peer), 0);
        drop(receiver.recv().await.unwrap());
        tokio::time::timeout(
            Duration::from_secs(1),
            peer.send(json!("after cancellation")),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&receiver.recv().await.unwrap().bytes).unwrap(),
            json!("after cancellation")
        );
    }

    #[tokio::test]
    async fn wire_writer_flushes_without_waiting_and_preserves_ready_burst_order() {
        use tokio::io::AsyncBufReadExt;
        let (peer, outbound) = Peer::wire_channel(64, 4096);
        let (writer, reader) = tokio::io::duplex(128);
        let writing = tokio::spawn(write_wire(writer, outbound));
        let mut reader = BufReader::new(reader);
        peer.send(json!({"sequence":0})).await.unwrap();
        let mut first = String::new();
        tokio::time::timeout(Duration::from_secs(1), reader.read_line(&mut first))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&first).unwrap()["sequence"],
            0
        );
        let sending = tokio::spawn(async move {
            for sequence in 1..200 {
                peer.send(json!({"sequence":sequence})).await.unwrap();
            }
        });
        for sequence in 1..200 {
            let mut line = String::new();
            tokio::time::timeout(Duration::from_secs(1), reader.read_line(&mut line))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&line).unwrap()["sequence"],
                sequence
            );
        }
        sending.await.unwrap();
        writing.await.unwrap().unwrap();
    }
}

struct Slot {
    session: Mutex<Session>,
    persisted_bytes: AtomicUsize,
    workspace: Workspace,
    busy: Arc<Semaphore>,
    cancel: Mutex<CancellationToken>,
    jobs: Jobs,
    mailboxes: Arc<crate::agents::Mailboxes>,
    mcp: Arc<McpServers>,
    models: ModelCatalog,
}

// A hydrated transcript must also become cold on early validation/MCP failures.
// Metadata remains in the slot; saves reject cold history until it is hydrated.
struct HistoryLease<'a> {
    session: tokio::sync::MutexGuard<'a, Session>,
    limit: usize,
}

impl std::ops::Deref for HistoryLease<'_> {
    type Target = Session;
    fn deref(&self) -> &Session {
        &self.session
    }
}

impl std::ops::DerefMut for HistoryLease<'_> {
    fn deref_mut(&mut self) -> &mut Session {
        &mut self.session
    }
}

impl Drop for HistoryLease<'_> {
    fn drop(&mut self) {
        self.session.release_history(self.limit);
    }
}

pub struct Server {
    config: Arc<Config>,
    peer: Peer,
    provider: Provider,
    store: Store,
    sessions: Mutex<HashMap<Uuid, Arc<Slot>>>,
    initialized: AtomicBool,
    agents: Arc<Semaphore>,
    turns: Arc<Semaphore>,
    session_preparation: Arc<Semaphore>,
    processes: Arc<Semaphore>,
    searches: Arc<Semaphore>,
    shutdown: CancellationToken,
    mcp_pool: Arc<McpPool>,
    desktop: Arc<crate::computer::Desktop>,
    builtins: Arc<crate::agent::Builtins>,
}

impl Server {
    pub async fn new(mut config: Config, peer: Peer) -> Result<Self> {
        config.validate()?;
        tokio::fs::create_dir_all(&config.state_dir).await?;
        config.state_dir = tokio::fs::canonicalize(&config.state_dir).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&config.state_dir, std::fs::Permissions::from_mode(0o700))
                .await?;
        }
        let store = Store::new(config.state_dir.join("sessions")).await?;
        let provider = Provider::new(&config)?;
        let mcp_pool = Arc::new(McpPool::new(config.max_mcp_connections));
        Ok(Self {
            agents: Arc::new(Semaphore::new(config.max_subagents)),
            turns: Arc::new(Semaphore::new(config.max_active_turns)),
            session_preparation: Arc::new(Semaphore::new(config.max_active_turns.min(8))),
            processes: Arc::new(Semaphore::new(config.max_processes)),
            searches: Arc::new(Semaphore::new(config.max_searches)),
            config: Arc::new(config),
            peer,
            provider,
            store,
            sessions: Mutex::new(HashMap::new()),
            initialized: AtomicBool::new(false),
            shutdown: CancellationToken::new(),
            mcp_pool,
            desktop: Arc::new(crate::computer::Desktop::default()),
            builtins: Arc::new(crate::agent::Builtins::default()),
        })
    }

    pub async fn handle(&self, method: &str, params: Value) -> Result<Value> {
        if method == "initialize" {
            ensure!(
                params["protocolVersion"].is_u64(),
                "Missing protocolVersion"
            );
            self.initialized.store(true, Ordering::Release);
            return Ok(
                json!({"protocolVersion":1,"agentInfo":{"name":"piko","version":env!("CARGO_PKG_VERSION"),"title":"Piko"},
                "agentCapabilities":{"loadSession":true,"promptCapabilities":{},"mcpCapabilities":{"http":true,"sse":false},"_meta":{"harness":{"tasks":true,"boundedContext":true,"computerUse":self.config.computer_use && crate::computer::supported()}}},
                "authMethods":[{"id":"chatgpt","name":"ChatGPT subscription","description":"Run piko login before starting the agent"},{"id":"api_key","name":"Provider API key","description":"Set the configured api_key_env environment variable"}]}),
            );
        }
        ensure!(
            self.initialized.load(Ordering::Acquire),
            "Call initialize before session methods"
        );
        match method {
            "_harness/stats" => {
                let mut stats = self.provider.metrics.snapshot();
                stats["runtime"] = json!({
                    "loadedSessions": self.sessions.lock().await.len(),
                    "activeRootTurns": self.config.max_active_turns - self.turns.available_permits(),
                    "liveSubagents": self.config.max_subagents - self.agents.available_permits(),
                    "toolProcesses": self.config.max_processes - self.processes.available_permits(),
                    "toolProcessLimit": self.config.max_processes,
                });
                Ok(stats)
            }
            "_harness/models" => Ok(json!({"models":self.provider.models().await})),
            "authenticate" => {
                let method = string(&params, "methodId")?;
                ensure!(
                    ["chatgpt", "api_key"].contains(&method),
                    "Unknown authentication method"
                );
                let sessions = self.sessions.lock().await;
                ensure!(
                    sessions.is_empty(),
                    "Unload all sessions before changing authentication"
                );
                self.provider.authenticate(method).await?;
                Ok(json!({}))
            }
            "session/new" | "session/load" => {
                let _preparation = if method == "session/load" {
                    Some(tokio::select! {
                        biased;
                        _ = self.shutdown.cancelled() => bail!("Server shutting down"),
                        permit = self.session_preparation.acquire() => permit?,
                    })
                } else {
                    None
                };
                let mut workspace = Workspace::new(Path::new(string(&params, "cwd")?)).await?;
                workspace.protect(self.config.state_dir.clone());
                let mcp = Arc::new(McpServers::parse(
                    &params["mcpServers"],
                    &workspace.root,
                    self.mcp_pool.clone(),
                )?);
                let mut session = if method == "session/load" {
                    self.store
                        .load(Uuid::parse_str(string(&params, "sessionId")?)?, &workspace)
                        .await?
                } else {
                    Session::new(&workspace, &self.config)
                };
                // Discovery may perform network I/O; never hold the session registry lock.
                let models = self.provider.models().await;
                if session.version == 1 {
                    session.reasoning_effort = self.config.reasoning_effort.clone();
                    session.version = 2;
                }
                if let Some(model) = models.iter().find(|model| model.id == session.model)
                    && (session.reasoning_effort.as_deref().is_some_and(|effort| {
                        (model.reasoning_capabilities_known || !model.reasoning_efforts.is_empty())
                            && !self.effort_allowed(model, effort)
                    }) || (method == "session/new" && session.reasoning_effort.is_none()))
                {
                    session.reasoning_effort = model.default_reasoning_effort.clone();
                }
                let id = session.id;
                let slot = Arc::new(Slot {
                    session: Mutex::new(session),
                    persisted_bytes: AtomicUsize::new(0),
                    workspace,
                    busy: Arc::new(Semaphore::new(0)),
                    cancel: Mutex::new(self.shutdown.child_token()),
                    jobs: Jobs::with_process_limit(
                        self.config.max_jobs,
                        self.config.tool_output_bytes,
                        self.processes.clone(),
                    ),
                    mailboxes: Arc::new(crate::agents::Mailboxes::default()),
                    mcp,
                    models,
                });
                let mut sessions = self.sessions.lock().await;
                ensure!(
                    sessions.len() < self.config.max_sessions,
                    "Loaded session limit reached; unload a session first"
                );
                ensure!(!sessions.contains_key(&id), "Session already loaded");
                sessions.insert(id, slot.clone());
                drop(sessions);
                let result: Result<()> = async {
                    let mut session = slot.session.lock().await;
                    let bytes = self.store.save_with_size(&session).await?;
                    slot.persisted_bytes.store(bytes, Ordering::Relaxed);
                    if method == "session/load" {
                        self.replay(&session).await?;
                    }
                    session.release_history(self.config.resident_history_bytes);
                    Ok(())
                }
                .await;
                if let Err(error) = result {
                    self.sessions.lock().await.remove(&id);
                    return Err(error);
                }
                slot.busy.add_permits(1);
                let session = slot.session.lock().await;
                Ok(json!({"sessionId":id,"modes":modes(&session.mode),
                    "models":{"currentModelId":session.model,"availableModels":self.available_models(&session,&slot.models)},
                    "configOptions":self.config_options(&session,&slot.models)}))
            }
            "session/prompt" => self.prompt(params).await,
            "session/cancel" => {
                let slot = self.slot(&params).await?;
                slot.cancel.lock().await.cancel();
                Ok(json!({}))
            }
            "session/set_model" => {
                self.set_option(&params, "model", "modelId").await?;
                Ok(json!({}))
            }
            "session/set_mode" => {
                self.set_option(&params, "mode", "modeId").await?;
                Ok(json!({}))
            }
            "session/set_config_option" => {
                let config_id = selection(&params, "configId")?;
                self.set_option(&params, config_id, "value").await
            }
            "_harness/tasks" => {
                let slot = self.slot(&params).await?;
                Ok(json!({"tasks":slot.jobs.list().await}))
            }
            "_harness/task/cancel" => {
                let slot = self.slot(&params).await?;
                slot.jobs
                    .get(string(&params, "taskId")?)
                    .await?
                    .cancel
                    .cancel();
                Ok(json!({}))
            }
            "_harness/session/unload" => {
                let slot = self.slot(&params).await?;
                let _permit = slot
                    .busy
                    .clone()
                    .try_acquire_owned()
                    .context("Session is busy")?;
                slot.cancel.lock().await.cancel();
                slot.jobs.cancel_all().await;
                let id = Uuid::parse_str(string(&params, "sessionId")?)?;
                self.sessions.lock().await.remove(&id);
                Ok(json!({}))
            }
            _ => bail!("Method not found"),
        }
    }

    fn effort_allowed(&self, model: &ModelInfo, effort: &str) -> bool {
        model
            .reasoning_efforts
            .iter()
            .any(|option| option.effort == effort)
            || (!model.reasoning_capabilities_known
                && model.reasoning_efforts.is_empty()
                && model.id == self.config.model
                && self.config.reasoning_effort.as_deref() == Some(effort))
    }

    fn reasoning_options(&self, model: &ModelInfo, saved: Option<&str>) -> Vec<Value> {
        let mut options = vec![
            json!({"value":"default","name":"Provider default","description":"Let the provider choose its default reasoning effort"}),
        ];
        for option in &model.reasoning_efforts {
            if option.effort != "default" {
                options.push(json!({"value":option.effort,"name":effort_name(&option.effort),"description":short(&option.description,256)}));
            }
        }
        if !model.reasoning_capabilities_known
            && model.reasoning_efforts.is_empty()
            && model.id == self.config.model
            && let Some(effort) = &self.config.reasoning_effort
        {
            options.push(json!({"value":effort,"name":effort_name(effort),"description":"From your trusted provider configuration"}));
        }
        if !model.reasoning_capabilities_known
            && model.reasoning_efforts.is_empty()
            && let Some(effort) = saved
            && effort != "default"
            && !options.iter().any(|option| option["value"] == effort)
        {
            options.push(json!({"value":effort,"name":effort_name(effort),"description":"Saved setting; provider capabilities are unavailable"}));
        }
        options
    }

    fn available_models(&self, session: &Session, catalog: &[ModelInfo]) -> Vec<Value> {
        let mut models: Vec<Value> = catalog.iter().map(|model| {
            let efforts: Vec<Value> = self.reasoning_options(model, if model.id == session.model { session.reasoning_effort.as_deref() } else { None }).into_iter().map(|value| value["value"].clone()).collect();
            let mut value = json!({"modelId":model.id,"name":short(&model.name,128),"_meta":{"harness":{"reasoningEfforts":efforts}}});
            if let Some(description) = &model.description {
                value["description"] = json!(short(description,256));
            }
            value
        }).collect();
        if !catalog.iter().any(|model| model.id == session.model) {
            let fallback = ModelInfo::configured(&session.model);
            let efforts: Vec<Value> = self
                .reasoning_options(&fallback, session.reasoning_effort.as_deref())
                .into_iter()
                .map(|option| option["value"].clone())
                .collect();
            models.push(json!({"modelId":session.model,"name":short(&session.model,128),"description":"Model retained by this saved session","_meta":{"harness":{"reasoningEfforts":efforts}}}));
        }
        models
    }

    fn config_options(&self, session: &Session, catalog: &[ModelInfo]) -> Value {
        let options: Vec<Value> = self
            .available_models(session, catalog)
            .into_iter()
            .map(|model| {
                let mut option =
                    json!({"value":model["modelId"],"name":model["name"],"_meta":model["_meta"]});
                if model["description"].is_string() {
                    option["description"] = model["description"].clone();
                }
                option
            })
            .collect();
        let fallback = ModelInfo::configured(&session.model);
        let model = catalog
            .iter()
            .find(|model| model.id == session.model)
            .unwrap_or(&fallback);
        json!([
            {"id":"model","name":"Model","category":"model","type":"select","currentValue":session.model,"options":options},
            {"id":"reasoning_effort","name":"Reasoning effort","category":"thought_level","type":"select","currentValue":session.reasoning_effort.as_deref().unwrap_or("default"),"options":self.reasoning_options(model, session.reasoning_effort.as_deref())},
            {"id":"mode","name":"Session mode","category":"mode","type":"select","currentValue":session.mode,"options":[
                {"value":"code","name":"Code","description":"Explore and edit; writes and shell commands require approval"},
                {"value":"plan","name":"Plan","description":"Read-only exploration; writes and shell commands disabled"}
            ]}
        ])
    }

    async fn set_option(&self, params: &Value, config_id: &str, value_key: &str) -> Result<Value> {
        let value = selection(params, value_key)?;
        let slot = self.slot(params).await?;
        let _permit = slot
            .busy
            .clone()
            .try_acquire_owned()
            .context("Session is busy")?;
        let mut session = HistoryLease {
            session: slot.session.lock().await,
            limit: self.config.resident_history_bytes,
        };
        let previous = (
            session.model.clone(),
            session.reasoning_effort.clone(),
            session.mode.clone(),
        );
        match config_id {
            "model" => {
                if value != session.model {
                    let model = slot
                        .models
                        .iter()
                        .find(|model| model.id == value)
                        .ok_or(InvalidParams("Model is not available in this session"))?;
                    session.reasoning_effort = session
                        .reasoning_effort
                        .take()
                        .filter(|effort| self.effort_allowed(model, effort))
                        .or_else(|| model.default_reasoning_effort.clone());
                    session.model = value.to_owned();
                }
            }
            "reasoning_effort" => {
                if value == "default" {
                    session.reasoning_effort = None;
                } else {
                    let fallback = ModelInfo::configured(&session.model);
                    let model = slot
                        .models
                        .iter()
                        .find(|model| model.id == session.model)
                        .unwrap_or(&fallback);
                    let saved_unknown_effort = !model.reasoning_capabilities_known
                        && model.reasoning_efforts.is_empty()
                        && session.reasoning_effort.as_deref() == Some(value);
                    if !self.effort_allowed(model, value) && !saved_unknown_effort {
                        return Err(InvalidParams(
                            "Reasoning effort is not available for the selected model",
                        )
                        .into());
                    }
                    session.reasoning_effort = Some(value.to_owned());
                }
            }
            "mode" => {
                if !["code", "plan"].contains(&value) {
                    return Err(InvalidParams("Unknown session mode").into());
                }
                if value != session.mode {
                    ensure!(
                        !slot
                            .jobs
                            .list()
                            .await
                            .iter()
                            .any(|job| job["status"] == "running"),
                        "Cancel running background tasks before changing mode"
                    );
                    session.mode = value.to_owned();
                }
            }
            _ => return Err(InvalidParams("Unknown configuration option").into()),
        }
        // Clients may reselect the same model before every prompt. Avoid rewriting
        // the full persisted history or emitting duplicate updates for a no-op.
        if previous.0 == session.model
            && previous.1 == session.reasoning_effort
            && previous.2 == session.mode
        {
            return Ok(json!({"configOptions":self.config_options(&session,&slot.models)}));
        }
        // Hold admission through hydrate + save + cold-history release; a file
        // read limit alone would queue fully decoded histories before their saves.
        let _preparation = tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => bail!("Server shutting down"),
            permit = self.session_preparation.acquire() => permit?,
        };
        if let Err(error) = self.store.hydrate(&mut session, &slot.workspace).await {
            (session.model, session.reasoning_effort, session.mode) = previous;
            return Err(error);
        }
        match self.store.save_with_size(&session).await {
            Ok(bytes) => slot.persisted_bytes.store(bytes, Ordering::Relaxed),
            Err(error) => {
                (session.model, session.reasoning_effort, session.mode) = previous;
                return Err(error);
            }
        }
        session.release_history(self.config.resident_history_bytes);
        if previous.2 != session.mode {
            self.peer
                .update(
                    &session.id.to_string(),
                    json!({"sessionUpdate":"current_mode_update","currentModeId":session.mode}),
                )
                .await?;
        }
        let config = self.config_options(&session, &slot.models);
        self.peer
            .update(
                &session.id.to_string(),
                json!({"sessionUpdate":"config_option_update","configOptions":config}),
            )
            .await?;
        Ok(json!({"configOptions":config}))
    }

    async fn slot(&self, params: &Value) -> Result<Arc<Slot>> {
        let id = Uuid::parse_str(string(params, "sessionId")?)?;
        self.sessions
            .lock()
            .await
            .get(&id)
            .cloned()
            .context("Unknown session; create or load it first")
    }

    async fn prompt(&self, params: Value) -> Result<Value> {
        let slot = self.slot(&params).await?;
        let _permit = slot
            .busy
            .clone()
            .try_acquire_owned()
            .context("Session is busy; cancel or wait for its active turn")?;
        let cancel = self.shutdown.child_token();
        *slot.cancel.lock().await = cancel.clone();
        // Queue the original prompt, not expanded model history/instructions.
        // Children use the independent model-request pool, so a parent awaiting
        // a child cannot exhaust this top-level turn gate and deadlock it.
        let _turn = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(json!({"stopReason":"cancelled"})),
            permit = self.turns.acquire() => permit?,
        };
        let text = self.prompt_text(&params, &slot.workspace).await?;
        drop(params);
        ensure!(!text.trim().is_empty(), "Prompt is empty");
        ensure!(
            text.len() <= self.config.context_bytes / 2,
            "Prompt exceeds context budget"
        );
        let mut session = HistoryLease {
            session: slot.session.lock().await,
            limit: self.config.resident_history_bytes,
        };
        if let Some(bytes) = self
            .store
            .hydrate_with_size(&mut session, &slot.workspace)
            .await?
        {
            slot.persisted_bytes.store(bytes, Ordering::Relaxed);
        }
        let ctx = AgentContext {
            config: self.config.clone(),
            provider: self.provider.clone(),
            peer: self.peer.clone(),
            workspace: slot.workspace.clone(),
            jobs: slot.jobs.clone(),
            mailboxes: slot.mailboxes.clone(),
            builtins: self.builtins.clone(),
            mcp: slot.mcp.clone(),
            desktop: self.desktop.clone(),
            agents: self.agents.clone(),
            searches: self.searches.clone(),
            session_id: session.id.to_string(),
            agent_id: None,
            model: session.model.clone(),
            reasoning_effort: session.reasoning_effort.clone(),
            depth: 0,
            read_only: session.mode == "plan",
            cancel: cancel.clone(),
        };
        let instructions = crate::agent::instructions(&ctx).await?;
        let toolset = match crate::agent::prepare(&ctx).await {
            Ok(toolset) => toolset,
            Err(_) if cancel.is_cancelled() => return Ok(json!({"stopReason":"cancelled"})),
            Err(error) => return Err(error),
        };
        let mut user = json!({"role":"user"});
        user["content"] = Value::String(text);
        let reserved = crate::session::json_bytes(&user)?
            + instructions.len()
            + toolset.schemas.encoded.get().len()
            + 512;
        ensure!(
            reserved <= self.config.context_bytes,
            "Prompt and project instructions exceed context budget"
        );
        let (mut prefix, omitted) = session.project(self.config.context_bytes - reserved);
        if omitted + session.dropped_turns > 0 {
            prefix.insert(0,std::borrow::Cow::Owned(json!({"role":"developer","content":format!("{} older turns are outside the model window. Do not assume their contents; re-read the workspace as needed. Saved history is separate from this window.",omitted + session.dropped_turns)})));
        }
        let start = 0;
        let mut items = vec![user];
        let base_bytes = slot.persisted_bytes.load(Ordering::Relaxed);
        // Check capacity before spending a request. Durable intent is written
        // before the first tool boundary; read-only replies need one final save.
        ensure!(
            base_bytes
                .saturating_add(crate::session::json_bytes(&items[start..])?)
                .saturating_add(1024)
                <= crate::session::MAX_SESSION_BYTES as usize,
            "Session reached its 16MiB persistence limit; start a new session. Saved history is preserved."
        );
        let result = crate::agent::run_with_instructions(
            &ctx,
            &mut items,
            &mut prefix,
            instructions,
            toolset,
            Some(crate::agent::Checkpoint {
                store: &self.store,
                session: &session,
                start,
                base_bytes,
            }),
        )
        .await;
        drop(prefix);
        // Save complete exchanges only; interrupted responses never enter model history.
        let mut turn = std::mem::take(&mut items);
        repair_calls(&mut turn);
        session.turns.push_back(Turn { items: turn.into() });
        let saved = self.store.save_with_size(&session).await;
        *slot.cancel.lock().await = self.shutdown.child_token();
        if let Err(error) = saved {
            // Never retain an unbounded unsaved turn, or overwrite its recovery
            // checkpoint with a later prompt. Reload explicitly after a save failure.
            session.turns.pop_back();
            self.sessions.lock().await.remove(&session.id);
            slot.jobs.cancel_all().await;
            return Err(
                error.context("Could not commit session; reload it to recover the last checkpoint")
            );
        }
        slot.persisted_bytes
            .store(saved.expect("successful session save"), Ordering::Relaxed);
        // Free projected old turns before releasing the durable idle history.
        drop(items);
        session.release_history(self.config.resident_history_bytes);
        if cancel.is_cancelled() {
            return Ok(json!({"stopReason":"cancelled"}));
        }
        let result = result?;
        Ok(json!({"stopReason":result.stop_reason}))
    }

    async fn prompt_text(&self, params: &Value, workspace: &Workspace) -> Result<String> {
        let blocks = params["prompt"]
            .as_array()
            .context("Missing prompt array")?;
        let mut text = String::new();
        for block in blocks {
            match block["type"].as_str() {
                Some("text") => text.push_str(string(block, "text")?),
                Some("resource_link") => {
                    let uri = string(block, "uri")?;
                    if uri.starts_with("file:") {
                        let path = reqwest::Url::parse(uri)?
                            .to_file_path()
                            .map_err(|_| anyhow::anyhow!("Invalid file resource URI"))?;
                        let path = workspace
                            .resolve(path.to_str().context("Invalid resource path")?, false)
                            .await?;
                        text.push_str(&format!(
                            "Resource {}:\n{}",
                            path.display(),
                            read_bounded(&path, self.config.tool_output_bytes).await?
                        ));
                    } else {
                        text.push_str(&format!("Resource link: {uri}"));
                    }
                }
                _ => bail!("Only text and resource_link prompts are supported"),
            }
            text.push('\n');
            ensure!(
                text.len() <= self.config.context_bytes / 2,
                "Prompt exceeds context budget"
            );
        }
        Ok(text)
    }

    async fn replay(&self, session: &Session) -> Result<()> {
        for turn in &session.turns {
            for item in turn.items.iter() {
                if item["type"] == "function_call" {
                    let input = item["arguments"]
                        .as_str()
                        .and_then(|args| serde_json::from_str::<Value>(args).ok());
                    self.peer.update(&session.id.to_string(),json!({"sessionUpdate":"tool_call","toolCallId":item["call_id"],"title":item["name"],"kind":"other","status":"in_progress","rawInput":input})).await?;
                } else if item["type"] == "function_call_output" {
                    let text = crate::multimodal::text(&item["output"]);
                    let output = serde_json::from_str::<Value>(&text).ok();
                    let failed = text.starts_with("Tool reported an error.")
                        || output
                            .as_ref()
                            .is_some_and(|output| output.get("error").is_some());
                    self.peer.update(&session.id.to_string(),json!({"sessionUpdate":"tool_call_update","toolCallId":item["call_id"],"status":if failed {"failed"} else {"completed"},"content":crate::multimodal::acp_content(&item["output"])})).await?;
                }
                if let Some(role) = item["role"].as_str() {
                    let text = if let Some(text) = item["content"].as_str() {
                        text.to_owned()
                    } else {
                        item["content"]
                            .as_array()
                            .map(|c| {
                                c.iter()
                                    .filter_map(|c| c["text"].as_str())
                                    .collect::<Vec<_>>()
                                    .join("")
                            })
                            .unwrap_or_default()
                    };
                    if !text.is_empty() && ["user", "assistant"].contains(&role) {
                        self.peer.update(&session.id.to_string(),json!({"sessionUpdate":if role == "user" {"user_message_chunk"} else {"agent_message_chunk"},"content":{"type":"text","text":text}})).await?;
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        for slot in self.sessions.lock().await.values() {
            slot.jobs.cancel_all().await;
        }
    }
}

pub(crate) fn unanswered_outputs(items: &[Value]) -> Vec<Value> {
    let mut pending = Vec::new();
    // Match in sequence: compatible providers may reuse an ID in a later step.
    // An older result must not hide a newly interrupted call with the same ID.
    for item in items {
        if let Some(id) = item["call_id"].as_str() {
            if item["type"] == "function_call" {
                pending.push(id);
            } else if item["type"] == "function_call_output"
                && let Some(index) = pending.iter().position(|pending| *pending == id)
            {
                pending.remove(index);
            }
        }
    }
    pending.into_iter().map(|id| json!({"type":"function_call_output","call_id":id,"output":json!({"error":"Tool was interrupted and not executed to completion. Inspect current state before retrying."}).to_string()})).collect()
}

pub fn repair_calls(items: &mut Vec<Value>) {
    items.extend(unanswered_outputs(items));
}

fn short(text: &str, limit: usize) -> String {
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

fn effort_name(effort: &str) -> String {
    match effort {
        "none" => "None".into(),
        "minimal" => "Minimal".into(),
        "low" => "Low".into(),
        "medium" => "Medium".into(),
        "high" => "High".into(),
        "xhigh" => "Extra high".into(),
        "max" => "Max".into(),
        "ultra" => "Ultra".into(),
        _ => effort.into(),
    }
}

fn modes(current: &str) -> Value {
    json!({"currentModeId":current,"availableModes":[{"id":"code","name":"Code","description":"Workspace reads; edits and shell require approval"},{"id":"plan","name":"Plan","description":"Read-only exploration; no writes or shell, including subagents"}]})
}

pub async fn read_frame(reader: &mut (impl AsyncBufRead + Unpin)) -> Result<Option<Vec<u8>>> {
    let mut frame = Vec::new();
    loop {
        let buffer = reader.fill_buf().await?;
        if buffer.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Ok(Some(frame))
            };
        }
        let count = buffer
            .iter()
            .position(|b| *b == b'\n')
            .map_or(buffer.len(), |i| i + 1);
        ensure!(
            frame.len() + count <= MAX_FRAME,
            "ACP message exceeds 1MiB limit"
        );
        let complete = buffer[count - 1] == b'\n';
        frame.extend_from_slice(&buffer[..count]);
        reader.consume(count);
        if complete {
            return Ok(Some(frame));
        }
    }
}

pub async fn serve(config: Config) -> Result<()> {
    let pending_prompts = Arc::new(Semaphore::new(config.max_pending_prompt_bytes));
    let (peer, outbound) = Peer::wire_channel(64, WIRE_QUEUE_BYTES);
    let server = Arc::new(Server::new(config, peer.clone()).await?);
    let writer = tokio::spawn(write_wire(tokio::io::stdout(), outbound));
    let mut reader = BufReader::new(tokio::io::stdin());
    let mut handlers = JoinSet::new();
    let signal = shutdown_signal();
    tokio::pin!(signal);
    let result: Result<()> = async {
        loop {
            let frame = tokio::select! {
                biased;
                _ = &mut signal => break,
                _ = peer.closed() => break,
                frame = read_frame(&mut reader) => frame?,
            };
            let Some(frame) = frame else {
                break;
            };
            if frame.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let mut value: Value = match serde_json::from_slice(&frame) {
                Ok(value) => value,
                Err(_) => {
                    peer.send(error(Value::Null, -32700, "Parse error")).await?;
                    continue;
                }
            };
            if value["jsonrpc"] != "2.0" || !value.is_object() {
                peer.send(error(Value::Null, -32600, "Invalid JSON-RPC request"))
                    .await?;
                continue;
            }
            if value.get("method").is_none() {
                peer.resolve(value).await;
                continue;
            }
            let Some(method) = value["method"].as_str() else {
                peer.send(error(value["id"].clone(), -32600, "Invalid method"))
                    .await?;
                continue;
            };
            let method = method.to_owned();
            let params = value.get_mut("params").map(Value::take).unwrap_or_else(|| json!({}));
            if value.get("id").is_none() {
                if method == "session/cancel" {
                    let _ = server.handle(&method, params).await;
                }
                continue;
            }
            if !(value["id"].is_string() || value["id"].is_number()) {
                peer.send(error(Value::Null, -32600, "Invalid JSON-RPC request ID"))
                    .await?;
                continue;
            }
            while handlers.try_join_next().is_some() {}
            if handlers.len() >= 2048 {
                peer.send(error(
                    value["id"].clone(),
                    -32000,
                    "Too many concurrent requests",
                ))
                .await?;
                continue;
            }
            let server = server.clone();
            let peer = peer.clone();
            // Count queued AND executing prompt frames. Reject overload without
            // blocking the reader: permission replies/cancellation must still flow.
            let prompt_bytes = if method == "session/prompt" {
                match pending_prompts.clone().try_acquire_many_owned(frame.len() as u32) {
                    Ok(permit) => Some(permit),
                    Err(_) => {
                        peer.send(error(value["id"].clone(), -32000,
                            "Pending prompt byte budget exhausted; retry after an active turn completes"))
                            .await?;
                        continue;
                    }
                }
            } else { None };
            let id = value["id"].take();
            drop(value);
            handlers.spawn(async move {
                let _prompt_bytes = prompt_bytes;
                let response = match server.handle(&method, params).await {
                    Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
                    Err(err) => {
                        let code = if err.to_string() == "Method not found" {
                            -32601
                        } else if err.downcast_ref::<InvalidParams>().is_some() {
                            -32602
                        } else {
                            -32000
                        };
                        error(id, code, &format!("{err:#}"))
                    }
                };
                let _ = peer.send(response).await;
            });
            tokio::task::yield_now().await;
        }
        Ok(())
    }
    .await;
    server.shutdown().await;
    // Give cancelled turns time to checkpoint, but never wait indefinitely on a dead client.
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while handlers.join_next().await.is_some() {}
    })
    .await;
    handlers.abort_all();
    while handlers.join_next().await.is_some() {}
    drop(server);
    drop(peer);
    let _ = tokio::time::timeout(Duration::from_secs(2), writer).await;
    result
}

fn error(id: Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::Notify,
    };

    #[tokio::test]
    async fn history_lease_releases_hydrated_transcripts_on_early_return() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = Workspace::new(temp.path()).await.unwrap();
        let store = Store::new(temp.path().join("sessions")).await.unwrap();
        let mut data = Session::new(&workspace, &Config::default());
        data.turns.push_back(Turn {
            items: vec![json!({"role":"user","content":"saved"})].into(),
        });
        store.save(&data).await.unwrap();
        data.release_history(0);
        let slot = Mutex::new(data);
        let fail = async {
            let mut lease = HistoryLease {
                session: slot.lock().await,
                limit: 0,
            };
            store.hydrate(&mut lease, &workspace).await?;
            ensure!(false, "Fixture preparation failure");
            Ok::<_, anyhow::Error>(())
        }
        .await;
        assert!(fail.is_err());
        let data = slot.lock().await;
        assert!(data.turns.is_empty());
        assert_eq!(
            store.load(data.id, &workspace).await.unwrap().turns.len(),
            1
        );
    }

    #[tokio::test]
    async fn slow_model_discovery_does_not_block_session_cancellation() {
        let temp = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let fixture = tokio::spawn({
            let started = started.clone();
            let release = release.clone();
            async move {
                for index in 0..2 {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut stream = BufReader::new(stream);
                    let mut line = String::new();
                    loop {
                        line.clear();
                        stream.read_line(&mut line).await.unwrap();
                        if line == "\r\n" {
                            break;
                        }
                    }
                    if index == 1 {
                        started.notify_one();
                        release.notified().await;
                    }
                    stream.get_mut().write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                }
            }
        });
        let config = Config {
            auth: "none".into(),
            base_url: format!("http://{address}/v1"),
            state_dir: temp.path().join("state"),
            ..Config::default()
        };
        let (peer, _outbound) = Peer::channel(16);
        let server = Arc::new(Server::new(config, peer).await.unwrap());
        server
            .handle("initialize", json!({"protocolVersion":1}))
            .await
            .unwrap();
        let params = json!({"cwd":temp.path(), "mcpServers":[]});
        let first = server.handle("session/new", params.clone()).await.unwrap();
        server.provider.invalidate_models().await;
        let creation = tokio::spawn({
            let server = server.clone();
            async move { server.handle("session/new", params).await }
        });
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        let cancelled = tokio::time::timeout(
            Duration::from_millis(250),
            server.handle("session/cancel", json!({"sessionId":first["sessionId"]})),
        )
        .await;
        release.notify_one();
        assert!(cancelled.unwrap().is_ok());
        assert!(creation.await.unwrap().is_ok());
        fixture.await.unwrap();
    }
}
