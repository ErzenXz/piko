use crate::{config::Config, workspace::Workspace};
use anyhow::{Context, Result, ensure};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{borrow::Cow, collections::VecDeque, io::Write, path::PathBuf, sync::Arc};
use uuid::Uuid;

pub(crate) const MAX_SESSION_BYTES: u64 = 16 * 1024 * 1024;

// Count encoded JSON without allocating a second copy of the active context.
pub(crate) fn json_bytes(value: &(impl Serialize + ?Sized)) -> Result<usize> {
    Ok(json_bytes_up_to(value, usize::MAX)?.expect("unbounded counter"))
}

fn json_bytes_up_to(value: &(impl Serialize + ?Sized), limit: usize) -> Result<Option<usize>> {
    struct Counter {
        bytes: usize,
        limit: usize,
        exceeded: bool,
    }
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes) {
                self.exceeded = true;
                return Err(std::io::Error::other("JSON size threshold exceeded"));
            }
            self.bytes += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter {
        bytes: 0,
        limit,
        exceeded: false,
    };
    match serde_json::to_writer(&mut counter, value) {
        Err(_) if counter.exceeded => Ok(None),
        Err(error) => Err(error.into()),
        Ok(()) => Ok(Some(counter.bytes)),
    }
}

const SMALL_SAVE_BYTES: usize = 256 * 1024;
const PERSISTENCE_LIMIT_ERROR: &str =
    "Session reached its 16MiB persistence limit; start a new session. Saved history is preserved.";

// Keep common small snapshots on the event-loop allocator. Large snapshots
// still stream on workers and never need a full-session encoded allocation.
fn encode_small(value: &impl Serialize, limit: usize) -> Result<Option<Vec<u8>>> {
    struct SmallBuffer {
        bytes: Vec<u8>,
        limit: usize,
        threshold_exceeded: bool,
    }
    impl Write for SmallBuffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                return Err(std::io::Error::other(PERSISTENCE_LIMIT_ERROR));
            }
            if bytes.len() > SMALL_SAVE_BYTES.saturating_sub(self.bytes.len()) {
                self.threshold_exceeded = true;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "Small snapshot threshold exceeded",
                ));
            }
            let needed = self.bytes.len() + bytes.len();
            if needed > self.bytes.capacity() {
                let capacity = needed
                    .max(self.bytes.capacity().saturating_mul(2))
                    .min(SMALL_SAVE_BYTES);
                self.bytes.reserve_exact(capacity - self.bytes.len());
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut buffer = SmallBuffer {
        bytes: Vec::new(),
        limit,
        threshold_exceeded: false,
    };
    match serde_json::to_writer(&mut buffer, value) {
        Ok(()) => Ok(Some(buffer.bytes)),
        // Custom Serialize failures stay failures. Only our writer's threshold
        // signal permits a second serialization through the large-file path.
        Err(error)
            if buffer.threshold_exceeded
                && error.io_error_kind() == Some(std::io::ErrorKind::WouldBlock) =>
        {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

enum Snapshot<T> {
    Encoded(Vec<u8>),
    Streamed(T),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Turn {
    pub items: Arc<Vec<Value>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    pub version: u32,
    pub id: Uuid,
    pub cwd: PathBuf,
    pub model: String,
    pub mode: String,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    pub turns: VecDeque<Turn>,
    pub dropped_turns: usize,
    #[serde(skip)]
    history_evicted: bool,
}

impl Session {
    pub fn new(workspace: &Workspace, config: &Config) -> Self {
        Self {
            version: 2,
            id: Uuid::new_v4(),
            cwd: workspace.root.clone(),
            model: config.model.clone(),
            mode: "code".into(),
            reasoning_effort: config.reasoning_effort.clone(),
            turns: VecDeque::new(),
            dropped_turns: 0,
            history_evicted: false,
        }
    }

    fn validate_identifiers(&self) -> Result<()> {
        ensure!(
            crate::config::model_identifier(&self.model),
            "Invalid persisted model identifier"
        );
        ensure!(
            self.reasoning_effort.as_ref().is_none_or(|effort| {
                !effort.is_empty()
                    && effort.len() <= 64
                    && effort
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            }),
            "Invalid persisted reasoning identifier"
        );
        Ok(())
    }

    /// Project whole turns into the model window without deleting saved history.
    pub fn context(&self, budget: usize) -> Vec<Value> {
        self.context_with_omitted(budget).0
    }

    pub fn context_with_omitted(&self, budget: usize) -> (Vec<Value>, usize) {
        let (items, omitted) = self.project(budget);
        (items.into_iter().map(Cow::into_owned).collect(), omitted)
    }

    /// Borrow immutable saved exchanges; only image-budget replacements need COW.
    pub fn project(&self, budget: usize) -> (Vec<Cow<'_, Value>>, usize) {
        let mut bytes = 2usize; // JSON array brackets and item separators.
        let mut keep = 0;
        for turn in self.turns.iter().rev() {
            let Some(size) = json_bytes_up_to(&turn.items, budget.saturating_sub(bytes))
                .ok()
                .flatten()
            else {
                break;
            };
            bytes = bytes.saturating_add(size);
            keep += 1;
        }
        let omitted = self.turns.len() - keep;
        let items = self
            .turns
            .iter()
            .skip(omitted)
            .flat_map(|turn| turn.items.iter().map(Cow::Borrowed))
            .collect();
        (items, omitted)
    }

    pub fn bytes(&self) -> usize {
        self.turns.iter().map(|t| json_bytes(t).unwrap_or(0)).sum()
    }

    /// Only call after a successful durable save. Metadata stays available to
    /// pickers; the next admitted turn reloads history before constructing input.
    pub fn release_history(&mut self, resident_limit: usize) {
        if self.history_evicted {
            return;
        }
        let mut remaining = resident_limit;
        let exceeds = self
            .turns
            .iter()
            .any(|turn| match json_bytes_up_to(turn, remaining) {
                Ok(Some(bytes)) => {
                    remaining -= bytes;
                    false
                }
                _ => true,
            });
        if exceeds {
            self.turns = VecDeque::new();
            self.history_evicted = true;
        }
    }
}

#[derive(Clone)]
pub struct Store {
    root: PathBuf,
    io: Arc<tokio::sync::Semaphore>,
}

impl Store {
    pub async fn new(root: PathBuf) -> Result<Self> {
        tokio::fs::create_dir_all(&root).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).await?;
        }
        Ok(Self {
            root,
            io: Arc::new(tokio::sync::Semaphore::new(8)),
        })
    }

    pub async fn save(&self, session: &Session) -> Result<()> {
        self.save_with_size(session).await.map(|_| ())
    }

    pub(crate) async fn save_with_size(&self, session: &Session) -> Result<usize> {
        ensure!(
            !session.history_evicted,
            "Reload cold history before saving"
        );
        session.validate_identifiers()?;
        let bytes = self
            .save_value(
                session.id,
                false,
                session.clone(),
                MAX_SESSION_BYTES as usize,
                None,
            )
            .await?;
        // The committed conversation is durable before retiring its checkpoint.
        match tokio::fs::remove_file(self.active_path(session.id)).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(bytes)
    }

    /// Save only this turn; completed history is not rewritten per tool step.
    pub async fn save_active(
        &self,
        session: &Session,
        active: &[Value],
        base_bytes: usize,
    ) -> Result<()> {
        ensure!(
            !session.history_evicted,
            "Reload cold history before checkpointing"
        );
        session.validate_identifiers()?;
        let permit = self.io.clone().acquire_owned().await?;
        // Recovery may need synthetic results for unanswered calls; reserve their
        // exact encoded size so a full session can still be loaded after a crash.
        let recovery = json_bytes(&crate::protocol::unanswered_outputs(active))?;
        let remaining = (MAX_SESSION_BYTES as usize)
            .saturating_sub(base_bytes)
            .saturating_sub(recovery)
            .saturating_sub(32);
        let snapshot = ActiveCheckpoint {
            session_id: session.id,
            base_turns: session.turns.len(),
            items: serde_json::value::to_raw_value(active)?,
        };
        self.save_value(session.id, true, snapshot, remaining, Some(permit))
            .await
            .map(|_| ())
    }

    fn active_path(&self, id: Uuid) -> PathBuf {
        self.root.join(format!("{id}.active.json"))
    }

    async fn save_value(
        &self,
        id: Uuid,
        active: bool,
        value: impl Serialize + Send + 'static,
        limit: usize,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<usize> {
        let permit = match permit {
            Some(permit) => permit,
            None => self.io.clone().acquire_owned().await?,
        };
        let value = match encode_small(&value, limit)? {
            Some(bytes) => {
                drop(value);
                Snapshot::Encoded(bytes)
            }
            None => Snapshot::Streamed(value),
        };
        let root = self.root.clone();
        let target = if active {
            self.active_path(id)
        } else {
            root.join(format!("{id}.json"))
        };
        let temporary = root.join(format!(".{id}.{}.tmp", Uuid::new_v4()));
        // Small snapshots reuse main-thread allocation locality; large immutable
        // turns share storage with a shallow snapshot and stream through one
        // fixed worker buffer. Failed writes leave the old file intact.
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            struct Temporary(PathBuf);
            impl Drop for Temporary {
                fn drop(&mut self) {
                    let _ = std::fs::remove_file(&self.0);
                }
            }
            struct Capped<W> {
                writer: W,
                bytes: usize,
                limit: usize,
            }
            impl<W: Write> Write for Capped<W> {
                fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                    if bytes.len() > self.limit.saturating_sub(self.bytes) {
                        return Err(std::io::Error::other(PERSISTENCE_LIMIT_ERROR));
                    }
                    let count = self.writer.write(bytes)?;
                    self.bytes += count;
                    Ok(count)
                }
                fn flush(&mut self) -> std::io::Result<()> {
                    self.writer.flush()
                }
            }
            let cleanup = Temporary(temporary);
            let mut options = std::fs::OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let file = options.open(&cleanup.0)?;
            let bytes = match value {
                Snapshot::Encoded(bytes) => {
                    let mut file = file;
                    file.write_all(&bytes)?;
                    file.sync_all()?;
                    bytes.len()
                }
                Snapshot::Streamed(value) => {
                    let mut writer = Capped {
                        writer: std::io::BufWriter::with_capacity(64 * 1024, file),
                        bytes: 0,
                        limit,
                    };
                    serde_json::to_writer(&mut writer, &value)?;
                    writer.flush()?;
                    writer.writer.get_ref().sync_all()?;
                    writer.bytes
                }
            };
            std::fs::rename(&cleanup.0, target)?;
            #[cfg(unix)]
            if active || root.join(format!("{id}.active.json")).try_exists()? {
                std::fs::File::open(root)?.sync_all()?;
            }
            Ok::<_, anyhow::Error>(bytes)
        })
        .await
        .context("Session checkpoint writer interrupted")?
    }

    pub async fn load(&self, id: Uuid, workspace: &Workspace) -> Result<Session> {
        let path = self.root.join(format!("{id}.json"));
        let mut session: Session = read_json(path, self.io.clone()).await?;
        session.validate_identifiers()?;
        ensure!(
            matches!(session.version, 1 | 2) && session.id == id,
            "Invalid session format"
        );
        ensure!(
            session.cwd == workspace.root,
            "Session belongs to a different working directory"
        );
        ensure!(
            ["code", "plan"].contains(&session.mode.as_str()),
            "Invalid persisted mode"
        );
        let checkpoint = self.active_path(id);
        match read_json::<LoadedCheckpoint>(checkpoint, self.io.clone()).await {
            Ok(active) => {
                ensure!(
                    active.session_id == id,
                    "Active checkpoint belongs to another session"
                );
                if active.base_turns == session.turns.len() {
                    session.turns.push_back(Turn {
                        items: active.items.into(),
                    });
                    ensure!(
                        json_bytes(&session)? as u64 <= MAX_SESSION_BYTES,
                        "Recovered session is too large"
                    );
                } else {
                    // A crash after commit but before checkpoint cleanup leaves one stale file.
                    ensure!(
                        active.base_turns.checked_add(1) == Some(session.turns.len()),
                        "Active checkpoint does not match saved history"
                    );
                }
            }
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) => {}
            Err(error) => return Err(error),
        }
        // A process can stop between durable intent and a completed tool result.
        // Mark those calls interrupted; loading must never execute them again.
        for turn in &mut session.turns {
            crate::protocol::repair_calls(Arc::make_mut(&mut turn.items));
        }
        Ok(session)
    }

    pub async fn hydrate(&self, session: &mut Session, workspace: &Workspace) -> Result<()> {
        self.hydrate_with_size(session, workspace).await.map(|_| ())
    }

    pub(crate) async fn hydrate_with_size(
        &self,
        session: &mut Session,
        workspace: &Workspace,
    ) -> Result<Option<usize>> {
        if session.history_evicted {
            let saved = self.load(session.id, workspace).await?;
            // Keep metadata: a picker change may already be pending for save.
            session.turns = saved.turns;
            session.dropped_turns = saved.dropped_turns;
            session.history_evicted = false;
            // Count the actual restored exchanges off the event loop. A recovery
            // checkpoint or externally changed state must not reuse a stale cap.
            let snapshot = session.clone();
            let bytes = tokio::task::spawn_blocking(move || json_bytes(&snapshot)).await??;
            return Ok(Some(bytes));
        }
        Ok(None)
    }
}

/// Parse from a small input buffer on a blocking worker. The byte cap also
/// covers a file that grows after its metadata check.
async fn read_json<T: DeserializeOwned + Send + 'static>(
    path: PathBuf,
    io: Arc<tokio::sync::Semaphore>,
) -> Result<T> {
    enum ReadValue<T> {
        Small(Vec<u8>),
        Large(T),
    }
    const SMALL_FILE: u64 = 256 * 1024;
    let permit = io.acquire_owned().await?;
    let value = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        use std::io::{Read, Seek, SeekFrom};
        let mut file = std::fs::File::open(path)?;
        let metadata = file.metadata()?;
        ensure!(
            metadata.is_file() && metadata.len() <= MAX_SESSION_BYTES,
            "Session file too large or not regular"
        );
        if metadata.len() <= SMALL_FILE {
            let mut bytes = Vec::with_capacity(metadata.len() as usize + 1);
            Read::by_ref(&mut file)
                .take(SMALL_FILE + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() as u64 <= SMALL_FILE {
                return Ok(ReadValue::Small(bytes));
            }
            // The file grew after stat. Keep the large-parser cap and buffer
            // policy instead of expanding an inline allocation without bounds.
            drop(bytes);
            file.seek(SeekFrom::Start(0))?;
        }
        let mut limited = file.take(MAX_SESSION_BYTES + 1);
        let value =
            serde_json::from_reader(std::io::BufReader::with_capacity(64 * 1024, &mut limited))?;
        ensure!(limited.limit() > 0, "Session file too large");
        Ok::<_, anyhow::Error>(ReadValue::Large(value))
    })
    .await
    .context("Session reader interrupted")??;
    // Small Value trees are created and retired on the event-loop allocator.
    // Large histories stay on workers with a small fixed input buffer.
    match value {
        ReadValue::Small(bytes) => Ok(serde_json::from_slice(&bytes)?),
        ReadValue::Large(value) => Ok(value),
    }
}

#[derive(Serialize)]
struct ActiveCheckpoint {
    session_id: Uuid,
    base_turns: usize,
    items: Box<serde_json::value::RawValue>,
}

#[derive(Deserialize)]
struct LoadedCheckpoint {
    session_id: Uuid,
    base_turns: usize,
    items: Vec<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn adaptive_encoding_respects_threshold_and_propagates_serializer_errors() {
        let exact = "x".repeat(SMALL_SAVE_BYTES - 2);
        let encoded = encode_small(&exact, MAX_SESSION_BYTES as usize)
            .unwrap()
            .unwrap();
        assert_eq!(encoded.len(), SMALL_SAVE_BYTES);
        assert!(encoded.capacity() <= SMALL_SAVE_BYTES);
        assert_eq!(serde_json::from_slice::<String>(&encoded).unwrap(), exact);
        assert!(
            encode_small(
                &"x".repeat(SMALL_SAVE_BYTES - 1),
                MAX_SESSION_BYTES as usize
            )
            .unwrap()
            .is_none()
        );
        assert!(encode_small(&"x".repeat(100), 16).is_err());
        struct Invalid;
        impl Serialize for Invalid {
            fn serialize<S: serde::Serializer>(
                &self,
                _: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("Fixture serialization failure"))
            }
        }
        assert!(
            encode_small(&Invalid, MAX_SESSION_BYTES as usize)
                .unwrap_err()
                .to_string()
                .contains("Fixture serialization failure")
        );
    }

    #[tokio::test]
    async fn adaptive_persistence_writes_identical_small_and_large_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path().join("sessions")).await.unwrap();
        let id = Uuid::new_v4();
        for payload in ["small 世界\n".into(), "x".repeat(SMALL_SAVE_BYTES + 8192)] {
            let value = json!({"payload":payload,"items":[null,true,42]});
            let expected = serde_json::to_vec(&value).unwrap();
            let count = store
                .save_value(id, false, value, MAX_SESSION_BYTES as usize, None)
                .await
                .unwrap();
            assert_eq!(count, expected.len());
            let path = store.root.join(format!("{id}.json"));
            assert_eq!(tokio::fs::read(&path).await.unwrap(), expected);
            // Both main-thread and streamed-worker cap failures preserve the
            // prior committed file and remove any partial temporary output.
            for (value, cap) in [
                (json!({"payload":"z".repeat(1024)}), 512),
                (
                    json!({"a":"z".repeat(SMALL_SAVE_BYTES),"b":"tail".repeat(64)}),
                    SMALL_SAVE_BYTES + 32,
                ),
            ] {
                assert!(store.save_value(id, false, value, cap, None).await.is_err());
                assert_eq!(tokio::fs::read(&path).await.unwrap(), expected);
                assert_eq!(std::fs::read_dir(&store.root).unwrap().count(), 1);
            }
        }
    }

    #[tokio::test]
    async fn cold_history_cannot_overwrite_saved_turns_and_preserves_picker_changes() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = Workspace::new(dir.path()).await.unwrap();
        let store = Store::new(dir.path().join("sessions")).await.unwrap();
        let mut session = Session::new(&workspace, &Config::default());
        session.turns.push_back(Turn {
            items: vec![json!({"role":"user","content":"durable history"})].into(),
        });
        store.save(&session).await.unwrap();
        session.release_history(0);
        assert!(session.turns.is_empty());
        assert!(store.save(&session).await.is_err());
        assert!(store.save_active(&session, &[], 0).await.is_err());
        session.model = "new-model".into();
        session.mode = "plan".into();
        store.hydrate(&mut session, &workspace).await.unwrap();
        assert_eq!(session.turns[0].items[0]["content"], "durable history");
        assert_eq!(session.model, "new-model");
        assert_eq!(session.mode, "plan");
        store.save(&session).await.unwrap();
        let loaded = store.load(session.id, &workspace).await.unwrap();
        assert_eq!(loaded.turns.len(), 1);
        assert_eq!(loaded.model, "new-model");
        assert_eq!(loaded.mode, "plan");
        session.release_history(0);
        tokio::fs::remove_file(store.root.join(format!("{}.json", session.id)))
            .await
            .unwrap();
        assert!(store.hydrate(&mut session, &workspace).await.is_err());
        assert!(store.save(&session).await.is_err());
    }

    #[tokio::test]
    async fn persisted_identifiers_reject_oversized_control_and_invalid_reasoning() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = Workspace::new(dir.path()).await.unwrap();
        let store = Store::new(dir.path().join("sessions")).await.unwrap();
        let mut session = Session::new(&workspace, &Config::default());
        let path = store.root.join(format!("{}.json", session.id));
        for model in [
            String::new(),
            "bad\nmodel".into(),
            "x".repeat(257),
            "é".repeat(129),
        ] {
            session.model = model;
            assert!(store.save(&session).await.is_err());
            tokio::fs::write(&path, serde_json::to_vec(&session).unwrap())
                .await
                .unwrap();
            assert!(store.load(session.id, &workspace).await.is_err());
        }
        session.model = "provider/custom:model".into();
        for effort in [
            String::new(),
            "bad\neffort".into(),
            "x".repeat(65),
            "bad effort".into(),
        ] {
            session.reasoning_effort = Some(effort);
            assert!(store.save(&session).await.is_err());
            tokio::fs::write(&path, serde_json::to_vec(&session).unwrap())
                .await
                .unwrap();
            assert!(store.load(session.id, &workspace).await.is_err());
        }
        session.model = "é".repeat(128);
        for version in [1, 2] {
            session.version = version;
            for effort in [None, Some("persistent".into()), Some("x".repeat(64))] {
                session.reasoning_effort = effort.clone();
                store.save(&session).await.unwrap();
                let loaded = store.load(session.id, &workspace).await.unwrap();
                assert_eq!(loaded.reasoning_effort, effort);
                assert_eq!(loaded.version, version);
            }
        }
    }

    #[test]
    fn counted_json_size_matches_encoding_with_escapes_and_unicode() {
        let value = json!({"message":"世界\n\t\"\\", "items":[null,true,-123,0.25]});
        assert_eq!(
            json_bytes(&value).unwrap(),
            serde_json::to_vec(&value).unwrap().len()
        );
        assert_eq!(
            json_bytes(&value["items"]).unwrap(),
            serde_json::to_vec(&value["items"]).unwrap().len()
        );
        let length = serde_json::to_vec(&value).unwrap().len();
        for limit in [0, 1, length - 1, length, length + 1] {
            assert_eq!(
                json_bytes_up_to(&value, limit).unwrap(),
                (length <= limit).then_some(length)
            );
        }
    }

    #[tokio::test]
    async fn context_projection_and_checkpoints_preserve_durable_history() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = Workspace::new(dir.path()).await.unwrap();
        let store = Store::new(dir.path().join("sessions")).await.unwrap();
        let mut session = Session::new(&workspace, &Config::default());
        session.turns.push_back(Turn {
            items: vec![json!({"role":"user","content":"old history"})].into(),
        });
        session.turns.push_back(Turn {
            items: vec![json!({"role":"user","content":"x".repeat(20000)})].into(),
        });
        let (context, omitted) = session.context_with_omitted(100);
        assert!(context.is_empty());
        assert_eq!(omitted, 2);
        assert_eq!(session.turns.len(), 2);
        assert_eq!(session.dropped_turns, 0);
        let active = vec![
            json!({"type":"function_call","call_id":"pending","name":"write_file","arguments":"{}"}),
        ];
        store.save(&session).await.unwrap();
        let base_bytes = json_bytes(&session).unwrap();
        store
            .save_active(&session, &active, base_bytes)
            .await
            .unwrap();
        let loaded = store.load(session.id, &workspace).await.unwrap();
        assert_eq!(loaded.turns.len(), 3);
        assert_eq!(loaded.turns[0].items[0]["content"], "old history");
        assert_eq!(loaded.turns[2].items.len(), 2);
        assert_eq!(loaded.turns[2].items[1]["call_id"], "pending");
        assert!(
            loaded.turns[2].items[1]["output"]
                .as_str()
                .unwrap()
                .contains("interrupted")
        );
        let mut completed = active;
        completed.push(
            json!({"type":"function_call_output","call_id":"pending","output":"saved result"}),
        );
        store
            .save_active(&session, &completed, base_bytes)
            .await
            .unwrap();
        let loaded = store.load(session.id, &workspace).await.unwrap();
        assert_eq!(loaded.turns[2].items[1]["output"], "saved result");
    }

    #[tokio::test]
    async fn rejected_checkpoint_capacity_preserves_prior_base_and_active_files() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = Workspace::new(dir.path()).await.unwrap();
        let store = Store::new(dir.path().join("sessions")).await.unwrap();
        let session = Session::new(&workspace, &Config::default());
        store.save(&session).await.unwrap();
        let active = vec![json!({"role":"user", "content":"durable intent"})];
        store
            .save_active(&session, &active, json_bytes(&session).unwrap())
            .await
            .unwrap();
        let base_path = store.root.join(format!("{}.json", session.id));
        let active_path = store.active_path(session.id);
        let base_before = tokio::fs::read(&base_path).await.unwrap();
        let active_before = tokio::fs::read(&active_path).await.unwrap();
        assert!(
            store
                .save_active(&session, &active, MAX_SESSION_BYTES as usize)
                .await
                .is_err()
        );
        // Exercise a base-write cap failure without allocating a 16MiB fixture.
        assert!(
            store
                .save_value(session.id, false, session.clone(), 4, None)
                .await
                .is_err()
        );
        assert_eq!(tokio::fs::read(&base_path).await.unwrap(), base_before);
        assert_eq!(tokio::fs::read(&active_path).await.unwrap(), active_before);
        let recovered = store.load(session.id, &workspace).await.unwrap();
        assert_eq!(recovered.turns.len(), 1);
        assert_eq!(recovered.turns[0].items.as_ref(), &active);
        assert_eq!(std::fs::read_dir(&store.root).unwrap().count(), 2);
    }

    #[tokio::test]
    async fn committed_history_ignores_stale_checkpoint_without_duplicate_turn() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = Workspace::new(dir.path()).await.unwrap();
        let store = Store::new(dir.path().join("sessions")).await.unwrap();
        let mut session = Session::new(&workspace, &Config::default());
        store.save(&session).await.unwrap();
        let pending = vec![json!({"role":"user", "content":"stale checkpoint"})];
        store
            .save_active(&session, &pending, json_bytes(&session).unwrap())
            .await
            .unwrap();
        let stale = tokio::fs::read(store.active_path(session.id))
            .await
            .unwrap();
        let completed = vec![json!({"role":"assistant", "content":"committed answer"})];
        session.turns.push_back(Turn {
            items: completed.clone().into(),
        });
        store.save(&session).await.unwrap();
        assert!(!store.active_path(session.id).exists());
        // Recreate the crash window after base rename but before active cleanup.
        tokio::fs::write(store.active_path(session.id), stale)
            .await
            .unwrap();
        for _ in 0..2 {
            let recovered = store.load(session.id, &workspace).await.unwrap();
            assert_eq!(recovered.turns.len(), 1);
            assert_eq!(recovered.turns[0].items.as_ref(), &completed);
        }
    }

    #[tokio::test]
    async fn checkpoint_identity_and_history_mismatches_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = Workspace::new(dir.path()).await.unwrap();
        let store = Store::new(dir.path().join("sessions")).await.unwrap();
        let session = Session::new(&workspace, &Config::default());
        store.save(&session).await.unwrap();
        for checkpoint in [
            json!({"session_id":Uuid::new_v4(), "base_turns":0, "items":[]}),
            json!({"session_id":session.id, "base_turns":1, "items":[]}),
            json!({"session_id":session.id, "base_turns":usize::MAX, "items":[]}),
        ] {
            tokio::fs::write(store.active_path(session.id), checkpoint.to_string())
                .await
                .unwrap();
            assert!(store.load(session.id, &workspace).await.is_err());
        }
    }

    #[tokio::test]
    async fn active_checkpoint_is_private_and_failed_replacement_cleans_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = Workspace::new(dir.path()).await.unwrap();
        let store = Store::new(dir.path().join("sessions")).await.unwrap();
        let session = Session::new(&workspace, &Config::default());
        let active = vec![json!({"role":"user", "content":"private intent"})];
        store
            .save_active(&session, &active, json_bytes(&session).unwrap())
            .await
            .unwrap();
        let active_path = store.active_path(session.id);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&active_path)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            assert_eq!(
                std::fs::metadata(&store.root).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
        tokio::fs::remove_file(&active_path).await.unwrap();
        tokio::fs::create_dir(&active_path).await.unwrap();
        assert!(
            store
                .save_active(&session, &active, json_bytes(&session).unwrap())
                .await
                .is_err()
        );
        assert!(active_path.is_dir());
        let entries: Vec<_> = std::fs::read_dir(&store.root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], active_path.file_name().unwrap());
    }
}
