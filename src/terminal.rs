//! Bounded native PTYs (Unix PTY / Windows ConPTY), integrated with background jobs.
use crate::{
    agent::AgentContext,
    jobs::{EXECUTION_ENV, Job, Jobs, ProcessGroup},
    protocol::Peer,
    tools::string,
};
use anyhow::{Context, Result, bail, ensure};
use portable_pty::{CommandBuilder, MasterPty, PtySize};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

const MAX_INPUT: usize = 16 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(2);

pub struct Terminal {
    input: std::sync::mpsc::SyncSender<Input>,
    resize: mpsc::Sender<Resize>,
}
struct Input {
    data: Vec<u8>,
    response: oneshot::Sender<Result<()>>,
}
struct Resize {
    size: PtySize,
    response: oneshot::Sender<Result<()>>,
}

#[cfg_attr(not(windows), allow(dead_code))]
struct ProtocolReplies {
    sender: std::sync::mpsc::SyncSender<Vec<u8>>,
    dimensions: Arc<std::sync::atomic::AtomicU32>,
    cancel: CancellationToken,
}

pub fn definitions() -> Vec<Value> {
    let function = |name: &str, description: &str, properties: Value| {
        let required: Vec<_> = properties
            .as_object()
            .expect("properties")
            .keys()
            .cloned()
            .collect();
        json!({"type":"function","name":name,"description":description,"strict":true,"parameters":{"type":"object","properties":properties,"required":required,"additionalProperties":false}})
    };
    vec![
        function(
            "terminal_start",
            "Start a command in a genuine interactive terminal after approval (Unix sh; Windows cmd.exe batch syntax, UTF-8). Returns a task_id; use task_status for bounded output. Execution is not sandboxed. Lifetime is limited by timeout_secs.",
            json!({"command":{"type":"string"},"timeout_secs":{"type":"integer","minimum":1},"rows":{"type":"integer","minimum":1,"maximum":500},"cols":{"type":"integer","minimum":1,"maximum":500}}),
        ),
        function(
            "terminal_write",
            "Send at most 16KiB of terminal input after explicit approval; input can execute commands. Include a newline to submit a command. Use task_status to read output.",
            json!({"task_id":{"type":"string"},"data":{"type":"string","maxLength":16384}}),
        ),
        function(
            "terminal_resize",
            "Resize your interactive terminal to 1..500 rows and columns.",
            json!({"task_id":{"type":"string"},"rows":{"type":"integer","minimum":1,"maximum":500},"cols":{"type":"integer","minimum":1,"maximum":500}}),
        ),
        function(
            "terminal_close",
            "Close your interactive terminal and cancel its process tree.",
            json!({"task_id":{"type":"string"}}),
        ),
    ]
}

pub async fn execute(ctx: &AgentContext, name: &str, args: &Value, call_id: &str) -> Result<Value> {
    ensure!(
        !ctx.read_only,
        "Interactive terminals are disabled in plan/read-only mode"
    );
    ensure!(!ctx.cancel.is_cancelled(), "Cancelled");
    if name == "terminal_start" {
        let command = string(args, "command")?;
        ensure!(
            !command.trim().is_empty() && command.len() <= MAX_INPUT && !command.contains('\0'),
            "Invalid terminal command"
        );
        let timeout = args["timeout_secs"]
            .as_u64()
            .context("Missing terminal timeout")?
            .clamp(1, ctx.config.command_timeout_secs);
        let size = size(args)?;
        ctx.peer.permission(&ctx.session_id, call_id, "Start interactive terminal", "execute", json!({"command":command,"cwd":ctx.workspace.root,"timeout_secs":timeout,"rows":size.rows,"cols":size.cols}), &ctx.cancel).await?;
        return start(
            &ctx.jobs,
            &ctx.workspace.root,
            command,
            timeout,
            size,
            ctx.agent_id.clone(),
            &ctx.cancel,
            &ctx.peer,
            &ctx.session_id,
            ctx.config.tool_output_bytes,
        )
        .await;
    }
    let job = ctx.jobs.get(string(args, "task_id")?).await?;
    ensure!(
        ctx.agent_id.is_none() || job.parent == ctx.agent_id,
        "Terminal belongs to another agent"
    );
    ensure!(
        job.kind == "terminal",
        "Task is not an interactive terminal"
    );
    if name == "terminal_close" {
        job.cancel.cancel();
        return match tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let snapshot = ctx
                    .jobs
                    .status(&job.id, ctx.agent_id.as_deref(), 3000, &ctx.cancel)
                    .await?;
                if snapshot["status"] != "running" {
                    return Ok(snapshot);
                }
            }
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Ok(json!({"task_id":job.id,"status":"cancelling"})),
        };
    }
    let terminal = job
        .terminal
        .lock()
        .await
        .clone()
        .context("Terminal has closed")?;
    ensure!(
        job.state.lock().await.status == "running" && !job.cancel.is_cancelled(),
        "Terminal has closed"
    );
    match name {
        "terminal_write" => {
            let data = string(args, "data")?;
            ensure!(
                !data.is_empty() && data.len() <= MAX_INPUT,
                "Terminal input must be 1..=16384 bytes"
            );
            ctx.peer
                .permission(
                    &ctx.session_id,
                    call_id,
                    "Write interactive terminal input",
                    "execute",
                    json!({"task_id":job.id,"data":data}),
                    &ctx.cancel,
                )
                .await?;
            ensure!(
                !ctx.cancel.is_cancelled() && !job.cancel.is_cancelled(),
                "Terminal cancelled"
            );
            let (tx, rx) = oneshot::channel();
            let result = tokio::time::timeout(IO_TIMEOUT, async {
                queue_input(
                    &terminal.input,
                    Input {
                        data: data.as_bytes().to_vec(),
                        response: tx,
                    },
                )
                .await?;
                rx.await.context("Terminal input closed")?
            });
            tokio::select! {
                _ = ctx.cancel.cancelled() => { job.cancel.cancel(); bail!("Cancelled"); },
                result = result => match result { Ok(result) => result?, Err(_) => { job.cancel.cancel(); bail!("Terminal input timed out; terminal cancelled"); } },
            }
            Ok(json!({"task_id":job.id,"bytes_written":data.len(),"status":"running"}))
        }
        "terminal_resize" => {
            let size = size(args)?;
            let (tx, rx) = oneshot::channel();
            tokio::time::timeout(IO_TIMEOUT, async {
                terminal
                    .resize
                    .send(Resize { size, response: tx })
                    .await
                    .context("Terminal closed")?;
                rx.await.context("Terminal closed")?
            })
            .await
            .context("Terminal resize timed out")??;
            Ok(json!({"task_id":job.id,"rows":size.rows,"cols":size.cols}))
        }
        _ => bail!("Unknown terminal tool"),
    }
}

fn size(args: &Value) -> Result<PtySize> {
    let dimension = |key: &str| -> Result<u16> {
        let value = args[key].as_u64().context("Missing terminal dimension")?;
        ensure!(
            (1..=500).contains(&value),
            "Terminal dimensions must be 1..=500"
        );
        Ok(value as u16)
    };
    Ok(PtySize {
        rows: dimension("rows")?,
        cols: dimension("cols")?,
        pixel_width: 0,
        pixel_height: 0,
    })
}

struct Process {
    master: Option<Box<dyn MasterPty + Send>>,
    child: Box<dyn portable_pty::Child + Send + Sync>,
    group: ProcessGroup,
    _process_permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    #[cfg(windows)]
    _script: tempfile::TempPath,
}
impl Process {
    fn kill(&mut self) {
        #[cfg(unix)]
        if let (Some(master), Some(pid)) = (&self.master, self.child.process_id())
            && let Some(foreground) = master.process_group_leader()
            // Job-control shells may move the foreground command into another group.
            && foreground > 0
            && unsafe { libc::getsid(foreground) } == pid as libc::pid_t
        {
            unsafe {
                libc::kill(-foreground, libc::SIGKILL);
            }
        }
        self.group.kill();
        let _ = self.child.kill();
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        self.kill();
    }
}
// Keep the closed script's deletion guard with Process, after child/PTY handles.
// No shell profile or inherited PowerShell module initialization is necessary.
#[cfg(any(windows, test))]
fn windows_script(command: &str) -> Result<tempfile::TempPath> {
    ensure!(!command.contains('\0'), "Terminal command contains NUL");
    let mut file = tempfile::Builder::new()
        .prefix("piko-terminal-")
        .suffix(".cmd")
        .tempfile()?;
    file.write_all(b"@echo off\r\n@chcp 65001 >nul\r\n")?;
    file.write_all(command.as_bytes())?;
    file.write_all(b"\r\n")?;
    file.flush()?;
    Ok(file.into_temp_path())
}

type ReaderHandle = std::sync::mpsc::SyncSender<Box<dyn Read + Send>>;
type WriterHandle = std::sync::mpsc::SyncSender<Box<dyn Write + Send>>;

fn spawn(
    cwd: &Path,
    command: &str,
    size: PtySize,
    reader_handle: ReaderHandle,
    writer_handle: WriterHandle,
    process_permit: Arc<tokio::sync::OwnedSemaphorePermit>,
) -> Result<Process> {
    // portable-pty silently substitutes HOME when cwd is unavailable. A removed
    // workspace must fail startup instead of executing the approved command there.
    // This startup check is not a race-proof filesystem sandbox.
    ensure!(
        cwd.is_dir(),
        "Terminal working directory is unavailable or is not a directory"
    );
    let pair = portable_pty::native_pty_system().openpty(size)?;
    #[cfg(unix)]
    {
        let fd = pair
            .master
            .as_raw_fd()
            .context("Native PTY has no file descriptor")?;
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        ensure!(
            flags >= 0 && unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } >= 0,
            "Cannot configure nonblocking terminal"
        );
    }
    // Handshake/drain workers receive the pipes BEFORE child creation or process
    // containment can fail. All PTY construction/error destruction runs off-loop.
    let reader = pair.master.try_clone_reader()?;
    let writer = pair.master.take_writer()?;
    reader_handle
        .send(reader)
        .map_err(|_| anyhow::anyhow!("Terminal reader stopped"))?;
    writer_handle
        .send(writer)
        .map_err(|_| anyhow::anyhow!("Terminal writer stopped"))?;
    #[cfg(windows)]
    let script = windows_script(command)?;
    #[cfg(windows)]
    let mut cmd = {
        let system_root = std::env::var_os("SYSTEMROOT")
            .or_else(|| std::env::var_os("WINDIR"))
            .context("Missing Windows system directory")?;
        let mut cmd = CommandBuilder::new(
            std::path::PathBuf::from(system_root)
                .join("System32")
                .join("cmd.exe"),
        );
        // CommandBuilder applies C argv escaping, which cannot represent arbitrary
        // cmd.exe scripts. Expand only this quoted, private script path; user
        // quotes, percent variables and multiline commands are parsed inside it.
        // IF keeps /S away from the path's quotes without CALL's second
        // percent-expansion pass. Registry shell preferences must not change it.
        cmd.args([
            "/D",
            "/E:ON",
            "/V:OFF",
            "/S",
            "/C",
            "if",
            "1==1",
            "%PIKO_TERMINAL_SCRIPT%",
        ]);
        cmd
    };
    #[cfg(not(windows))]
    let mut cmd = {
        let mut cmd = CommandBuilder::new("/bin/sh");
        cmd.args(["-c", command]);
        cmd
    };
    cmd.cwd(cwd);
    cmd.env_clear();
    for key in EXECUTION_ENV {
        if let Some(value) = std::env::var_os(key) {
            cmd.env(key, value);
        }
    }
    cmd.env("TERM", "xterm-256color");
    #[cfg(windows)]
    {
        let mut path = std::ffi::OsString::from("\"");
        path.push(script.as_os_str());
        path.push("\"");
        cmd.env("PIKO_TERMINAL_SCRIPT", path);
    }
    let mut child = pair.slave.spawn_command(cmd)?;
    drop(pair.slave);
    let group = match ProcessGroup::for_pty(child.as_mut()) {
        Ok(group) => group,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
    };
    Ok(Process {
        master: Some(pair.master),
        child,
        group,
        _process_permit: process_permit,
        #[cfg(windows)]
        _script: script,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn start(
    jobs: &Jobs,
    cwd: &Path,
    command: &str,
    timeout: u64,
    size: PtySize,
    parent: Option<String>,
    cancel: &CancellationToken,
    peer: &Peer,
    session: &str,
    limit: usize,
) -> Result<Value> {
    let process_permit = jobs.acquire_process(cancel)?;
    let job = jobs
        .reserve("terminal", parent, cancel.child_token())
        .await?;
    let (input, writer_rx) = std::sync::mpsc::sync_channel(1);
    let (resize, resize_rx) = mpsc::channel(4);
    let (output_tx, output_rx) = mpsc::channel(4);
    let reader_stop = CancellationToken::new();
    let writer_cancel = job.cancel.clone();
    let writer_stop = reader_stop.clone();
    let reader_cancel = reader_stop.clone();
    #[cfg(windows)]
    let (protocol_tx, protocol_rx) = std::sync::mpsc::sync_channel(8);
    #[cfg(windows)]
    let dimensions = Some(Arc::new(std::sync::atomic::AtomicU32::new(
        (u32::from(size.rows) << 16) | u32::from(size.cols),
    )));
    #[cfg(not(windows))]
    let dimensions: Option<Arc<std::sync::atomic::AtomicU32>> = None;
    #[cfg(windows)]
    let protocol_replies = Some(ProtocolReplies {
        sender: protocol_tx,
        dimensions: dimensions.as_ref().unwrap().clone(),
        cancel: job.cancel.clone(),
    });
    #[cfg(not(windows))]
    let protocol_replies = None;
    #[cfg(windows)]
    let protocol_input = Some(protocol_rx);
    #[cfg(not(windows))]
    let protocol_input = None;
    let (reader_handle, reader_rx) = std::sync::mpsc::sync_channel::<Box<dyn Read + Send>>(1);
    let (writer_handle, writer_handle_rx) =
        std::sync::mpsc::sync_channel::<Box<dyn Write + Send>>(1);
    // Allocate persistent workers BEFORE ConPTY exists. A thread-creation failure
    // cannot strand an inherit-cursor handshake or block the async runtime closing it.
    let reader_permit = process_permit.clone();
    let reader_thread = std::thread::Builder::new()
        .name("piko-pty-read".into())
        .stack_size(128 * 1024)
        .spawn(move || {
            let _process_permit = reader_permit;
            if let Ok(reader) = reader_rx.recv() {
                read_output(reader, output_tx, reader_cancel, protocol_replies);
            }
        });
    if let Err(error) = reader_thread {
        job.finish("failed", error.to_string(), None, limit).await;
        return Err(error.into());
    }
    let writer_permit = process_permit.clone();
    let writer_thread = std::thread::Builder::new()
        .name("piko-pty-write".into())
        .stack_size(128 * 1024)
        .spawn(move || {
            let _process_permit = writer_permit;
            if let Ok(writer) = writer_handle_rx.recv() {
                write_input(
                    writer,
                    writer_rx,
                    writer_cancel,
                    writer_stop,
                    protocol_input,
                );
            }
        });
    if let Err(error) = writer_thread {
        job.finish("failed", error.to_string(), None, limit).await;
        return Err(error.into());
    }
    let directory = cwd.to_owned();
    let command = command.to_owned();
    let spawned = match tokio::task::spawn_blocking(move || {
        spawn(
            &directory,
            &command,
            size,
            reader_handle,
            writer_handle,
            process_permit,
        )
    })
    .await
    {
        Ok(result) => result,
        Err(error) => Err(anyhow::anyhow!("Terminal start interrupted: {error}")),
    };
    let process = match spawned {
        Ok(process) => process,
        Err(error) => {
            reader_stop.cancel();
            job.cancel.cancel();
            job.finish("failed", format!("{error:#}"), None, limit)
                .await;
            return Err(error);
        }
    };
    *job.terminal.lock().await = Some(Arc::new(Terminal { input, resize }));
    let monitor_job = job.clone();
    let monitor_peer = peer.clone();
    let monitor_session = session.to_owned();
    tokio::spawn(async move {
        monitor(
            process,
            output_rx,
            resize_rx,
            reader_stop,
            monitor_job,
            timeout,
            limit,
            monitor_peer,
            monitor_session,
            dimensions,
        )
        .await;
    });
    if job.cancel.is_cancelled() {
        bail!("Cancelled");
    }
    peer.task_event(
        session,
        &job.id,
        json!({"status":"running","kind":"terminal","rows":size.rows,"cols":size.cols}),
    )
    .await?;
    Ok(job.snapshot().await)
}

fn read_output(
    mut reader: Box<dyn Read + Send>,
    sender: mpsc::Sender<Vec<u8>>,
    stop: CancellationToken,
    replies: Option<ProtocolReplies>,
) {
    #[cfg(windows)]
    let mut protocol = Protocol::default();
    #[cfg(not(windows))]
    let _ = replies;
    let mut buffer = [0u8; 4096];
    let mut discard = false;
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                #[cfg(windows)]
                if let Some(replies) = &replies {
                    let dimensions = replies
                        .dimensions
                        .load(std::sync::atomic::Ordering::Acquire);
                    protocol.resize((dimensions >> 16) as u16, dimensions as u16);
                    match protocol.push(&buffer[..count]) {
                        Ok(response) if !response.is_empty() => {
                            if replies.sender.try_send(response).is_err() {
                                replies.cancel.cancel();
                            }
                        }
                        Ok(_) => {}
                        Err(_) => {
                            replies.cancel.cancel();
                        }
                    }
                }
                if !discard && sender.blocking_send(buffer[..count].to_vec()).is_err() {
                    discard = true;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if stop.is_cancelled() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            // Linux PTYs report EIO after their slave is closed.
            Err(_) => break,
        }
    }
}

async fn queue_input(sender: &std::sync::mpsc::SyncSender<Input>, mut input: Input) -> Result<()> {
    loop {
        match sender.try_send(input) {
            Ok(()) => return Ok(()),
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => bail!("Terminal input closed"),
            Err(std::sync::mpsc::TrySendError::Full(pending)) => input = pending,
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn write_input(
    mut writer: Box<dyn Write + Send>,
    receiver: std::sync::mpsc::Receiver<Input>,
    cancel: CancellationToken,
    stop: CancellationToken,
    protocols: Option<std::sync::mpsc::Receiver<Vec<u8>>>,
) {
    let mut shutdown = None;
    loop {
        let stopping = cancel.is_cancelled() || stop.is_cancelled() || shutdown.is_some();
        if stopping && protocols.is_none() {
            break;
        }
        if stopping {
            let deadline = shutdown.get_or_insert_with(|| Instant::now() + IO_TIMEOUT);
            if Instant::now() >= *deadline {
                break;
            }
        }
        // Control replies have their own bounded queue and always precede ordinary input.
        let protocol = protocols.as_ref().and_then(|queue| queue.try_recv().ok());
        let (input, control) = if let Some(data) = protocol {
            (
                Input {
                    data,
                    response: oneshot::channel().0,
                },
                true,
            )
        } else if stopping {
            match protocols
                .as_ref()
                .unwrap()
                .recv_timeout(Duration::from_millis(20))
            {
                Ok(data) => (
                    Input {
                        data,
                        response: oneshot::channel().0,
                    },
                    true,
                ),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        } else {
            let delay = if protocols.is_some() { 20 } else { 100 };
            match receiver.recv_timeout(Duration::from_millis(delay)) {
                Ok(input) => (input, false),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    if protocols.is_none() {
                        break;
                    }
                    shutdown = Some(Instant::now() + IO_TIMEOUT);
                    continue;
                }
            }
        };
        // ConPTY interprets carriage return as Enter. Accept ordinary LF/CRLF
        // tool input consistently without expanding its byte budget.
        #[cfg(windows)]
        let input = Input {
            data: String::from_utf8_lossy(&input.data)
                .replace("\r\n", "\r")
                .replace('\n', "\r")
                .into_bytes(),
            response: input.response,
        };
        let deadline = Instant::now() + IO_TIMEOUT;
        let mut written = 0;
        let result: Result<()> = (|| {
            while written < input.data.len() {
                ensure!(
                    (control || (!cancel.is_cancelled() && !stop.is_cancelled()))
                        && Instant::now() < deadline,
                    "Terminal input cancelled or timed out"
                );
                match writer.write(&input.data[written..]) {
                    Ok(0) => bail!("Terminal input closed"),
                    Ok(count) => written += count,
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5))
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            writer.flush()?;
            Ok(())
        })();
        let failed = result.is_err();
        let _ = input.response.send(result);
        if failed {
            cancel.cancel();
        }
        if failed && control {
            break;
        }
    }
}

// portable-pty enables ConPTY cursor inheritance. Microsoft requires answering
// CPR and DA1 on the input pipe; otherwise even startup/ClosePseudoConsole can hang.
// This is a bounded query responder/cursor tracker for text capture, not a screen grid.
// https://learn.microsoft.com/windows/console/createpseudoconsole
#[cfg(any(windows, test))]
struct Protocol {
    state: u8,
    sequence: [u8; 32],
    length: usize,
    row: u16,
    col: u16,
    rows: u16,
    cols: u16,
    saved: (u16, u16),
    decoder: Decoder,
    window: Instant,
    queries: usize,
}
#[cfg(any(windows, test))]
impl Default for Protocol {
    fn default() -> Self {
        Self {
            state: 0,
            sequence: [0; 32],
            length: 0,
            row: 1,
            col: 1,
            rows: 24,
            cols: 80,
            saved: (1, 1),
            decoder: Decoder::default(),
            window: Instant::now(),
            queries: 0,
        }
    }
}
#[cfg(any(windows, test))]
impl Protocol {
    fn resize(&mut self, rows: u16, cols: u16) {
        self.rows = rows.max(1);
        self.cols = cols.max(1);
        self.row = self.row.min(self.rows);
        self.col = self.col.min(self.cols);
    }
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<u8>> {
        if self.window.elapsed() >= Duration::from_secs(1) {
            self.window = Instant::now();
            self.queries = 0;
        }
        let mut replies = Vec::new();
        for ch in self.decoder.push(bytes).chars() {
            match self.state {
                0 => match ch {
                    '\x1b' => self.state = 1,
                    '\r' => self.col = 1,
                    '\n' => self.row = (self.row + 1).min(self.rows),
                    '\x08' => self.col = self.col.saturating_sub(1).max(1),
                    '\t' => self.col = (((self.col - 1) / 8 + 1) * 8 + 1).min(self.cols),
                    ch if !ch.is_control() => {
                        if self.col == self.cols {
                            self.col = 1;
                            self.row = (self.row + 1).min(self.rows);
                        } else {
                            self.col += 1;
                        }
                    }
                    _ => {}
                },
                1 => match ch {
                    '[' => {
                        self.state = 2;
                        self.length = 0;
                    }
                    ']' => self.state = 3,
                    'P' | '^' | '_' => self.state = 5,
                    '7' => {
                        self.saved = (self.row, self.col);
                        self.state = 0;
                    }
                    '8' => {
                        (self.row, self.col) = self.saved;
                        self.state = 0;
                    }
                    'D' => {
                        self.row = (self.row + 1).min(self.rows);
                        self.state = 0;
                    }
                    'E' => {
                        self.row = (self.row + 1).min(self.rows);
                        self.col = 1;
                        self.state = 0;
                    }
                    'M' => {
                        self.row = self.row.saturating_sub(1).max(1);
                        self.state = 0;
                    }
                    '\x1b' => {}
                    _ => self.state = 0,
                },
                2 => {
                    if ('@'..='~').contains(&ch) {
                        if let Some(response) = self.csi(ch) {
                            self.queries += 1;
                            ensure!(
                                self.queries <= 64 && replies.len() + response.len() <= 512,
                                "Terminal query flood"
                            );
                            replies.extend_from_slice(response.as_bytes());
                        }
                        self.state = 0;
                    } else if ch.is_ascii() && self.length < self.sequence.len() {
                        self.sequence[self.length] = ch as u8;
                        self.length += 1;
                    } else {
                        self.state = 7;
                    }
                }
                // OSC ends with BEL or ST; other terminal strings only end with ST.
                3 => match ch {
                    '\x07' => self.state = 0,
                    '\x1b' => self.state = 4,
                    _ => {}
                },
                4 => self.state = if ch == '\\' { 0 } else { 3 },
                5 => {
                    if ch == '\x1b' {
                        self.state = 6;
                    }
                }
                6 => self.state = if ch == '\\' { 0 } else { 5 },
                _ => {
                    if ('@'..='~').contains(&ch) {
                        self.state = 0;
                    }
                }
            }
        }
        Ok(replies)
    }
    fn csi(&mut self, final_byte: char) -> Option<String> {
        let parameters = std::str::from_utf8(&self.sequence[..self.length]).unwrap_or("");
        match (parameters, final_byte) {
            ("6", 'n') => return Some(format!("\x1b[{};{}R", self.row, self.col)),
            ("?6", 'n') => return Some(format!("\x1b[?{};{}R", self.row, self.col)),
            ("5", 'n') => return Some("\x1b[0n".into()),
            ("" | "0", 'c') => return Some("\x1b[?1;0c".into()),
            (">" | ">0", 'c') => return Some("\x1b[>0;0;0c".into()),
            _ => {}
        }
        if !parameters
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b';')
        {
            return None;
        }
        let mut values = parameters
            .split(';')
            .map(|value| value.parse::<u16>().unwrap_or(1).max(1));
        let first = values.next().unwrap_or(1);
        match final_byte {
            'H' | 'f' => {
                self.row = first.min(self.rows);
                self.col = values.next().unwrap_or(1).min(self.cols);
            }
            'A' => self.row = self.row.saturating_sub(first).max(1),
            'B' => self.row = self.row.saturating_add(first).min(self.rows),
            'C' => self.col = self.col.saturating_add(first).min(self.cols),
            'D' => self.col = self.col.saturating_sub(first).max(1),
            'E' => {
                self.row = self.row.saturating_add(first).min(self.rows);
                self.col = 1;
            }
            'F' => {
                self.row = self.row.saturating_sub(first).max(1);
                self.col = 1;
            }
            'G' => self.col = first.min(self.cols),
            'd' => self.row = first.min(self.rows),
            's' => self.saved = (self.row, self.col),
            'u' => {
                self.row = self.saved.0.min(self.rows);
                self.col = self.saved.1.min(self.cols);
            }
            _ => {}
        }
        None
    }
}

#[derive(Default)]
struct Decoder {
    pending: Vec<u8>,
}
impl Decoder {
    fn push(&mut self, bytes: &[u8]) -> String {
        self.pending.extend_from_slice(bytes);
        let mut offset = 0;
        let mut end = self.pending.len();
        loop {
            match std::str::from_utf8(&self.pending[offset..]) {
                Ok(_) => break,
                Err(error) => match error.error_len() {
                    Some(length) => offset += error.valid_up_to() + length,
                    None => {
                        end = offset + error.valid_up_to();
                        break;
                    }
                },
            }
        }
        let output = String::from_utf8_lossy(&self.pending[..end]).into_owned();
        self.pending.drain(..end);
        output
    }
    fn finish(self) -> String {
        String::from_utf8_lossy(&self.pending).into_owned()
    }
}

#[allow(clippy::too_many_arguments)]
async fn monitor(
    mut process: Process,
    mut output: mpsc::Receiver<Vec<u8>>,
    mut resize: mpsc::Receiver<Resize>,
    reader_stop: CancellationToken,
    job: Arc<Job>,
    timeout: u64,
    limit: usize,
    peer: Peer,
    session: String,
    dimensions: Option<Arc<std::sync::atomic::AtomicU32>>,
) {
    let deadline = tokio::time::sleep(Duration::from_secs(timeout));
    tokio::pin!(deadline);
    let mut poll = tokio::time::interval(Duration::from_millis(25));
    let mut decoder = Decoder::default();
    let mut output_open = true;
    let (status, code) = loop {
        tokio::select! {
            biased;
            _ = job.cancel.cancelled() => break ("cancelled", None),
            _ = &mut deadline => break ("timed_out", None),
            Some(request) = resize.recv() => {
                let result = process.master.as_ref().context("Terminal closed").and_then(|master|master.resize(request.size));
                if result.is_ok() && let Some(dimensions) = &dimensions {
                    dimensions.store((u32::from(request.size.rows) << 16) | u32::from(request.size.cols), std::sync::atomic::Ordering::Release);
                }
                let _ = request.response.send(result);
            },
            chunk = output.recv(), if output_open => match chunk { Some(bytes) => job.append_output(&decoder.push(&bytes), limit).await, None => output_open = false },
            _ = poll.tick() => match process.child.try_wait() {
                Ok(Some(exit)) => break (if exit.success() { "completed" } else { "failed" }, i32::try_from(exit.exit_code()).ok()),
                Ok(None) => {}, Err(_) => break ("failed", None),
            },
        }
    };
    #[cfg(unix)]
    if let Some(pid) = process.child.process_id() {
        kill_session(pid).await;
    }
    process.kill();
    job.terminal.lock().await.take();
    reader_stop.cancel();
    let closing = tokio::task::spawn_blocking(move || {
        let _ = process.child.wait();
        drop(process);
    });
    // Keep draining while ConPTY closes; its ClosePseudoConsole can wait on its output pipe.
    let _ = tokio::time::timeout(Duration::from_secs(1), async {
        while let Some(bytes) = output.recv().await {
            job.append_output(&decoder.push(&bytes), limit).await;
        }
        let _ = closing.await;
    })
    .await;
    drop(output);
    job.append_output(&decoder.finish(), limit).await;
    let mut state = job.state.lock().await;
    state.status = status.into();
    state.exit_code = code;
    drop(state);
    job.done.notify_waiters();
    let _ = peer
        .task_event(&session, &job.id, job.snapshot().await)
        .await;
}

#[cfg(unix)]
async fn kill_session(session: u32) {
    use tokio::{io::AsyncReadExt, process::Command};
    // Interactive job-control shells create multiple groups. Target only processes
    // whose kernel session ID matches this PTY's freshly-created private session.
    // ps exposes IDs only: no command lines, credentials, or other user content.
    let sweep = async {
        let mut child = Command::new("/bin/ps")
            .args(["-e", "-o", "pid="])
            .env_clear()
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let mut bytes = Vec::new();
        child
            .stdout
            .take()
            .context("Missing process list")?
            .take(256 * 1024)
            .read_to_end(&mut bytes)
            .await?;
        let _ = child.wait().await?;
        for pid in String::from_utf8_lossy(&bytes)
            .split_whitespace()
            .rev()
            .filter_map(|pid| pid.parse::<i32>().ok())
        {
            if pid > 0 && unsafe { libc::getsid(pid) } == session as libc::pid_t {
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    let _ = tokio::time::timeout(Duration::from_millis(500), sweep).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_output_survives_every_boundary_and_bounds_partial_bytes() {
        let text = "hello 世界 😀 fin";
        for split in 0..=text.len() {
            let mut decoder = Decoder::default();
            let first = decoder.push(&text.as_bytes()[..split]);
            assert!(decoder.pending.len() <= 3);
            let second = decoder.push(&text.as_bytes()[split..]);
            assert_eq!(format!("{first}{second}{}", decoder.finish()), text);
        }
        let mut decoder = Decoder::default();
        assert_eq!(decoder.push(&[0xff, b'a', 0xe4]), "�a");
        assert_eq!(decoder.push(&[0xb8, 0x96]), "世");
        assert!(decoder.finish().is_empty());
    }

    #[test]
    fn windows_script_preserves_commands_and_cleans_up_its_file() {
        let command = "set value=hello\r\necho \"%TEMP%\\quoted path\"\r\necho héllo %value%";
        let script = windows_script(command).unwrap();
        let path = script.to_path_buf();
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            contents,
            format!("@echo off\r\n@chcp 65001 >nul\r\n{command}\r\n")
        );
        assert_eq!(path.extension().unwrap(), "cmd");
        drop(script);
        assert!(!path.exists());
        assert!(windows_script("echo bad\0command").is_err());
    }

    #[test]
    #[cfg(windows)]
    fn windows_cmd_script_invocation_preserves_quoted_paths_and_variables() {
        let directory = tempfile::tempdir().unwrap();
        let quoted = directory.path().join("quoted path");
        std::fs::create_dir(&quoted).unwrap();
        let output_file = quoted.join("result.txt");
        let command = format!(
            "set piko_smoke=quoted-value\r\necho %piko_smoke%>\"{}\"\r\n\"%SystemRoot%\\System32\\cmd.exe\" /D /C type \"{}\"",
            output_file.display(),
            output_file.display()
        );
        let script = windows_script(&command).unwrap();
        let system_root = std::env::var_os("SYSTEMROOT").unwrap();
        let executable = std::path::PathBuf::from(system_root)
            .join("System32")
            .join("cmd.exe");
        assert!(!executable.as_os_str().to_string_lossy().contains('/'));
        let output = std::process::Command::new(executable)
            .args([
                "/D",
                "/E:ON",
                "/V:OFF",
                "/S",
                "/C",
                "if",
                "1==1",
                "%PIKO_TERMINAL_SCRIPT%",
            ])
            .env("PIKO_TERMINAL_SCRIPT", format!("\"{}\"", script.display()))
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("quoted-value"));
        assert_eq!(
            std::fs::read_to_string(output_file).unwrap(),
            "quoted-value\r\n"
        );
    }

    #[test]
    fn terminal_dimensions_are_strictly_bounded() {
        for value in [
            json!({"rows":0,"cols":80}),
            json!({"rows":24,"cols":501}),
            json!({"rows":-1,"cols":80}),
            json!({"rows":24.5,"cols":80}),
        ] {
            assert!(size(&value).is_err());
        }
        assert_eq!(size(&json!({"rows":500,"cols":1})).unwrap().rows, 500);
        assert_eq!(definitions().len(), 4);
    }

    async fn wait_finished(jobs: &Jobs, id: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let result = jobs
                    .status(id, None, 1000, &CancellationToken::new())
                    .await
                    .unwrap();
                if result["status"] != "running" {
                    return result;
                }
            }
        })
        .await
        .unwrap()
    }

    async fn wait_process_slot(pool: &tokio::sync::Semaphore) {
        tokio::time::timeout(Duration::from_secs(4), async {
            while pool.available_permits() == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    #[cfg(any(unix, windows))]
    async fn shell_and_pty_share_process_budget_and_cancellation_releases_workers() {
        let directory = tempfile::tempdir().unwrap();
        let pool = Arc::new(tokio::sync::Semaphore::new(1));
        let terminals = Jobs::with_process_limit(4, 1024, pool.clone());
        let shells = Jobs::with_process_limit(4, 1024, pool.clone());
        let (peer, mut receiver) = Peer::channel(32);
        let events = tokio::spawn(async move { while receiver.recv().await.is_some() {} });
        let cancel = CancellationToken::new();
        #[cfg(unix)]
        let command = "sleep 30";
        #[cfg(windows)]
        let command = "ping -n 31 127.0.0.1 >nul";
        let started = start(
            &terminals,
            directory.path(),
            command,
            30,
            PtySize::default(),
            None,
            &cancel,
            &peer,
            "shared-pty",
            1024,
        )
        .await
        .unwrap();
        assert_eq!(pool.available_permits(), 0);
        let denied = shells
            .shell(
                directory.path(),
                command,
                30,
                true,
                None,
                &cancel,
                &peer,
                "shared-shell",
            )
            .await
            .unwrap_err();
        assert!(denied.to_string().contains("Global shell/terminal"));
        assert!(
            start(
                &shells,
                directory.path(),
                command,
                30,
                PtySize::default(),
                None,
                &cancel,
                &peer,
                "shared-denied",
                1024
            )
            .await
            .is_err()
        );
        assert!(
            shells.list().await.is_empty(),
            "Denied starts must not reserve jobs or workers"
        );
        let id = started["task_id"].as_str().unwrap();
        terminals.get(id).await.unwrap().cancel.cancel();
        assert_eq!(wait_finished(&terminals, id).await["status"], "cancelled");
        wait_process_slot(&pool).await;
        let shell = shells
            .shell(
                directory.path(),
                command,
                30,
                true,
                None,
                &cancel,
                &peer,
                "shared-shell",
            )
            .await
            .unwrap();
        assert_eq!(pool.available_permits(), 0);
        assert!(
            start(
                &terminals,
                directory.path(),
                command,
                30,
                PtySize::default(),
                None,
                &cancel,
                &peer,
                "shared-denied",
                1024
            )
            .await
            .is_err()
        );
        let id = shell["task_id"].as_str().unwrap();
        shells.get(id).await.unwrap().cancel.cancel();
        assert_eq!(wait_finished(&shells, id).await["status"], "cancelled");
        wait_process_slot(&pool).await;
        let finished = shells
            .shell(
                directory.path(),
                "echo released",
                5,
                false,
                None,
                &cancel,
                &peer,
                "shared-reused",
            )
            .await
            .unwrap();
        assert_eq!(finished["status"], "completed");
        wait_process_slot(&pool).await;
        assert_eq!(pool.available_permits(), 1);
        events.abort();
    }

    #[tokio::test]
    #[cfg(any(unix, windows))]
    async fn cancelled_and_failed_terminal_start_release_process_budget() {
        let directory = tempfile::tempdir().unwrap();
        let pool = Arc::new(tokio::sync::Semaphore::new(1));
        let jobs = Jobs::with_process_limit(4, 1024, pool.clone());
        let (peer, mut receiver) = Peer::channel(32);
        let events = tokio::spawn(async move { while receiver.recv().await.is_some() {} });
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(
            start(
                &jobs,
                directory.path(),
                "echo never",
                5,
                PtySize::default(),
                None,
                &cancel,
                &peer,
                "cancel-before-start",
                1024
            )
            .await
            .is_err()
        );
        assert!(jobs.list().await.is_empty());
        assert_eq!(pool.available_permits(), 1);
        let cancel = CancellationToken::new();
        let missing = directory.path().join("missing-directory");
        assert!(
            start(
                &jobs,
                &missing,
                "echo never",
                5,
                PtySize::default(),
                None,
                &cancel,
                &peer,
                "failed-start",
                1024
            )
            .await
            .is_err()
        );
        wait_process_slot(&pool).await;
        assert_eq!(pool.available_permits(), 1);
        assert!(
            jobs.shell(
                &missing,
                "echo never",
                5,
                true,
                None,
                &cancel,
                &peer,
                "failed-shell"
            )
            .await
            .is_err()
        );
        assert_eq!(pool.available_permits(), 1);
        events.abort();
    }

    #[tokio::test]
    #[cfg(any(unix, windows))]
    async fn native_terminal_accepts_input_resizes_and_preserves_unicode() {
        let directory = tempfile::tempdir().unwrap();
        let jobs = Jobs::new(2, 4096);
        let (peer, mut receiver) = Peer::channel(32);
        let events = tokio::spawn(async move { while receiver.recv().await.is_some() {} });
        #[cfg(unix)]
        let command = "test -t 0 && test -t 1 || exit 9; printf 'ready>'; IFS= read -r line; printf 'reply:%s\\n' \"$line\"; stty size";
        #[cfg(windows)]
        let command = "python -c \"import sys; sys.exit(0 if sys.stdin.isatty() and sys.stdout.isatty() else 9)\"\r\nif errorlevel 1 exit /b 9\r\nset /p piko_line=ready^>\r\necho reply:%piko_line%";
        let began = Instant::now();
        let snapshot = start(
            &jobs,
            directory.path(),
            command,
            if cfg!(windows) { 30 } else { 10 },
            PtySize::default(),
            Some("owner".into()),
            &CancellationToken::new(),
            &peer,
            "pty-test",
            4096,
        )
        .await
        .unwrap();
        let id = snapshot["task_id"].as_str().unwrap();
        let job = jobs.get(id).await.unwrap();
        let ready = tokio::time::timeout(
            Duration::from_secs(if cfg!(windows) { 15 } else { 5 }),
            async {
                loop {
                    let snapshot = jobs
                        .status(id, Some("owner"), 1000, &CancellationToken::new())
                        .await
                        .unwrap();
                    if snapshot["output"].as_str().unwrap().contains("ready>") {
                        break;
                    }
                    assert_eq!(snapshot["status"], "running", "{snapshot}");
                }
            },
        )
        .await;
        assert!(
            ready.is_ok(),
            "Terminal failed to become ready after {:?}: {}",
            began.elapsed(),
            job.snapshot().await
        );
        assert!(
            jobs.status(id, Some("different-owner"), 0, &CancellationToken::new())
                .await
                .is_err()
        );
        let terminal = job.terminal.lock().await.clone().unwrap();
        let (tx, rx) = oneshot::channel();
        terminal
            .resize
            .send(Resize {
                size: PtySize {
                    rows: 39,
                    cols: 105,
                    pixel_width: 0,
                    pixel_height: 0,
                },
                response: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap().unwrap();
        let (tx, rx) = oneshot::channel();
        terminal
            .input
            .try_send(Input {
                data: "héllo\n".as_bytes().to_vec(),
                response: tx,
            })
            .unwrap();
        rx.await.unwrap().unwrap();
        drop(terminal);
        let finished = wait_finished(&jobs, id).await;
        assert_eq!(finished["status"], "completed", "{finished}");
        assert!(
            finished["output"].as_str().unwrap().contains("reply:héllo"),
            "{finished}"
        );
        #[cfg(unix)]
        assert!(
            finished["output"].as_str().unwrap().contains("39 105"),
            "{finished}"
        );
        assert!(job.terminal.lock().await.is_none());
        events.abort();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn timeout_cleans_descendants_and_output_stays_bounded() {
        let directory = tempfile::tempdir().unwrap();
        let jobs = Jobs::new(2, 128);
        let (peer, mut receiver) = Peer::channel(32);
        let events = tokio::spawn(async move { while receiver.recv().await.is_some() {} });
        let cancel = CancellationToken::new();
        let started = start(
            &jobs,
            directory.path(),
            "sleep 30 & printf 'child:%s\\n' \"$!\"; wait",
            1,
            PtySize::default(),
            None,
            &cancel,
            &peer,
            "pty-timeout",
            128,
        )
        .await
        .unwrap();
        let id = started["task_id"].as_str().unwrap();
        let finished = wait_finished(&jobs, id).await;
        assert_eq!(finished["status"], "timed_out", "{finished}");
        let output = finished["output"].as_str().unwrap();
        let pid: i32 = output
            .trim()
            .strip_prefix("child:")
            .unwrap()
            .parse()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while unsafe { libc::kill(pid, 0) } == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let started = start(
            &jobs,
            directory.path(),
            "printf '%06000d' 0",
            5,
            PtySize::default(),
            None,
            &cancel,
            &peer,
            "pty-output",
            128,
        )
        .await
        .unwrap();
        let finished = wait_finished(&jobs, started["task_id"].as_str().unwrap()).await;
        assert_eq!(finished["status"], "completed", "{finished}");
        assert_eq!(finished["truncated"], true);
        assert!(finished["output"].as_str().unwrap().len() <= 128);
        events.abort();
    }

    #[tokio::test]
    #[cfg(any(unix, windows))]
    async fn cancellation_releases_terminal_and_job_capacity() {
        let directory = tempfile::tempdir().unwrap();
        let jobs = Jobs::new(1, 128);
        let (peer, mut receiver) = Peer::channel(32);
        let events = tokio::spawn(async move { while receiver.recv().await.is_some() {} });
        #[cfg(unix)]
        let command = "sleep 30";
        #[cfg(windows)]
        let command = "ping -n 31 127.0.0.1 >nul";
        let started = start(
            &jobs,
            directory.path(),
            command,
            30,
            PtySize::default(),
            Some("owner".into()),
            &CancellationToken::new(),
            &peer,
            "pty-cancel",
            128,
        )
        .await
        .unwrap();
        let job = jobs
            .get(started["task_id"].as_str().unwrap())
            .await
            .unwrap();
        job.cancel.cancel();
        let finished = wait_finished(&jobs, &job.id).await;
        assert_eq!(finished["status"], "cancelled");
        assert!(job.terminal.lock().await.is_none());
        assert!(
            jobs.reserve("test", None, CancellationToken::new())
                .await
                .is_ok()
        );
        events.abort();
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    #[test]
    fn writer_stops_even_when_an_approval_waiter_retains_its_sender() {
        struct Writer(std::sync::mpsc::Sender<()>);
        impl Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl Drop for Writer {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let (closed, finished) = std::sync::mpsc::channel();
        let stop = CancellationToken::new();
        let worker_stop = stop.clone();
        let worker = std::thread::spawn(move || {
            write_input(
                Box::new(Writer(closed)),
                receiver,
                CancellationToken::new(),
                worker_stop,
                None,
            )
        });
        stop.cancel();
        finished.recv_timeout(Duration::from_secs(1)).unwrap();
        // Still retain sender here: terminal shutdown cannot depend on its last clone dropping.
        assert!(
            sender
                .try_send(Input {
                    data: vec![b'x'],
                    response: oneshot::channel().0
                })
                .is_err()
        );
        worker.join().unwrap();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn canonical_terminal_eof_reaches_the_program() {
        let directory = tempfile::tempdir().unwrap();
        let jobs = Jobs::new(1, 1024);
        let (peer, mut updates) = Peer::channel(32);
        let events = tokio::spawn(async move { while updates.recv().await.is_some() {} });
        let started = start(
            &jobs,
            directory.path(),
            "printf 'ready>'; IFS= read -r line || printf 'eof-received\\n'",
            5,
            PtySize::default(),
            None,
            &CancellationToken::new(),
            &peer,
            "pty-eof",
            1024,
        )
        .await
        .unwrap();
        let id = started["task_id"].as_str().unwrap();
        let job = jobs.get(id).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = jobs
                    .status(id, None, 100, &CancellationToken::new())
                    .await
                    .unwrap();
                if snapshot["output"].as_str().unwrap().contains("ready>") {
                    break;
                }
            }
        })
        .await
        .unwrap();
        let terminal = job.terminal.lock().await.clone().unwrap();
        let (tx, rx) = oneshot::channel();
        terminal
            .input
            .try_send(Input {
                data: vec![4],
                response: tx,
            })
            .unwrap();
        rx.await.unwrap().unwrap();
        let finished = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = jobs
                    .status(id, None, 100, &CancellationToken::new())
                    .await
                    .unwrap();
                if snapshot["status"] != "running" {
                    break snapshot;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(finished["status"], "completed", "{finished}");
        assert!(
            finished["output"]
                .as_str()
                .unwrap()
                .contains("eof-received"),
            "{finished}"
        );
        events.abort();
    }
}

#[cfg(test)]
mod protocol_tests {
    use super::*;

    #[test]
    fn conpty_initial_cpr_and_da1_are_answered_across_every_split() {
        let request = b"\x1b[6n\x1b[c";
        for split in 0..=request.len() {
            let mut protocol = Protocol::default();
            let mut response = protocol.push(&request[..split]).unwrap();
            response.extend(protocol.push(&request[split..]).unwrap());
            assert_eq!(response, b"\x1b[1;1R\x1b[?1;0c");
        }
    }

    #[test]
    fn cursor_queries_are_bounded_and_ignored_inside_terminal_strings() {
        let mut protocol = Protocol::default();
        protocol.resize(39, 105);
        assert_eq!(
            protocol
                .push(b"hello\r\n\x1b[8;10H\x1b[?6n\x1b[5n")
                .unwrap(),
            b"\x1b[?8;10R\x1b[0n"
        );
        assert!(
            protocol
                .push(b"\x1b]2;title\x1b[6n\x07\x1bPignored\x1b[c\x1b\\")
                .unwrap()
                .is_empty()
        );
        let mut oversized = b"\x1b[".to_vec();
        oversized.extend(vec![b'1'; 1000]);
        oversized.extend(b"n\x1b[6n");
        assert_eq!(protocol.push(&oversized).unwrap(), b"\x1b[8;10R");
        assert!(protocol.length <= 32);
        assert!(Protocol::default().push(&b"\x1b[6n".repeat(65)).is_err());
    }

    #[test]
    fn fixed_protocol_replies_precede_queued_model_input_and_survive_cancellation() {
        struct Writer {
            bytes: Arc<std::sync::Mutex<Vec<u8>>>,
            written: std::sync::mpsc::Sender<()>,
        }
        impl Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.bytes.lock().unwrap().extend_from_slice(bytes);
                let _ = self.written.send(());
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let bytes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (input, receiver) = std::sync::mpsc::sync_channel(1);
        let (control, protocols) = std::sync::mpsc::sync_channel(8);
        let (written, received) = std::sync::mpsc::channel();
        input
            .send(Input {
                data: b"USER".to_vec(),
                response: oneshot::channel().0,
            })
            .unwrap();
        control.send(b"\x1b[1;1R\x1b[?1;0c".to_vec()).unwrap();
        let cancel = CancellationToken::new();
        let stop = CancellationToken::new();
        let worker_cancel = cancel.clone();
        let worker_stop = stop.clone();
        let output = bytes.clone();
        let worker = std::thread::spawn(move || {
            write_input(
                Box::new(Writer {
                    bytes: output,
                    written,
                }),
                receiver,
                worker_cancel,
                worker_stop,
                Some(protocols),
            )
        });
        received.recv_timeout(Duration::from_secs(1)).unwrap();
        received.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(*bytes.lock().unwrap(), b"\x1b[1;1R\x1b[?1;0cUSER");
        cancel.cancel();
        stop.cancel();
        // ConPTY may still need a reply during ClosePseudoConsole.
        control.send(b"\x1b[1;1R".to_vec()).unwrap();
        received.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(control);
        worker.join().unwrap();
        assert!(bytes.lock().unwrap().ends_with(b"\x1b[1;1R"));
    }
}
