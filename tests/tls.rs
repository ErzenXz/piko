//! Exercise the compiled HTTP/TLS backend without public network access, paid
//! APIs, certificate-validation bypasses, or operating-system trust changes.
use futures_util::StreamExt;
use piko::provider::SseDecoder;
use reqwest::{Certificate, Client, tls::Version};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

struct Fixture {
    child: Child,
    port: u16,
    ca: Vec<u8>,
    _temporary: tempfile::TempDir,
}

impl Fixture {
    fn start(version: &str) -> Self {
        Self::with_certificate(version, "server")
    }

    fn with_certificate(version: &str, certificate: &'static str) -> Self {
        let python = std::env::var_os("PYTHON")
            .unwrap_or_else(|| if cfg!(windows) { "python" } else { "python3" }.into());
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/tls.py");
        let temporary = tempfile::tempdir().expect("TLS fixture scratch directory");
        let child = Command::new(python)
            .arg("-B")
            .arg(script)
            .args(["--tls-version", version, "--certificate", certificate])
            .arg("--temporary-directory")
            .arg(temporary.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start Python standard-library TLS fixture");
        let mut fixture = Self {
            child,
            port: 0,
            ca: Vec::new(),
            _temporary: temporary,
        };
        let stdout = fixture.child.stdout.take().expect("fixture stdout");
        let (sender, receiver) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let mut line = String::new();
            let result = BufReader::new(stdout).read_line(&mut line).map(|_| line);
            let _ = sender.send(result);
        });
        let line = receiver
            .recv_timeout(Duration::from_secs(15))
            .expect("TLS fixture must start within fifteen seconds")
            .expect("read TLS fixture port");
        let ready: Value = serde_json::from_str(&line).expect("TLS fixture startup JSON");
        fixture.port = ready["port"]
            .as_u64()
            .and_then(|n| u16::try_from(n).ok())
            .expect("TLS fixture advertised a port");
        fixture.ca = std::fs::read(ready["ca"].as_str().expect("TLS fixture CA path"))
            .expect("read isolated TLS fixture CA");
        fixture
    }

    fn address(&self) -> SocketAddr {
        (Ipv4Addr::LOCALHOST, self.port).into()
    }

    fn url(&self, host: &str, path: &str) -> String {
        format!("https://{host}:{}{path}", self.port)
    }

    fn client(&self, trust_fixture: bool, version: Option<Version>, timeout: Duration) -> Client {
        let mut builder = Client::builder()
            .no_proxy()
            .http1_only()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .tls_built_in_root_certs(false)
            .resolve("localhost", self.address())
            .resolve("wrong-host.invalid", self.address());
        if trust_fixture {
            builder = builder.add_root_certificate(
                Certificate::from_pem(&self.ca).expect("isolated fixture CA"),
            );
        }
        if let Some(version) = version {
            builder = builder.min_tls_version(version).max_tls_version(version);
        }
        builder
            .build()
            .expect("configured TLS version must be supported")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test]
async fn tls_12_accepts_explicit_test_root_and_ip_subject_alt_name() {
    let fixture = Fixture::start("1.2");
    let response: Value = fixture
        .client(true, Some(Version::TLS_1_2), Duration::from_secs(5))
        .get(fixture.url("127.0.0.1", "/ok"))
        .send()
        .await
        .expect("trusted TLS 1.2 endpoint")
        .json()
        .await
        .expect("TLS response JSON");
    assert_eq!(response, json!({"ok":true,"tls":"TLSv1.2"}));
}

#[tokio::test]
async fn tls_13_accepts_explicit_test_root_and_dns_subject_alt_name() {
    let fixture = Fixture::start("1.3");
    let response: Value = fixture
        .client(true, None, Duration::from_secs(5))
        .get(fixture.url("localhost", "/ok"))
        .send()
        .await
        .expect("trusted TLS 1.3 endpoint; do not silently downgrade")
        .json()
        .await
        .expect("TLS response JSON");
    assert_eq!(response, json!({"ok":true,"tls":"TLSv1.3"}));
}

#[tokio::test]
async fn untrusted_certificate_is_rejected() {
    let fixture = Fixture::start("both");
    let result = fixture
        .client(false, None, Duration::from_secs(3))
        .get(fixture.url("localhost", "/ok"))
        .send()
        .await;
    let error = result.expect_err("an untrusted certificate must never be accepted");
    assert!(error.is_connect() && !error.is_timeout());
}

#[tokio::test]
async fn trusted_certificate_with_wrong_hostname_is_rejected() {
    let fixture = Fixture::start("both");
    let result = fixture
        .client(true, None, Duration::from_secs(3))
        .get(fixture.url("wrong-host.invalid", "/ok"))
        .send()
        .await;
    let error = result.expect_err("a trusted CA must not bypass hostname validation");
    assert!(error.is_connect() && !error.is_timeout());
}

#[tokio::test]
async fn expired_or_not_yet_valid_leaf_is_rejected_with_a_trusted_ca() {
    for certificate in ["expired", "future"] {
        let fixture = Fixture::with_certificate("both", certificate);
        let error = fixture
            .client(true, None, Duration::from_secs(3))
            .get(fixture.url("localhost", "/ok"))
            .send()
            .await
            .expect_err("a trusted CA must not bypass certificate validity dates");
        assert!(
            error.is_connect() && !error.is_timeout(),
            "{certificate}: {error}"
        );
    }
}

async fn output(response: reqwest::Response) -> Vec<Value> {
    assert!(response.status().is_success());
    let mut stream = response.bytes_stream();
    let mut decoder = SseDecoder::with_limit(64 * 1024);
    let mut output = None;
    while let Some(chunk) = stream.next().await {
        for data in decoder
            .push(&chunk.expect("TLS stream chunk"))
            .expect("bounded SSE")
        {
            let event: Value = serde_json::from_str(&data).expect("SSE event JSON");
            if event["type"] == "response.completed" {
                output = event["response"]["output"].as_array().cloned();
            }
        }
    }
    output.expect("completed response, not an interrupted TLS stream")
}

#[tokio::test]
async fn https_stream_preserves_tool_call_result_pairing_and_split_utf8() {
    let fixture = Fixture::start("both");
    let client = fixture.client(true, None, Duration::from_secs(5));
    let url = fixture.url("localhost", "/responses");
    let mut input = vec![json!({"role":"user","content":"read the fixture"})];
    let calls = output(
        client
            .post(&url)
            .json(&json!({"input":input}))
            .send()
            .await
            .expect("first HTTPS model request"),
    )
    .await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["type"], "function_call");
    assert_eq!(calls[0]["name"], "read_file");
    let arguments: Value =
        serde_json::from_str(calls[0]["arguments"].as_str().expect("arguments JSON")).unwrap();
    assert_eq!(arguments["path"], "tls-fixture.txt");
    let call_id = calls[0]["call_id"].clone();
    input.extend(calls);
    input.push(
        json!({"type":"function_call_output","call_id":call_id,"output":"fixture read result"}),
    );
    let final_output = output(
        client
            .post(&url)
            .json(&json!({"input":input}))
            .send()
            .await
            .expect("second HTTPS model request"),
    )
    .await;
    assert_eq!(
        final_output[0]["content"][0]["text"],
        "TLS ✓ — tool result received"
    );
}

#[tokio::test]
async fn https_request_timeout_is_bounded() {
    let fixture = Fixture::start("both");
    let client = fixture.client(true, None, Duration::from_millis(150));
    let started = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.get(fixture.url("localhost", "/slow")).send(),
    )
    .await
    .expect("HTTP timeout must remain responsive");
    assert!(
        result
            .expect_err("slow endpoint must time out")
            .is_timeout()
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn cancellation_can_drop_a_stalled_authenticated_tls_stream_promptly() {
    let fixture = Fixture::start("both");
    let client = fixture.client(true, None, Duration::from_secs(5));
    let response = client
        .get(fixture.url("localhost", "/stall"))
        .send()
        .await
        .expect("TLS headers");
    let mut stream = response.bytes_stream();
    assert!(stream.next().await.expect("initial stream data").is_ok());
    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(25)).await;
        trigger.cancel();
    });
    let started = Instant::now();
    let cancelled = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => true,
            _ = stream.next() => false,
        }
    })
    .await
    .expect("TLS stream cancellation must remain responsive");
    assert!(cancelled);
    assert!(started.elapsed() < Duration::from_secs(1));
}
