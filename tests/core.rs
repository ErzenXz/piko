use piko::{
    auth::claims,
    config::Config,
    jobs::Jobs,
    metrics::Metrics,
    protocol::{Peer, Server, read_frame, repair_calls},
    provider::{ChatStream, SseDecoder, chat_request},
    search::search,
    session::{Session, Store, Turn},
    workspace::{Workspace, read_bounded, truncate},
};
use serde_json::json;
use tempfile::tempdir;
use tokio::{io::BufReader, sync::Semaphore};
use tokio_util::sync::CancellationToken;

#[test]
fn config_rejects_insecure_urls_and_invalid_limits() {
    assert!(!Config::default().computer_use);
    assert_eq!(Config::default().model, "gpt-6-luna");
    for url in [
        "http://example.com/v1",
        "https://user:password@example.com",
        "https://example.com?key=x",
        "https://example.com/#x",
    ] {
        let config = Config {
            base_url: url.into(),
            ..Config::default()
        };
        assert!(config.validate().is_err(), "{url}");
    }
    let mut config = Config {
        base_url: "http://127.0.0.1:1234/v1".into(),
        ..Config::default()
    };
    assert!(config.validate().is_ok());
    config.max_model_requests = 0;
    assert!(config.validate().is_err());
    assert!(toml::from_str::<Config>("unknown_typo = 1").is_err());
    for example in [
        include_str!("../examples/chatgpt.toml"),
        include_str!("../examples/ollama.toml"),
        include_str!("../examples/openrouter.toml"),
        include_str!("../examples/computer.toml"),
        include_str!("../examples/laptop-swarm.toml"),
    ] {
        toml::from_str::<Config>(example)
            .unwrap()
            .validate()
            .unwrap();
    }
}

#[test]
fn mcp_config_validation_and_pool_capacity_are_bounded() {
    use piko::mcp::{Pool, Servers};
    use std::{path::Path, sync::Arc};
    let pool = Arc::new(Pool::default());
    for entries in [
        json!([{"name":"bad name","command":"unused","args":[]}]),
        json!([{"name":"x","type":"sse","url":"http://localhost"}]),
        json!([{"name":"x","type":"http","url":"http://example.com"}]),
        json!([{"name":"x","type":"http","url":"https://user:secret@example.com"}]),
        json!([{"name":"x","type":"http","url":"https://example.com","headers":[{"name":"Host","value":"other"}]}]),
        json!([{"name":"x","command":"unused","args":["a".repeat(8193)]}]),
        json!([{"name":"x","command":"unused","args":[]},{"name":"x","command":"unused","args":[]}]),
        json!([{"name":"x","command":"unused","args":[],"env":[{"name":"A","value":"1"},{"name":"A","value":"2"}]}]),
    ] {
        assert!(Servers::parse(&entries, Path::new("/workspace"), pool.clone()).is_err());
    }
    let mut connections = Vec::new();
    for i in 0..32 {
        connections.push(
            Servers::parse(
                &json!([{"name":format!("s{i}"),"command":"unused","args":[]}]),
                Path::new("/workspace"),
                pool.clone(),
            )
            .unwrap(),
        );
    }
    assert!(
        Servers::parse(
            &json!([{"name":"overflow","command":"unused","args":[]}]),
            Path::new("/workspace"),
            pool.clone()
        )
        .is_err()
    );
    drop(connections);
    assert!(
        Servers::parse(
            &json!([{"name":"reclaimed","type":"http","url":"http://[::1]:9999/mcp"}]),
            Path::new("/workspace"),
            pool
        )
        .is_ok()
    );
}

#[test]
fn computer_viewport_preserves_aspect_and_never_upscales() {
    use piko::computer::viewport;
    assert_eq!(viewport(3200, 2400), (1280, 960));
    assert_eq!(viewport(1920, 1080), (1280, 720));
    assert_eq!(viewport(1080, 1920), (540, 960));
    assert_eq!(viewport(800, 600), (800, 600));
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn computer_driver_scales_coordinates_and_rejects_unsafe_arguments() {
    use piko::computer::Desktop;
    use std::os::unix::fs::PermissionsExt;
    let temp = tempdir().unwrap();
    let program = temp.path().join("pointer");
    let log = temp.path().join("actions.json");
    let source = format!(
        "#!/usr/bin/python3\nimport json,sys\nif sys.argv[1]=='getdisplaygeometry': print('3200 2400')\nelse:\n with open({},'w') as f: json.dump(sys.argv[1:],f)\n",
        serde_json::to_string(&log).unwrap()
    );
    tokio::fs::write(&program, source).await.unwrap();
    tokio::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700))
        .await
        .unwrap();
    let config = Config {
        computer_use: true,
        computer_xdotool: program.to_string_lossy().into_owned(),
        ..Config::default()
    };
    let desktop = Desktop::default();
    desktop
        .run(
            &config,
            &json!({"action":"click","x":640,"y":480}),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    let expected = tokio::fs::read_to_string(&log).await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&expected).unwrap(),
        json!(["mousemove", "1600", "1200", "click", "1"])
    );
    for args in [
        json!({"action":"move","x":1280,"y":0}),
        json!({"action":"type","text":"x".repeat(4097)}),
        json!({"action":"key","key":"--window 1"}),
        json!({"action":"scroll","direction":"down","amount":21}),
        json!({"action":"unknown"}),
    ] {
        assert!(
            desktop
                .run(&config, &args, &CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(tokio::fs::read_to_string(&log).await.unwrap(), expected);
    }
}

#[test]
fn multimodal_outputs_bound_images_and_preserve_text_and_structure() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use piko::multimodal::{image_dimensions, normalize};
    let data = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl6mNQAAAAASUVORK5CYII=";
    assert_eq!(
        image_dimensions("image/png", &STANDARD.decode(data).unwrap()),
        Some((1, 1))
    );
    let value = json!({"content":[{"type":"text","text":"hello"},{"type":"image","mimeType":"image/png","data":data},{"type":"image","mimeType":"image/png","data":data},{"type":"resource","resource":{"text":"resource text","uri":"file:///never-fetched"}}],"structuredContent":{"answer":42}});
    let output = normalize(&value, 1024);
    assert_eq!(output.images.len(), 1);
    assert!(
        output.images[0]["image_url"]
            .as_str()
            .unwrap()
            .starts_with("data:image/png;base64,")
    );
    assert!(
        output.text.contains("hello")
            && output.text.contains("resource text")
            && output.text.contains("42")
    );
    assert!(!output.text.contains(data));
    let invalid = normalize(
        &json!({"content":[{"type":"image","mimeType":"image/svg+xml","data":"xxx"}]}),
        1024,
    );
    assert!(invalid.images.is_empty());
    let mut oversized = STANDARD.decode(data).unwrap();
    oversized[16..20].copy_from_slice(&50_000u32.to_be_bytes());
    let output = normalize(
        &json!({"content":[{"type":"image","mimeType":"image/png","data":STANDARD.encode(oversized)}]}),
        1024,
    );
    assert!(output.images.is_empty());
    assert!(image_dimensions("image/jpeg", b"bad").is_none());
    let output = normalize(
        &json!({"content":[{"type":"text","text":"x".repeat(3000)}]}),
        1024,
    );
    assert!(output.text.len() <= 1040);
}

#[test]
fn chat_images_follow_all_tool_results_and_old_images_can_be_evicted() {
    use piko::multimodal::{acp_content, trim_images};
    let image = json!({"type":"input_image","image_url":"data:image/png;base64,example"});
    let mut items = vec![
        json!({"type":"function_call","call_id":"c1","name":"computer_use","arguments":"{}"}),
        json!({"type":"function_call","call_id":"c2","name":"computer_use","arguments":"{}"}),
        json!({"type":"function_call_output","call_id":"c1","output":[{"type":"input_text","text":"first"},image]}),
        json!({"type":"function_call_output","call_id":"c2","output":[{"type":"input_text","text":"second"},image]}),
    ];
    let chat = chat_request("vision", "instructions", &items, &[], 100).unwrap();
    assert_eq!(chat["messages"][2]["role"], "tool");
    assert_eq!(chat["messages"][3]["role"], "tool");
    assert_eq!(chat["messages"][4]["role"], "user");
    assert_eq!(chat["messages"][4]["content"][1]["type"], "image_url");
    assert_eq!(
        acp_content(&items[3]["output"])[1]["content"]["type"],
        "image"
    );
    trim_images(&mut items, 40);
    assert_eq!(items[2]["output"][1]["type"], "input_text");
    assert_eq!(items[3]["output"][1]["type"], "input_image");
    assert_eq!(items.len(), 4);
}

#[test]
fn sse_handles_every_split_unicode_crlf_and_multiline() {
    let stream =
        ": comment\r\ndata: {\"hello\":\"世界\"}\r\n\r\ndata: one\ndata: two\n\ndata: [DONE]\n\n";
    for split in 0..stream.len() {
        let mut decoder = SseDecoder::default();
        let mut events = decoder.push(&stream.as_bytes()[..split]).unwrap();
        events.extend(decoder.push(&stream.as_bytes()[split..]).unwrap());
        assert_eq!(events, vec!["{\"hello\":\"世界\"}", "one\ntwo", "[DONE]"]);
    }
    assert!(
        SseDecoder::default()
            .push(&vec![b'x'; 4 * 1024 * 1024 + 1])
            .is_err()
    );
    assert!(claims("not-a-token").is_err());
    assert!(claims("header.!!!.signature").is_err());
}

#[test]
fn chat_reassembles_parallel_function_deltas_and_rejects_partial_calls() {
    let mut stream = ChatStream::default();
    stream.push(&json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","function":{"name":"grep","arguments":"{\"pattern\":"}},{"index":1,"id":"call-2","function":{"name":"glob","arguments":"{}"}}]}}]})).unwrap();
    stream.push(&json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"main\"}"}}]},"finish_reason":"tool_calls"}]})).unwrap();
    let response = stream.finish(String::new()).unwrap();
    assert!(!response.incomplete);
    assert_eq!(response.output.len(), 2);
    assert_eq!(response.output[0]["arguments"], "{\"pattern\":\"main\"}");
    assert!(ChatStream::default().finish(String::new()).is_err());
    let mut stream = ChatStream::default();
    stream.push(&json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{"}}]},"finish_reason":"tool_calls"}]})).unwrap();
    assert!(stream.finish(String::new()).is_err());
    let mut stream = ChatStream::default();
    stream
        .push(&json!({"choices":[{"delta":{},"finish_reason":"length"}]}))
        .unwrap();
    assert!(stream.finish("partial".into()).unwrap().incomplete);
}

#[test]
fn chat_translation_keeps_tool_pairing_and_omits_encrypted_reasoning() {
    let request = chat_request(
        "model",
        "instructions",
        &[
            json!({"role":"user","content":"go"}),
            json!({"type":"reasoning","encrypted_content":"not-for-chat"}),
            json!({"type":"function_call","call_id":"c1","name":"glob","arguments":"{}"}),
            json!({"type":"function_call","call_id":"c2","name":"grep","arguments":"{}"}),
            json!({"type":"function_call_output","call_id":"c1","output":"a"}),
            json!({"type":"function_call_output","call_id":"c2","output":"b"}),
        ],
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        request["messages"][2]["tool_calls"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(request["messages"][3]["tool_call_id"], "c1");
    assert!(!request.to_string().contains("not-for-chat"));
}

#[test]
fn usage_is_token_weighted_and_unknown_cache_details_stay_unknown() {
    let metrics = Metrics::default();
    let unknown = metrics
        .record(&json!({"input_tokens":100,"output_tokens":5}))
        .unwrap();
    assert!(unknown["cachedInputTokens"].is_null());
    assert!(metrics.snapshot()["inputTokenCacheHitRate"].is_null());
    metrics.record(&json!({"input_tokens":1000,"input_tokens_details":{"cached_tokens":900},"output_tokens":10}));
    metrics.record(&json!({"prompt_tokens":3000,"prompt_tokens_details":{"cached_tokens":3000},"completion_tokens":30}));
    let stats = metrics.snapshot();
    assert_eq!(stats["responsesWithCacheDetails"], 2);
    assert_eq!(stats["inputTokens"], 4000);
    assert_eq!(stats["cachedInputTokens"], 3900);
    assert!((stats["inputTokenCacheHitRate"].as_f64().unwrap() - 0.975).abs() < 1e-12);
    assert_eq!(truncate("a世界", 4), "a世\n[truncated]");
}

#[test]
fn oversized_completed_turns_are_evicted_and_refusals_are_visible() {
    let mut stream = ChatStream::default();
    assert_eq!(
        stream
            .push(&json!({"choices":[{"delta":{"refusal":"Cannot help"},"finish_reason":"stop"}]}))
            .unwrap()
            .as_deref(),
        Some("Cannot help")
    );
    let mut stream = ChatStream::with_limit(10);
    assert!(stream.push(&json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c","function":{"name":"write_file","arguments":"large-payload"}}]}}]})).is_err());
    assert!(SseDecoder::with_limit(4).push(b"12345").is_err());
}

#[tokio::test]
async fn context_projection_can_omit_every_turn_without_deleting_history_and_hardlinks_are_denied()
{
    let dir = tempdir().unwrap();
    let workspace = Workspace::new(dir.path()).await.unwrap();
    let mut session = Session::new(&workspace, &Config::default());
    session.turns.push_back(Turn {
        items: vec![json!({"role":"user","content":"x".repeat(20000)})].into(),
    });
    assert!(session.context(100).is_empty());
    assert_eq!(session.dropped_turns, 0);
    assert_eq!(session.turns.len(), 1);
    #[cfg(unix)]
    {
        let secret = dir.path().join(".env");
        std::fs::write(&secret, "sensitive marker").unwrap();
        std::fs::hard_link(&secret, dir.path().join("alias")).unwrap();
        assert!(workspace.resolve("alias", false).await.is_err());
        assert!(read_bounded(&dir.path().join("alias"), 100).await.is_err());
        let found = search(
            &workspace,
            &dir.path().join("state"),
            &Semaphore::new(1),
            "grep",
            &json!({"pattern":"sensitive","glob":null}),
            1024,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(!found["content"].as_str().unwrap().contains("marker"));
    }
}

#[tokio::test]
async fn framing_is_newline_delimited_bounded_and_accepts_eof() {
    let mut reader = BufReader::new(&b"{\"a\":1}\n{\"b\":2}"[..]);
    assert_eq!(
        read_frame(&mut reader).await.unwrap().unwrap(),
        b"{\"a\":1}\n"
    );
    assert_eq!(
        read_frame(&mut reader).await.unwrap().unwrap(),
        b"{\"b\":2}"
    );
    assert!(read_frame(&mut reader).await.unwrap().is_none());
    let oversized = vec![b'x'; 1024 * 1024 + 1];
    assert!(
        read_frame(&mut BufReader::new(oversized.as_slice()))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn workspace_rejects_traversal_and_protected_paths() {
    let dir = tempdir().unwrap();
    let workspace = Workspace::new(dir.path()).await.unwrap();
    std::fs::write(dir.path().join("existing.txt"), "readable").unwrap();
    let existing = workspace.resolve("existing.txt", false).await.unwrap();
    assert_eq!(read_bounded(&existing, 100).await.unwrap(), "readable");
    for path in [
        "../escape",
        ".git/config",
        ".env",
        ".env.local",
        ".ssh/id_rsa",
    ] {
        assert!(workspace.resolve(path, true).await.is_err(), "{path}");
    }
    assert!(
        Workspace::new(std::path::Path::new("relative"))
            .await
            .is_err()
    );
    let target = workspace.resolve("src/new.rs", true).await.unwrap();
    assert_eq!(target, workspace.root.join("src/new.rs"));
    let file = dir.path().join("binary");
    tokio::fs::write(&file, b"hello\0world").await.unwrap();
    assert!(read_bounded(&file, 100).await.is_err());
    #[cfg(unix)]
    {
        let outside = tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("link")).unwrap();
        assert!(workspace.resolve("link/new", true).await.is_err());
    }
}

#[tokio::test]
async fn persistence_is_atomic_workspace_scoped_and_private() {
    let dir = tempdir().unwrap();
    let workspace = Workspace::new(dir.path()).await.unwrap();
    let config = Config::default();
    let store = Store::new(dir.path().join("state")).await.unwrap();
    let mut session = Session::new(&workspace, &config);
    for index in 0..4 {
        session.turns.push_back(Turn {
            items: vec![json!({"role":"user","content":format!("turn-{index}-{}","x".repeat(50))})]
                .into(),
        });
    }
    let two_turn_budget = session.bytes() / 2;
    let context = session.context(two_turn_budget);
    assert_eq!(session.turns.len(), 4);
    assert_eq!(session.dropped_turns, 0);
    assert_eq!(context.len(), 2);
    store.save(&session).await.unwrap();
    let loaded = store.load(session.id, &workspace).await.unwrap();
    assert_eq!(loaded.dropped_turns, 0);
    assert_eq!(loaded.turns.len(), 4);
    let other = tempdir().unwrap();
    assert!(
        store
            .load(session.id, &Workspace::new(other.path()).await.unwrap())
            .await
            .is_err()
    );
    let mut files = tokio::fs::read_dir(dir.path().join("state")).await.unwrap();
    let file = files.next_entry().await.unwrap().unwrap();
    assert!(!file.file_name().to_string_lossy().ends_with(".tmp"));
    assert!(files.next_entry().await.unwrap().is_none());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            file.metadata().await.unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    let mut items = vec![
        json!({"type":"function_call","call_id":"interrupted","name":"shell","arguments":"{}"}),
    ];
    repair_calls(&mut items);
    repair_calls(&mut items);
    assert_eq!(items.len(), 2);
}

#[tokio::test]
async fn permissions_require_explicit_allow_and_cancel_promptly() {
    for allow in [true, false] {
        let (peer, mut rx) = Peer::channel(8);
        let client = peer.clone();
        tokio::spawn(async move {
            let message = rx.recv().await.unwrap();
            client.resolve(json!({"id":message["id"],"result":{"outcome":{"outcome":"selected","optionId":if allow {"allow-once"} else {"reject-once"}}}})).await;
        });
        assert_eq!(
            peer.permission(
                "session",
                "call",
                "edit",
                "edit",
                json!({}),
                &CancellationToken::new()
            )
            .await
            .is_ok(),
            allow
        );
    }
    let (peer, _rx) = Peer::channel(8);
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(
        peer.permission("s", "c", "title", "execute", json!({}), &cancel)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn job_admission_ownership_and_eviction_are_bounded() {
    let jobs = Jobs::new(1, 100);
    let token = CancellationToken::new();
    let job = jobs
        .reserve("agent", Some("owner".into()), token.child_token())
        .await
        .unwrap();
    assert!(
        jobs.reserve("agent", None, token.child_token())
            .await
            .is_err()
    );
    assert!(
        jobs.status(&job.id, Some("other"), 0, &token)
            .await
            .is_err()
    );
    job.finish("completed", "x".repeat(200), None, 100).await;
    assert_eq!(job.snapshot().await["truncated"], true);
    jobs.reserve("agent", None, token.child_token())
        .await
        .unwrap();
    assert!(jobs.get(&job.id).await.is_err());
    jobs.cancel_all().await;
}

#[tokio::test]
async fn search_is_ignore_aware_bounded_and_never_searches_state() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join(".gitignore"), "ignored.rs\n").unwrap();
    std::fs::write(dir.path().join("main.rs"), "fn main() {}\nneedle\n").unwrap();
    std::fs::write(dir.path().join("ignored.rs"), "needle secret").unwrap();
    std::fs::write(dir.path().join(".env"), "needle secret").unwrap();
    std::fs::create_dir(dir.path().join("state")).unwrap();
    std::fs::write(dir.path().join("state/auth.json"), "needle secret").unwrap();
    let workspace = Workspace::new(dir.path()).await.unwrap();
    let gate = Semaphore::new(1);
    let result = search(
        &workspace,
        &dir.path().join("state"),
        &gate,
        "grep",
        &json!({"pattern":"needle","glob":"**/*"}),
        1024,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let text = result["content"].as_str().unwrap();
    assert!(text.contains("main.rs:2"), "{result}");
    assert!(!text.contains("secret"), "{result}");
    let files = search(
        &workspace,
        &dir.path().join("state"),
        &gate,
        "glob",
        &json!({"pattern":"**/*"}),
        1024,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(files["content"].as_str().unwrap().trim(), "main.rs");
    std::fs::write(dir.path().join("large.rs"), "needle\n".repeat(10000)).unwrap();
    let capped = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        search(
            &workspace,
            &dir.path().join("state"),
            &gate,
            "grep",
            &json!({"pattern":"needle","glob":null}),
            100,
            &CancellationToken::new(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(capped["truncated"], true);
    // Glob metacharacters in state paths deliberately select the Rust fallback.
    std::fs::rename(dir.path().join("state"), dir.path().join("state[1]")).unwrap();
    #[cfg(unix)]
    std::fs::hard_link(
        dir.path().join("state[1]/auth.json"),
        dir.path().join("alias.rs"),
    )
    .unwrap();
    let fallback = search(
        &workspace,
        &dir.path().join("state[1]"),
        &gate,
        "grep",
        &json!({"pattern":"needle","glob":"**/*"}),
        1024 * 1024,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(fallback["engine"], "rust-regex");
    let text = fallback["content"].as_str().unwrap();
    assert!(text.contains("main.rs:2"), "{fallback}");
    assert!(
        !text.contains("secret") && !text.contains("alias"),
        "{fallback}"
    );
}

#[tokio::test]
async fn acp_initialization_modes_limits_and_resumption() {
    let dir = tempdir().unwrap();
    let config = Config {
        state_dir: dir.path().join("state"),
        max_sessions: 1,
        ..Config::default()
    };
    let (peer, mut updates) = Peer::channel(64);
    let server = Server::new(config, peer).await.unwrap();
    assert!(
        server
            .handle("session/new", json!({"cwd":dir.path(),"mcpServers":[]}))
            .await
            .is_err()
    );
    assert_eq!(
        server
            .handle("initialize", json!({"protocolVersion":1}))
            .await
            .unwrap()["protocolVersion"],
        1
    );
    let new = server
        .handle("session/new", json!({"cwd":dir.path(),"mcpServers":[]}))
        .await
        .unwrap();
    let id = new["sessionId"].clone();
    assert!(
        server
            .handle("session/new", json!({"cwd":dir.path(),"mcpServers":[]}))
            .await
            .is_err()
    );
    server
        .handle("session/set_mode", json!({"sessionId":id,"modeId":"plan"}))
        .await
        .unwrap();
    assert_eq!(
        updates.recv().await.unwrap()["params"]["update"]["currentModeId"],
        "plan"
    );
    server
        .handle("_harness/session/unload", json!({"sessionId":id}))
        .await
        .unwrap();
    let loaded = server
        .handle(
            "session/load",
            json!({"cwd":dir.path(),"sessionId":id,"mcpServers":[]}),
        )
        .await
        .unwrap();
    assert_eq!(loaded["modes"]["currentModeId"], "plan");
    assert!(
        server
            .handle(
                "session/new",
                json!({"cwd":dir.path(),"mcpServers":[{"name":"not-implemented"}]})
            )
            .await
            .is_err()
    );
    assert!(
        server.handle("_harness/stats", json!({})).await.unwrap()["inputTokenCacheHitRate"]
            .is_null()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn shell_output_timeout_and_environment_are_bounded() {
    let dir = tempdir().unwrap();
    let jobs = Jobs::new(8, 1024);
    let (peer, mut rx) = Peer::channel(32);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let cancel = CancellationToken::new();
    let output = jobs
        .shell(
            dir.path(),
            "printf 'ok'; test -z \"$OPENAI_API_KEY\"",
            2,
            false,
            None,
            &cancel,
            &peer,
            "s",
        )
        .await
        .unwrap();
    assert_eq!(output["status"], "completed");
    assert_eq!(output["output"], "ok");
    let output = jobs
        .shell(
            dir.path(),
            "sleep 30 & wait",
            1,
            false,
            None,
            &cancel,
            &peer,
            "s",
        )
        .await
        .unwrap();
    assert_eq!(output["status"], "timed_out");
    let output = jobs
        .shell(
            dir.path(),
            "yes x | head -c 100000",
            2,
            false,
            None,
            &cancel,
            &peer,
            "s",
        )
        .await
        .unwrap();
    assert_eq!(output["truncated"], true);
    assert!(output["output"].as_str().unwrap().len() <= 1024);
}

#[tokio::test]
async fn already_cancelled_shell_never_starts_or_consumes_a_job_slot() {
    let root = tempdir().unwrap();
    let jobs = Jobs::new(1, 1024);
    let cancel = CancellationToken::new();
    cancel.cancel();
    let (peer, _updates) = Peer::channel(8);
    assert!(
        jobs.shell(
            root.path(),
            "echo unexpected>should-not-exist",
            1,
            false,
            None,
            &cancel,
            &peer,
            "cancelled-test"
        )
        .await
        .is_err()
    );
    assert!(!root.path().join("should-not-exist").exists());
    assert!(jobs.list().await.is_empty());
}

#[test]
fn interruption_repair_matches_reused_call_ids_in_order() {
    let mut items = vec![
        json!({"type":"function_call","call_id":"reused"}),
        json!({"type":"function_call_output","call_id":"reused","output":"first result"}),
        json!({"type":"function_call","call_id":"reused"}),
    ];
    repair_calls(&mut items);
    assert_eq!(items.len(), 4);
    assert!(items[3]["output"].as_str().unwrap().contains("interrupted"));
    repair_calls(&mut items);
    assert_eq!(items.len(), 4);
}
