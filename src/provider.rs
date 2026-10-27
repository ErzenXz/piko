use crate::{auth::Auth, config::Config, metrics::Metrics, protocol::Peer};
use anyhow::{Context, Result, bail, ensure};
use futures_util::StreamExt;
use serde::{Serialize, ser::SerializeSeq};
use serde_json::{Value, json};
use std::{borrow::Cow, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

const MAX_EVENT: usize = 4 * 1024 * 1024;

/// One wire array over borrowed saved exchanges and an owned active turn.
#[derive(Clone, Copy)]
pub struct Input<'a> {
    pub prefix: &'a [Cow<'a, Value>],
    pub active: &'a [Value],
}

impl<'a> Input<'a> {
    pub fn plain(active: &'a [Value]) -> Self {
        Self {
            prefix: &[],
            active,
        }
    }
    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.prefix
            .iter()
            .map(Cow::as_ref)
            .chain(self.active.iter())
    }
}

impl Serialize for Input<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.prefix.len() + self.active.len()))?;
        for item in self.iter() {
            seq.serialize_element(item)?;
        }
        seq.end()
    }
}

// Borrow conversation values until serialization; retries share the encoded body bytes.
#[derive(Serialize)]
struct ResponsesRequest<'a> {
    model: &'a str,
    instructions: &'a str,
    input: Input<'a>,
    tools: &'a serde_json::value::RawValue,
    stream: bool,
    store: bool,
    parallel_tool_calls: bool,
    tool_choice: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_output_tokens: Option<u32>,
    include: [&'static str; 1],
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<Reasoning<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prompt_cache_key: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prompt_cache_retention: Option<&'a str>,
}

#[derive(Serialize)]
struct Reasoning<'a> {
    effort: &'a str,
}

fn retryable_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

fn retry_delay(header: Option<&reqwest::header::HeaderValue>, attempt: u32) -> Result<Duration> {
    let seconds = match header {
        Some(header) => header
            .to_str()
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|seconds| *seconds <= 30)
            .context("Provider requested a later retry; wait for its Retry-After interval before trying again")?,
        None => 1 << attempt.min(3),
    };
    Ok(Duration::from_secs(seconds))
}

#[derive(Clone)]
pub struct Provider {
    client: reqwest::Client,
    config: Arc<Config>,
    auth: Auth,
    catalog: Arc<crate::models::Catalog>,
    requests: Arc<Semaphore>,
    pub metrics: Arc<Metrics>,
}

pub struct Response {
    pub output: Vec<Value>,
    pub text: String,
    pub incomplete: bool,
}

impl Provider {
    pub async fn authenticate(&self, method: &str) -> Result<()> {
        self.auth.authenticate(&self.client, method).await?;
        self.invalidate_models().await;
        Ok(())
    }

    pub async fn models(&self) -> crate::models::ModelCatalog {
        self.catalog
            .get(&self.client, &self.auth, &self.config)
            .await
    }

    pub async fn invalidate_models(&self) {
        self.catalog.invalidate(&self.config.state_dir).await;
    }

    pub fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(20))
                .timeout(Duration::from_secs(config.request_timeout_secs))
                .redirect(reqwest::redirect::Policy::none())
                .pool_max_idle_per_host(config.max_model_requests.min(256))
                .build()?,
            config: Arc::new(config.clone()),
            auth: Auth::new(config),
            catalog: Arc::new(crate::models::Catalog::default()),
            requests: Arc::new(Semaphore::new(config.max_model_requests)),
            metrics: Arc::new(Metrics::default()),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn respond(
        &self,
        model: &str,
        reasoning_effort: Option<&str>,
        instructions: &str,
        input: Input<'_>,
        tools: &[Value],
        encoded_tools: Option<&serde_json::value::RawValue>,
        peer: &Peer,
        session_id: &str,
        agent_id: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<Response> {
        // One deadline includes queueing, authentication, retries, backoff and the
        // complete SSE body. A retry never grants the model another full timeout.
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(self.config.request_timeout_secs);
        tokio::select! {
            biased;
            _ = cancel.cancelled() => bail!("Cancelled"),
            result = tokio::time::timeout_at(deadline, self.respond_inner(
                model, reasoning_effort, instructions, input, tools, encoded_tools, peer,
                session_id, agent_id, cancel,
            )) => result.context("Model request deadline exceeded; retry when ready")?,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn respond_inner(
        &self,
        model: &str,
        reasoning_effort: Option<&str>,
        instructions: &str,
        input: Input<'_>,
        tools: &[Value],
        encoded_tools: Option<&serde_json::value::RawValue>,
        peer: &Peer,
        session_id: &str,
        agent_id: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<Response> {
        let _permit = tokio::select! {
            _ = cancel.cancelled() => bail!("Cancelled"),
            permit = self.requests.acquire() => permit?,
        };
        let chat = self.config.wire_api == "chat_completions";
        let mut credentials = self.auth.credentials(&self.client, false).await?;
        let cached = credentials.account_id.is_some()
            || credentials.base_url.trim_end_matches('/') == "https://api.openai.com/v1";
        // Keep the established cache namespace across the Piko rename.
        let cache_key = cached.then(|| format!("harness-v1:{session_id}"));
        let retention = (cached && credentials.account_id.is_none())
            .then_some(self.config.prompt_cache_retention.as_deref())
            .flatten();
        let fallback_tools;
        let encoded_tools = match encoded_tools {
            Some(encoded) => encoded,
            None => {
                fallback_tools = serde_json::value::to_raw_value(tools)?;
                &fallback_tools
            }
        };
        let body = if chat {
            let mut body = chat_request_view(
                model,
                instructions,
                input,
                tools,
                self.config.max_output_tokens,
            )?;
            if let Some(effort) = reasoning_effort {
                body["reasoning_effort"] = json!(effort);
            }
            if let Some(key) = &cache_key {
                body["prompt_cache_key"] = json!(key);
            }
            if let Some(retention) = retention {
                body["prompt_cache_retention"] = json!(retention);
            }
            serde_json::to_vec(&body)?
        } else {
            serde_json::to_vec(&ResponsesRequest {
                model,
                instructions,
                input,
                tools: encoded_tools,
                stream: true,
                store: false,
                parallel_tool_calls: true,
                tool_choice: "auto",
                max_output_tokens: credentials
                    .account_id
                    .is_none()
                    .then_some(self.config.max_output_tokens),
                include: ["reasoning.encrypted_content"],
                reasoning: reasoning_effort.map(|effort| Reasoning { effort }),
                prompt_cache_key: cache_key.as_deref(),
                prompt_cache_retention: retention,
            })?
        };
        let endpoint = if chat {
            "chat/completions"
        } else {
            "responses"
        };
        let template = self
            .client
            .post(format!(
                "{}/{endpoint}",
                credentials.base_url.trim_end_matches('/')
            ))
            .header("Accept", "text/event-stream")
            .header("Content-Type", "application/json")
            .body(body);
        let mut attempt = 0;
        let mut refreshed = false;
        let response = loop {
            let mut request = template
                .try_clone()
                .context("Model request body cannot be retried")?;
            if let Some(key) = &credentials.bearer {
                request = request.bearer_auth(key);
            }
            if let Some(account) = &credentials.account_id {
                request = request
                    .header("ChatGPT-Account-Id", account)
                    .header("originator", "codex_cli_rs");
            }
            let sent = tokio::select! {
                biased;
                _ = cancel.cancelled() => bail!("Cancelled"),
                response = request.send() => response,
            };
            let response = match sent {
                Ok(response) => response,
                Err(error) => {
                    // No response bytes reached the executor. Never retry this loop
                    // after stream consumption begins, and never replay agent tools.
                    let transient = error.is_connect() || error.is_timeout() || error.is_request();
                    if transient && attempt < 3 {
                        let delay = retry_delay(None, attempt)?;
                        attempt += 1;
                        tokio::select! {
                            _ = cancel.cancelled() => bail!("Cancelled"),
                            _ = tokio::time::sleep(delay) => {}
                        }
                        continue;
                    }
                    // reqwest errors can contain credential-bearing URLs.
                    bail!("Model connection failed; check the network and provider endpoint");
                }
            };
            if response.status().is_success() {
                break response;
            }
            let status = response.status();
            if status.as_u16() == 401 && credentials.account_id.is_some() && !refreshed {
                drop(response);
                credentials = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => bail!("Cancelled"),
                    result = self.auth.credentials(&self.client, true) => result?,
                };
                refreshed = true;
                continue;
            }
            if retryable_status(status) && attempt < 3 {
                let delay = retry_delay(response.headers().get("retry-after"), attempt)?;
                drop(response);
                attempt += 1;
                tokio::select! {
                    _ = cancel.cancelled() => bail!("Cancelled"),
                    _ = tokio::time::sleep(delay) => {}
                }
                continue;
            }
            // Do not forward arbitrary upstream bodies: they may echo sensitive input.
            bail!(
                "Provider returned HTTP {} (check credentials, model, quota, and endpoint)",
                status.as_u16()
            );
        };
        // No request is retried after headers. Release the shared encoded body
        // now rather than retaining a full prompt for the duration of the stream.
        drop(template);
        ensure!(
            response
                .headers()
                .get("content-type")
                .is_none_or(|h| h
                    .to_str()
                    .is_ok_and(|mime| mime.split(';').next().is_some_and(|mime| mime
                        .trim()
                        .eq_ignore_ascii_case("text/event-stream")))),
            "Provider did not return an SSE stream"
        );
        let mut stream = response.bytes_stream();
        let mut decoder =
            SseDecoder::with_limit((self.config.context_bytes + 65536).min(MAX_EVENT));
        let mut text = String::new();
        let mut chat_stream = ChatStream::with_limit(self.config.context_bytes);
        let mut chat_usage = None;
        while let Some(chunk) = tokio::select! {
            biased;
            _ = cancel.cancelled() => bail!("Cancelled"),
            chunk = stream.next() => chunk,
        } {
            for data in decoder.push(&chunk.map_err(|_| {
                anyhow::anyhow!("Model stream interrupted; completed tools were not retried")
            })?)? {
                if data == "[DONE]" {
                    if chat {
                        self.emit_usage(peer, session_id, agent_id, chat_usage.as_ref())
                            .await?;
                        return chat_stream.finish(text);
                    }
                    continue;
                }
                let mut event: Value =
                    serde_json::from_str(&data).context("Invalid OpenAI SSE JSON")?;
                if chat {
                    if event["usage"].is_object() {
                        chat_usage = Some(event["usage"].clone());
                    }
                    ensure!(
                        event.get("error").is_none(),
                        "Provider stream reported an error"
                    );
                    let delta = chat_stream.push(&event)?;
                    ensure!(
                        text.len() + chat_stream.bytes + delta.as_ref().map_or(0, String::len)
                            <= self.config.context_bytes,
                        "Model output exceeds context limit"
                    );
                    if let Some(delta) = delta {
                        text.push_str(&delta);
                        emit_text(peer, session_id, agent_id, &delta).await?;
                    }
                    continue;
                }
                match event["type"].as_str().unwrap_or_default() {
                    "response.output_text.delta" | "response.refusal.delta" => {
                        if let Some(delta) = event["delta"].as_str() {
                            ensure!(
                                text.len() + delta.len() <= self.config.context_bytes,
                                "Model output exceeds stream limit"
                            );
                            text.push_str(delta);
                            emit_text(peer, session_id, agent_id, delta).await?;
                        }
                    }
                    "response.completed" | "response.incomplete" => {
                        let response = &event["response"];
                        self.emit_usage(peer, session_id, agent_id, response.get("usage"))
                            .await?;
                        let output = response["output"]
                            .as_array()
                            .context("Missing response output")?;
                        ensure!(output.len() <= 128, "Too many model output items");
                        ensure!(
                            crate::session::json_bytes(output)? <= self.config.context_bytes,
                            "Model output exceeds context limit"
                        );
                        for item in output {
                            if item["type"] == "function_call" {
                                ensure!(
                                    item["arguments"]
                                        .as_str()
                                        .is_some_and(|args| args.len() <= 256 * 1024),
                                    "Tool arguments exceed size limit"
                                );
                            }
                        }
                        let incomplete = event["type"] == "response.incomplete";
                        let output = event["response"]["output"].take();
                        let Value::Array(output) = output else {
                            unreachable!("validated output")
                        };
                        return Ok(Response {
                            output,
                            text,
                            incomplete,
                        });
                    }
                    "response.failed" | "error" => bail!("OpenAI stream reported an error"),
                    _ => {}
                }
            }
        }
        bail!("OpenAI stream ended before a completed response; no tools were executed")
    }

    async fn emit_usage(
        &self,
        peer: &Peer,
        session: &str,
        agent: Option<&str>,
        usage: Option<&Value>,
    ) -> Result<()> {
        if let Some(usage) = usage.and_then(|usage| self.metrics.record(usage)) {
            peer.send(json!({"jsonrpc":"2.0","method":"_harness/usage","params":{"sessionId":session,"agentId":agent,"usage":usage}})).await?;
        }
        Ok(())
    }
}

async fn emit_text(peer: &Peer, session: &str, agent: Option<&str>, delta: &str) -> Result<()> {
    if let Some(id) = agent {
        peer.task_event(session, id, json!({"textDelta":delta}))
            .await
    } else {
        peer.update(
            session,
            json!({"sessionUpdate":"agent_message_chunk", "content":{"type":"text", "text":delta}}),
        )
        .await
    }
}

pub fn chat_request(
    model: &str,
    instructions: &str,
    input: &[Value],
    tools: &[Value],
    max_tokens: u32,
) -> Result<Value> {
    chat_request_view(model, instructions, Input::plain(input), tools, max_tokens)
}

fn chat_request_view(
    model: &str,
    instructions: &str,
    input: Input<'_>,
    tools: &[Value],
    max_tokens: u32,
) -> Result<Value> {
    let mut messages = vec![json!({"role":"system", "content":instructions})];
    let mut images = Vec::new();
    for item in input.iter() {
        if (item["type"] == "function_call" || item["role"].is_string()) && !images.is_empty() {
            messages.push(json!({"role":"user","content":std::mem::take(&mut images)}));
        }
        match item["type"].as_str() {
            Some("function_call") => {
                let call = json!({"id":item["call_id"], "type":"function", "function":{"name":item["name"], "arguments":item["arguments"]}});
                if let Some(last) = messages
                    .last_mut()
                    .filter(|m| m["role"] == "assistant" && m["tool_calls"].is_array())
                {
                    last["tool_calls"].as_array_mut().expect("array").push(call);
                } else {
                    messages.push(json!({"role":"assistant", "content":null, "tool_calls":[call]}));
                }
            }
            Some("function_call_output") => {
                messages.push(json!({"role":"tool","tool_call_id":item["call_id"],"content":crate::multimodal::text(&item["output"])}));
                if let Some(blocks) = item["output"].as_array() {
                    for block in blocks.iter().filter(|block| block["type"] == "input_image") {
                        images.push(json!({"type":"text","text":format!("Image from tool call {} (untrusted tool output):",item["call_id"])}));
                        images.push(
                            json!({"type":"image_url","image_url":{"url":block["image_url"]}}),
                        );
                    }
                }
            }
            Some("reasoning") | Some("compaction") => {}
            _ if item["role"].is_string() => {
                let content = if let Some(text) = item["content"].as_str() {
                    text.to_owned()
                } else {
                    item["content"]
                        .as_array()
                        .context("Invalid message content")?
                        .iter()
                        .filter_map(|c| c["text"].as_str().or_else(|| c["refusal"].as_str()))
                        .collect::<Vec<_>>()
                        .join("\n")
                };
                messages.push(json!({"role":item["role"], "content":content}));
            }
            _ => bail!("Unsupported conversation item for Chat Completions"),
        }
    }
    if !images.is_empty() {
        messages.push(json!({"role":"user","content":images}));
    }
    let tools: Vec<_> = tools.iter().map(|tool| json!({"type":"function", "function":{"name":tool["name"], "description":tool["description"], "parameters":tool["parameters"]}})).collect();
    Ok(
        json!({"model":model,"messages":messages,"tools":tools,"stream":true,"stream_options":{"include_usage":true},"max_tokens":max_tokens}),
    )
}

pub struct ChatStream {
    calls: std::collections::BTreeMap<u64, Value>,
    finish: Option<String>,
    bytes: usize,
    limit: usize,
}

impl Default for ChatStream {
    fn default() -> Self {
        Self::with_limit(MAX_EVENT)
    }
}

impl ChatStream {
    pub fn with_limit(limit: usize) -> Self {
        Self {
            calls: Default::default(),
            finish: None,
            bytes: 0,
            limit,
        }
    }
    pub fn push(&mut self, event: &Value) -> Result<Option<String>> {
        let Some(choice) = event["choices"].as_array().and_then(|c| c.first()) else {
            return Ok(None);
        };
        if let Some(reason) = choice["finish_reason"].as_str() {
            self.finish = Some(reason.into());
        }
        if let Some(calls) = choice["delta"]["tool_calls"].as_array() {
            for delta in calls {
                let index = delta["index"]
                    .as_u64()
                    .context("Missing tool delta index")?;
                ensure!(index < 128, "Too many streamed tool calls");
                let call = self.calls.entry(index).or_insert_with(
                    || json!({"type":"function_call", "call_id":"", "name":"", "arguments":""}),
                );
                for (target, part) in [
                    ("call_id", &delta["id"]),
                    ("name", &delta["function"]["name"]),
                    ("arguments", &delta["function"]["arguments"]),
                ] {
                    if let Some(part) = part.as_str() {
                        self.bytes += part.len();
                        ensure!(
                            self.bytes <= self.limit,
                            "Tool payload exceeds context limit"
                        );
                        let Value::String(current) = &mut call[target] else {
                            unreachable!("streamed call fields are strings")
                        };
                        ensure!(
                            current.len() + part.len() <= 256 * 1024,
                            "Tool arguments exceed size limit"
                        );
                        current.push_str(part);
                    }
                }
            }
        }
        Ok(choice["delta"]["content"]
            .as_str()
            .or_else(|| choice["delta"]["refusal"].as_str())
            .map(str::to_owned))
    }

    pub fn finish(self, text: String) -> Result<Response> {
        let reason = self
            .finish
            .context("Chat stream ended without a finish reason")?;
        let mut output = Vec::new();
        if !text.is_empty() {
            output.push(json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":text}],"phase":"final_answer"}));
        }
        for call in self.calls.into_values() {
            ensure!(
                call["call_id"].as_str().is_some_and(|s| !s.is_empty())
                    && call["name"].as_str().is_some_and(|s| !s.is_empty()),
                "Incomplete streamed tool call"
            );
            output.push(call);
        }
        ensure!(
            crate::session::json_bytes(&output)? <= self.limit,
            "Model output exceeds context limit"
        );
        Ok(Response {
            output,
            text,
            incomplete: !["stop", "tool_calls"].contains(&reason.as_str()),
        })
    }
}

pub struct SseDecoder {
    line: Vec<u8>,
    data: String,
    data_seen: bool,
    first_line: bool,
    skip_lf: bool,
    limit: usize,
}

impl Default for SseDecoder {
    fn default() -> Self {
        Self::with_limit(MAX_EVENT)
    }
}

impl SseDecoder {
    pub fn with_limit(limit: usize) -> Self {
        Self {
            line: Vec::new(),
            data: String::new(),
            data_seen: false,
            first_line: true,
            skip_lf: false,
            limit,
        }
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<String>> {
        let mut events = Vec::new();
        // SSE permits CR, LF and CRLF. A CR completes the line immediately;
        // its optional LF is ignored even when it arrives in the next chunk.
        // UTF-8 (including a split initial BOM) is decoded only at line endings.
        for mut fragment in bytes.split_inclusive(|byte| matches!(byte, b'\r' | b'\n')) {
            if self.skip_lf {
                self.skip_lf = false;
                if fragment.first() == Some(&b'\n') {
                    fragment = &fragment[1..];
                    if fragment.is_empty() {
                        continue;
                    }
                }
            }
            let complete = fragment
                .last()
                .is_some_and(|byte| matches!(byte, b'\r' | b'\n'));
            let content = if complete {
                self.skip_lf = fragment.last() == Some(&b'\r');
                &fragment[..fragment.len() - 1]
            } else {
                fragment
            };
            ensure!(
                self.line.len() + self.data.len() + content.len() <= self.limit,
                "SSE event exceeds size limit"
            );
            self.line.extend_from_slice(content);
            if !complete {
                continue;
            }
            let mut line = std::str::from_utf8(&self.line).context("Invalid SSE UTF-8")?;
            if self.first_line {
                line = line.strip_prefix('\u{feff}').unwrap_or(line);
                self.first_line = false;
            }
            if line.is_empty() {
                if self.data_seen {
                    events.push(std::mem::take(&mut self.data));
                    self.data_seen = false;
                }
            } else if let Some(data) = line.strip_prefix("data:") {
                if self.data_seen {
                    self.data.push('\n');
                }
                self.data.push_str(data.strip_prefix(' ').unwrap_or(data));
                self.data_seen = true;
            } else if line == "data" {
                if self.data_seen {
                    self.data.push('\n');
                }
                self.data_seen = true;
            }
            self.line.clear();
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn many_small_tool_deltas_append_without_replacing_the_buffer() {
        let mut stream = ChatStream::default();
        stream.push(&json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"one","function":{"name":"write_file","arguments":""}}]}}]})).unwrap();
        // Reserve once to detect accidental replacement/reallocation on each delta.
        let Value::String(arguments) = &mut stream.calls.get_mut(&0).unwrap()["arguments"] else {
            panic!()
        };
        arguments.reserve(4096);
        let buffer = arguments.as_ptr();
        let delta = json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"x"}}]}}]});
        for _ in 0..4096 {
            stream.push(&delta).unwrap();
        }
        let Value::String(arguments) = &stream.calls[&0]["arguments"] else {
            panic!()
        };
        assert_eq!(arguments.len(), 4096);
        assert_eq!(arguments.as_ptr(), buffer);
        stream
            .push(&json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}))
            .unwrap();
        assert_eq!(
            stream.finish(String::new()).unwrap().output[0]["arguments"],
            "x".repeat(4096)
        );
    }

    #[test]
    fn sse_limits_apply_across_fragments_and_reset_per_event() {
        let mut decoder = SseDecoder::with_limit(12);
        assert!(decoder.push(b"data: hello").unwrap().is_empty());
        assert!(decoder.push(b"!\n").unwrap().is_empty());
        assert_eq!(
            decoder.push(b"\ndata: next\n\n").unwrap(),
            ["hello!", "next"]
        );
        let mut decoder = SseDecoder::with_limit(12);
        decoder.push(b"data: hello").unwrap();
        assert!(decoder.push(b"!!").is_err());
    }

    #[test]
    fn borrowed_responses_request_preserves_optional_fields() {
        let input = [json!({"role":"user","content":"hello"})];
        let tools = [json!({"type":"function","name":"read_file"})];
        let encoded = serde_json::value::to_raw_value(&tools).unwrap();
        let mut request = ResponsesRequest {
            model: "model",
            instructions: "instructions",
            input: Input::plain(&input),
            tools: &encoded,
            stream: true,
            store: false,
            parallel_tool_calls: true,
            tool_choice: "auto",
            max_output_tokens: Some(1024),
            include: ["reasoning.encrypted_content"],
            reasoning: Some(Reasoning { effort: "high" }),
            prompt_cache_key: Some("harness-v1:session"),
            prompt_cache_retention: Some("24h"),
        };
        assert!(std::ptr::eq(request.input.active.as_ptr(), input.as_ptr()));
        assert!(std::ptr::eq(request.tools, encoded.as_ref()));
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(value["input"], json!(input));
        assert_eq!(value["tools"], json!(tools));
        assert_eq!(value["max_output_tokens"], 1024);
        assert_eq!(value["reasoning"]["effort"], "high");
        assert_eq!(value["prompt_cache_retention"], "24h");
        request.max_output_tokens = None;
        request.reasoning = None;
        request.prompt_cache_key = None;
        request.prompt_cache_retention = None;
        let value = serde_json::to_value(&request).unwrap();
        for key in [
            "max_output_tokens",
            "reasoning",
            "prompt_cache_key",
            "prompt_cache_retention",
        ] {
            assert!(value.get(key).is_none());
        }
    }

    #[test]
    fn retry_policy_preserves_server_delay_and_rejects_long_or_date_advice() {
        for status in [408, 429, 500, 502, 503, 504] {
            assert!(retryable_status(
                reqwest::StatusCode::from_u16(status).unwrap()
            ));
        }
        for status in [400, 401, 403, 404, 501, 505] {
            assert!(!retryable_status(
                reqwest::StatusCode::from_u16(status).unwrap()
            ));
        }
        assert_eq!(retry_delay(None, 2).unwrap(), Duration::from_secs(4));
        for seconds in [0, 1, 30] {
            let header = reqwest::header::HeaderValue::from_str(&seconds.to_string()).unwrap();
            assert_eq!(
                retry_delay(Some(&header), 0).unwrap(),
                Duration::from_secs(seconds)
            );
        }
        for value in ["31", "90", "Wed, 21 Oct 2026 07:28:00 GMT", "invalid"] {
            let header = reqwest::header::HeaderValue::from_str(value).unwrap();
            assert!(retry_delay(Some(&header), 0).is_err());
        }
    }

    #[test]
    fn sse_accepts_bom_and_all_line_endings_at_every_chunk_boundary() {
        let stream = "\u{feff}data: héllo\r\rdata: second\r\ndata: line\n\ndata:\r\r".as_bytes();
        for boundary in 0..=stream.len() {
            let mut decoder = SseDecoder::with_limit(128);
            let mut events = decoder.push(&stream[..boundary]).unwrap();
            events.extend(decoder.push(&stream[boundary..]).unwrap());
            assert_eq!(events, ["héllo", "second\nline", ""], "split at {boundary}");
        }
        let mut decoder = SseDecoder::with_limit(128);
        let mut events = Vec::new();
        for byte in stream {
            events.extend(decoder.push(std::slice::from_ref(byte)).unwrap());
        }
        assert_eq!(events, ["héllo", "second\nline", ""]);
    }

    #[test]
    fn sse_cr_and_multiline_data_remain_bounded() {
        let mut decoder = SseDecoder::with_limit(12);
        decoder.push(b"data: hello\r").unwrap();
        assert!(decoder.push(b"data: next\r").is_err());
        let mut decoder = SseDecoder::with_limit(32);
        assert_eq!(
            decoder.push(b"data\rdata: second\r\r").unwrap(),
            ["\nsecond"]
        );
    }

    #[test]
    fn retry_templates_share_the_already_encoded_body() {
        let template = reqwest::Client::new()
            .post("http://127.0.0.1/responses")
            .body(vec![b'x'; 4096]);
        let first = template.try_clone().unwrap().build().unwrap();
        let second = template.try_clone().unwrap().build().unwrap();
        let first = first.body().unwrap().as_bytes().unwrap();
        let second = second.body().unwrap().as_bytes().unwrap();
        assert_eq!(first, second);
        assert!(std::ptr::eq(first.as_ptr(), second.as_ptr()));
    }

    #[tokio::test]
    async fn already_cancelled_response_never_waits_for_credentials_or_network() {
        let provider = Provider::new(&Config::default()).unwrap();
        let (peer, _outbound) = Peer::channel(1);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let error = provider
            .respond(
                "model",
                None,
                "instructions",
                Input::plain(&[]),
                &[],
                None,
                &peer,
                "session",
                None,
                &cancel,
            )
            .await
            .err()
            .unwrap();
        assert_eq!(error.to_string(), "Cancelled");
    }
}
