use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Metrics {
    responses: AtomicU64,
    observed: AtomicU64,
    input: AtomicU64,
    total_input: AtomicU64,
    cached: AtomicU64,
    output: AtomicU64,
    total_output: AtomicU64,
    writes: AtomicU64,
    write_observed: AtomicU64,
}

struct Usage {
    input: u64,
    output: u64,
    cached: Option<u64>,
    writes: Option<u64>,
}

impl Usage {
    fn parse(usage: &Value) -> Option<Self> {
        let number =
            |camel: &str, snake: &str| usage[camel].as_u64().or_else(|| usage[snake].as_u64());
        let anthropic = usage.get("cache_read_input_tokens").is_some()
            || usage.get("cache_creation_input_tokens").is_some();
        let native_gemini = number("promptTokenCount", "prompt_token_count");
        let (input, cached, writes) = if anthropic {
            // Anthropic's input_tokens EXCLUDES cache reads and writes. The
            // cache_creation lifetime breakdown is already included in its total.
            let ordinary = usage["input_tokens"].as_u64()?;
            let reads = usage["cache_read_input_tokens"].as_u64();
            let writes = usage["cache_creation_input_tokens"].as_u64();
            // Do not report an incomplete Anthropic input total as complete.
            let input = ordinary.checked_add(reads?)?.checked_add(writes?)?;
            (input, reads, writes)
        } else if let Some(input) = native_gemini {
            (
                input,
                number("cachedContentTokenCount", "cached_content_token_count"),
                None,
            )
        } else {
            let input = usage["input_tokens"]
                .as_u64()
                .or_else(|| usage["prompt_tokens"].as_u64())
                .or_else(|| {
                    usage["prompt_cache_hit_tokens"]
                        .as_u64()?
                        .checked_add(usage["prompt_cache_miss_tokens"].as_u64()?)
                })?;
            let details = if usage["input_tokens_details"].is_object() {
                &usage["input_tokens_details"]
            } else {
                &usage["prompt_tokens_details"]
            };
            (
                input,
                details["cached_tokens"]
                    .as_u64()
                    .or_else(|| usage["prompt_cache_hit_tokens"].as_u64()),
                details["cache_write_tokens"].as_u64(),
            )
        };
        // Cache writes are distinct from reads, and both are subsets of the
        // inclusive input total. Malformed values are unknown, never clamped.
        let mut cached = cached.filter(|count| *count <= input);
        let mut writes = writes.filter(|count| *count <= input);
        if cached
            .zip(writes)
            .is_some_and(|(reads, writes)| reads > input - writes)
        {
            cached = None;
            writes = None;
        }
        let output = usage["output_tokens"]
            .as_u64()
            .or_else(|| usage["completion_tokens"].as_u64())
            .or_else(|| {
                native_gemini?;
                number("candidatesTokenCount", "candidates_token_count")
                    .unwrap_or(0)
                    .checked_add(number("thoughtsTokenCount", "thoughts_token_count").unwrap_or(0))
            })
            .unwrap_or(0);
        Some(Self {
            input,
            output,
            cached,
            writes,
        })
    }
}

impl Metrics {
    pub fn record(&self, usage: &Value) -> Option<Value> {
        let parsed = Usage::parse(usage)?;
        let Usage {
            input,
            output,
            cached,
            writes,
        } = parsed;
        self.responses.fetch_add(1, Ordering::Relaxed);
        self.total_input.fetch_add(input, Ordering::Relaxed);
        self.total_output.fetch_add(output, Ordering::Relaxed);
        // Missing cached-token details are unknown, not evidence of zero hits.
        if let Some(cached) = cached {
            self.observed.fetch_add(1, Ordering::Relaxed);
            self.input.fetch_add(input, Ordering::Relaxed);
            self.cached.fetch_add(cached, Ordering::Relaxed);
            self.output.fetch_add(output, Ordering::Relaxed);
        }
        if let Some(writes) = writes {
            self.write_observed.fetch_add(1, Ordering::Relaxed);
            self.writes.fetch_add(writes, Ordering::Relaxed);
        }
        let mut normalized = json!({
            "inputTokens":input,
            "cachedInputTokens":cached,
            "cacheWriteInputTokens":writes,
            "outputTokens":output,
            "inputTokenCacheHitRate":cached.and_then(|cached| (input>0).then_some(cached as f64/input as f64))
        });
        if let Some(tier) = usage["service_tier"]
            .as_str()
            .or_else(|| usage["serviceTier"].as_str())
            .filter(|tier| tier.len() <= 64)
        {
            normalized["serviceTier"] = json!(tier);
        }
        Some(normalized)
    }

    pub fn snapshot(&self) -> Value {
        let responses = self.responses.load(Ordering::Relaxed);
        let observed = self.observed.load(Ordering::Relaxed);
        let input = self.input.load(Ordering::Relaxed);
        let cached = self.cached.load(Ordering::Relaxed);
        let write_observed = self.write_observed.load(Ordering::Relaxed);
        json!({
            "responsesWithUsage":responses,
            "responsesWithCacheDetails":observed,
            // Compatibility aliases cover ONLY responses with known cache reads.
            "inputTokens":input,
            "outputTokens":self.output.load(Ordering::Relaxed),
            "inputTokensWithCacheDetails":input,
            "totalInputTokens":self.total_input.load(Ordering::Relaxed),
            "totalOutputTokens":self.total_output.load(Ordering::Relaxed),
            "cachedInputTokens":cached,
            "responsesWithCacheWriteDetails":write_observed,
            "cacheWriteInputTokens":(write_observed>0).then(||self.writes.load(Ordering::Relaxed)),
            "cacheDetailsCoverage":(responses>0).then_some(observed as f64/responses as f64),
            "inputTokenCacheHitRate":(input>0).then_some(cached as f64/input as f64)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn totals_include_unknown_cache_usage_without_diluting_known_hit_rate() {
        let metrics = Metrics::default();
        metrics
            .record(&json!({"input_tokens":100,"output_tokens":5}))
            .unwrap();
        metrics
            .record(&json!({"input_tokens":900,"output_tokens":15,
            "input_tokens_details":{"cached_tokens":450,"cache_write_tokens":300}}))
            .unwrap();
        let stats = metrics.snapshot();
        assert_eq!(stats["totalInputTokens"], 1000);
        assert_eq!(stats["totalOutputTokens"], 20);
        assert_eq!(stats["inputTokensWithCacheDetails"], 900);
        assert_eq!(stats["cacheWriteInputTokens"], 300);
        assert_eq!(stats["inputTokenCacheHitRate"], 0.5);
        assert_eq!(stats["cacheDetailsCoverage"], 0.5);
        assert_eq!(stats["responsesWithUsage"], 2);
    }

    #[test]
    fn anthropic_reads_and_writes_are_added_once() {
        let stats = Metrics::default()
            .record(&json!({
                "input_tokens":20,"output_tokens":7,"cache_read_input_tokens":50,
                "cache_creation_input_tokens":30,"cache_creation":{
                    "ephemeral_5m_input_tokens":10,"ephemeral_1h_input_tokens":20}
            }))
            .unwrap();
        assert_eq!(stats["inputTokens"], 100);
        assert_eq!(stats["cachedInputTokens"], 50);
        assert_eq!(stats["cacheWriteInputTokens"], 30);
        assert!(
            Metrics::default()
                .record(&json!({
            "input_tokens":20,"cache_read_input_tokens":50}))
                .is_none()
        );
    }

    #[test]
    fn deepseek_and_gemini_formats_keep_effective_input_and_reasoning_output() {
        let metrics = Metrics::default();
        let deepseek = metrics
            .record(&json!({"prompt_tokens":100,
            "prompt_cache_hit_tokens":80,"prompt_cache_miss_tokens":20,
            "completion_tokens":5}))
            .unwrap();
        assert_eq!(deepseek["inputTokens"], 100);
        assert_eq!(deepseek["cachedInputTokens"], 80);
        for usage in [
            json!({"promptTokenCount":100,"cachedContentTokenCount":70,
            "candidatesTokenCount":10,"thoughtsTokenCount":5}),
            json!({"prompt_token_count":100,"cached_content_token_count":70,
                "candidates_token_count":10,"thoughts_token_count":5}),
        ] {
            let gemini = metrics.record(&usage).unwrap();
            assert_eq!(gemini["inputTokens"], 100);
            assert_eq!(gemini["cachedInputTokens"], 70);
            assert_eq!(gemini["outputTokens"], 15);
        }
    }

    #[test]
    fn writes_are_never_hits_and_unknown_or_invalid_details_remain_unknown() {
        let reasoning_only = Metrics::default()
            .record(&json!({"promptTokenCount":100,"thoughtsTokenCount":15}))
            .unwrap();
        assert_eq!(reasoning_only["outputTokens"], 15);
        let metrics = Metrics::default();
        let writes = metrics
            .record(&json!({"prompt_tokens":100,
            "prompt_tokens_details":{"cache_write_tokens":90},
            "service_tier":"default"}))
            .unwrap();
        assert!(writes["cachedInputTokens"].is_null());
        assert!(writes["inputTokenCacheHitRate"].is_null());
        assert_eq!(writes["cacheWriteInputTokens"], 90);
        assert_eq!(writes["serviceTier"], "default");
        let invalid = metrics
            .record(&json!({"input_tokens":100,
            "input_tokens_details":{"cached_tokens":90,"cache_write_tokens":90}}))
            .unwrap();
        assert!(invalid["cachedInputTokens"].is_null());
        assert!(invalid["cacheWriteInputTokens"].is_null());
        assert_eq!(metrics.snapshot()["responsesWithCacheDetails"], 0);
        assert!(metrics.snapshot()["inputTokenCacheHitRate"].is_null());
        let empty = Metrics::default().snapshot();
        assert!(empty["cacheDetailsCoverage"].is_null());
        assert!(empty["cacheWriteInputTokens"].is_null());
    }
}
