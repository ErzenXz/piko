//! Retry policy for requests which have not started delivering a response stream.
//!
//! The caller owns cancellation, the total request deadline, and bounded body reads.
//! Never apply this policy to a partially consumed stream or replay executed tools.
use reqwest::{StatusCode, header::HeaderMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) const MAX_RETRIES: u32 = 3;
const MAX_ERROR_BYTES: usize = 8 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RetryDecision {
    Stop,
    RetryAfter(Duration),
    /// Server advice cannot safely be satisfied inside this request's deadline.
    RetryLater,
}

/// Only inspect bounded structured codes; never expose upstream messages or bodies.
pub(crate) fn terminal_quota_error(body: &[u8]) -> bool {
    if body.len() > MAX_ERROR_BYTES {
        return false;
    }
    let Ok(envelope) = serde_json::from_slice::<serde_json::Value>(body) else {
        return false;
    };
    let error = &envelope["error"];
    error["details"]["error_code"].as_str() == Some("enforced_spend_limit_reached")
        || matches!(
            error["type"].as_str(),
            Some("insufficient_quota" | "billing_error")
        )
        || matches!(
            error["code"].as_str(),
            Some(
                "insufficient_quota"
                    | "credit_balance_exhausted"
                    | "organization_spend_limit_exceeded"
                    | "project_spend_limit_exceeded"
                    | "organization_usage_limit_exceeded"
                    | "billing_hard_limit_reached"
                    | "billing_not_active"
            )
        )
}

pub(crate) fn response_retry(
    status: StatusCode,
    headers: &HeaderMap,
    body: &[u8],
    attempt: u32,
    now: SystemTime,
    remaining: Duration,
) -> RetryDecision {
    if attempt >= MAX_RETRIES
        || status.is_success()
        || terminal_quota_error(body)
        || matches!(status.as_u16(), 400 | 401 | 402 | 403 | 404 | 422)
    {
        return RetryDecision::Stop;
    }
    let directive = headers.get("x-should-retry").and_then(|h| h.to_str().ok());
    if directive == Some("false")
        || !(directive == Some("true")
            || matches!(
                status.as_u16(),
                408 | 409 | 429 | 500 | 502 | 503 | 504 | 529
            ))
    {
        return RetryDecision::Stop;
    }
    // Both headers are minima if a proxy supplies conflicting advice. Taking the
    // longer valid value prevents retrying earlier than either server requested.
    let mut directed = None;
    for (name, millis) in [("retry-after-ms", true), ("retry-after", false)] {
        for header in headers.get_all(name) {
            let delay = header
                .to_str()
                .ok()
                .and_then(|value| parse_delay(value, millis, now));
            let Some(delay) = delay else {
                return RetryDecision::RetryLater;
            };
            directed = Some(directed.map_or(delay, |previous: Duration| previous.max(delay)));
        }
    }
    let delay = directed.unwrap_or_else(|| backoff(attempt));
    if delay > Duration::from_secs(30) || delay >= remaining {
        RetryDecision::RetryLater
    } else {
        RetryDecision::RetryAfter(delay)
    }
}

/// Caller must restrict transport retries to connect/timeout errors, never a
/// broad is_request() error, which can include an ambiguous POST failure.
pub(crate) fn transport_retry_delay(attempt: u32, remaining: Duration) -> Option<Duration> {
    if attempt >= MAX_RETRIES {
        return None;
    }
    let delay = backoff(attempt);
    (delay < remaining).then_some(delay)
}

fn backoff(attempt: u32) -> Duration {
    // Existing UUID entropy keeps concurrent agents from retrying in lockstep,
    // without another dependency or persistent per-agent RNG allocation.
    let entropy = uuid::Uuid::new_v4().as_u128() as u64;
    jittered_backoff(attempt, entropy)
}

fn jittered_backoff(attempt: u32, entropy: u64) -> Duration {
    let base_ms = 1_000_u64 << attempt.min(3);
    Duration::from_millis(base_ms * 3 / 4 + entropy % (base_ms / 4 + 1))
}

fn parse_delay(value: &str, millis: bool, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if value.is_empty() || value.len() > 128 {
        return None;
    }
    // Allow provider-compatible fractional delays. Round upward to a nanosecond
    // so conversion cannot shorten a server-directed minimum.
    if let Ok(number) = value.parse::<f64>() {
        let seconds = if millis { number / 1_000.0 } else { number };
        if !seconds.is_finite() || seconds < 0.0 || seconds > 31_536_000.0 {
            return None;
        }
        return Duration::try_from_secs_f64((seconds * 1e9).ceil() / 1e9).ok();
    }
    if millis {
        return None;
    }
    let date = http_date(value)?;
    Some(date.duration_since(now).unwrap_or(Duration::ZERO))
}

/// Parse the canonical IMF-fixdate form required of HTTP senders (RFC 9110).
/// Obsolete or malformed dates fail closed instead of permitting an early retry.
fn http_date(value: &str) -> Option<SystemTime> {
    let bytes = value.as_bytes();
    if bytes.len() != 29
        || !bytes.is_ascii()
        || &bytes[3..5] != b", "
        || bytes[7] != b' '
        || bytes[11] != b' '
        || bytes[16] != b' '
        || bytes[19] != b':'
        || bytes[22] != b':'
        || &bytes[25..] != b" GMT"
        || !["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"].contains(&&value[..3])
    {
        return None;
    }
    let decimal = |range: std::ops::Range<usize>| -> Option<i64> {
        let part = &value[range];
        part.bytes()
            .all(|byte| byte.is_ascii_digit())
            .then_some(())?;
        part.parse().ok()
    };
    let day = decimal(5..7)?;
    let month = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    .iter()
    .position(|month| *month == &value[8..11])? as i64
        + 1;
    let year = decimal(12..16)?;
    let hour = decimal(17..19)?;
    let minute = decimal(20..22)?;
    let second = decimal(23..25)?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if !(1970..=9999).contains(&year)
        || !(1..=days_in_month).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    // Gregorian civil date to days since Unix epoch; all arithmetic is bounded.
    let adjusted_year = year - i64::from(month <= 2);
    let era = adjusted_year / 400;
    let year_of_era = adjusted_year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let days = era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100
        + day_of_year
        - 719_468;
    let seconds = u64::try_from(days * 86_400 + hour * 3_600 + minute * 60 + second).ok()?;
    UNIX_EPOCH.checked_add(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retry(
        headers: &[(&'static str, &'static str)],
        body: &[u8],
        remaining: u64,
    ) -> RetryDecision {
        let mut map = HeaderMap::new();
        for (key, value) in headers {
            map.append(*key, value.parse().unwrap());
        }
        response_retry(
            StatusCode::TOO_MANY_REQUESTS,
            &map,
            body,
            0,
            UNIX_EPOCH,
            Duration::from_secs(remaining),
        )
    }

    #[test]
    fn fractions_and_conflicting_server_minima() {
        assert_eq!(
            retry(&[("retry-after-ms", "12.5")], b"", 30),
            RetryDecision::RetryAfter(Duration::from_micros(12_500))
        );
        assert_eq!(
            retry(&[("retry-after", "1.25")], b"", 30),
            RetryDecision::RetryAfter(Duration::from_millis(1250))
        );
        assert_eq!(
            retry(&[("retry-after-ms", "1"), ("retry-after", "10")], b"", 30),
            RetryDecision::RetryAfter(Duration::from_secs(10))
        );
        assert_eq!(
            retry(&[("retry-after", "2"), ("retry-after", "3")], b"", 30),
            RetryDecision::RetryAfter(Duration::from_secs(3))
        );
    }

    #[test]
    fn never_shorten_directed_wait_to_fit_deadline() {
        for advice in [
            "30",
            "31",
            "999999999999999999999999999999999999",
            "1e999",
            "NaN",
            "inf",
            "-1",
            "garbage",
        ] {
            assert_eq!(
                retry(&[("retry-after", advice)], b"", 30),
                RetryDecision::RetryLater,
                "{advice}"
            );
        }
        assert_eq!(
            retry(&[("retry-after", "0")], b"", 30),
            RetryDecision::RetryAfter(Duration::ZERO)
        );
    }

    #[test]
    fn canonical_dates_and_calendar_validation() {
        let date = http_date("Sun, 06 Nov 1994 08:49:37 GMT").unwrap();
        assert_eq!(
            date.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            784_111_777
        );
        assert_eq!(
            parse_delay(
                "Sun, 06 Nov 1994 08:49:37 GMT",
                false,
                date - Duration::from_secs(7)
            ),
            Some(Duration::from_secs(7))
        );
        assert_eq!(
            parse_delay(
                "Sun, 06 Nov 1994 08:49:37 GMT",
                false,
                date + Duration::from_secs(7)
            ),
            Some(Duration::ZERO)
        );
        assert!(http_date("Tue, 29 Feb 2000 00:00:00 GMT").is_some());
        for invalid in [
            "Sun, 29 Feb 2100 00:00:00 GMT",
            "Sun, 31 Apr 2026 00:00:00 GMT",
            "Sun, 06 Nov 1994 24:00:00 GMT",
            "🦀, 06 Nov 1994 08:49:37 GMT",
        ] {
            assert!(http_date(invalid).is_none());
        }
    }

    #[test]
    fn billing_is_terminal_even_if_server_says_retry() {
        for code in [
            "insufficient_quota",
            "credit_balance_exhausted",
            "organization_spend_limit_exceeded",
            "project_spend_limit_exceeded",
            "organization_usage_limit_exceeded",
            "billing_hard_limit_reached",
        ] {
            let body = format!(r#"{{"error":{{"code":"{code}","message":"private input"}}}}"#);
            assert_eq!(
                retry(&[("x-should-retry", "true")], body.as_bytes(), 30),
                RetryDecision::Stop
            );
        }
        assert_eq!(
            retry(&[], br#"{"error":{"type":"insufficient_quota"}}"#, 30),
            RetryDecision::Stop
        );
        assert_eq!(
            retry(&[("x-should-retry", "false")], b"", 30),
            RetryDecision::Stop
        );
        assert!(matches!(
            retry(&[], br#"{"error":{"code":"slow_down"}}"#, 30),
            RetryDecision::RetryAfter(_)
        ));
        assert!(terminal_quota_error(
            br#"{"error":{"code":"insufficient_\u0071uota"}}"#
        ));
        assert!(terminal_quota_error(
            br#"{"error":{"code":429,"type":"insufficient_quota"}}"#
        ));
        assert!(terminal_quota_error(br#"{"error":{"type":"rate_limit_error","details":{"error_code":"enforced_spend_limit_reached"}}}"#));
        assert!(!terminal_quota_error(&vec![b' '; MAX_ERROR_BYTES + 1]));
    }

    #[test]
    fn bounded_attempts_and_jitter() {
        for attempt in 0..3 {
            assert!(transport_retry_delay(attempt, Duration::from_secs(30)).is_some());
            let base = Duration::from_secs(1 << attempt);
            for entropy in [0, 999, u64::MAX] {
                let delay = jittered_backoff(attempt, entropy);
                assert!(delay >= base * 3 / 4 && delay <= base);
            }
        }
        assert!(transport_retry_delay(3, Duration::from_secs(30)).is_none());
        assert!(transport_retry_delay(0, Duration::ZERO).is_none());
        assert_eq!(
            response_retry(
                StatusCode::SERVICE_UNAVAILABLE,
                &HeaderMap::new(),
                b"",
                3,
                UNIX_EPOCH,
                Duration::from_secs(30)
            ),
            RetryDecision::Stop
        );
    }

    #[test]
    fn non_retryable_status_cannot_override_credentials_or_input_errors() {
        let mut headers = HeaderMap::new();
        headers.insert("x-should-retry", "true".parse().unwrap());
        for status in [200, 400, 401, 402, 403, 404, 422] {
            assert_eq!(
                response_retry(
                    StatusCode::from_u16(status).unwrap(),
                    &headers,
                    b"",
                    0,
                    UNIX_EPOCH,
                    Duration::from_secs(30)
                ),
                RetryDecision::Stop
            );
        }
    }
}
