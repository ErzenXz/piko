//! Provider-specific wire options; arbitrary compatible endpoints stay conservative.
use crate::config::Config;
use anyhow::{Result, ensure};
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Profile {
    OpenAi,
    Subscription,
    OpenRouter,
    Compatible,
}

#[derive(Serialize)]
pub(crate) struct CacheOptions<'a> {
    mode: &'static str,
    ttl: &'a str,
}

#[derive(Serialize)]
pub(crate) struct CacheControl {
    r#type: &'static str,
}

#[derive(Default, Serialize)]
pub(crate) struct Options<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_options: Option<CacheOptions<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_retention: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControl>,
}

impl Profile {
    pub fn resolve(config: &Config, endpoint: &str, subscription: bool) -> Self {
        if subscription {
            return Self::Subscription;
        }
        match config.provider_profile.as_str() {
            "openai" => Self::OpenAi,
            "openrouter" => Self::OpenRouter,
            "compatible" => Self::Compatible,
            _ => match endpoint.trim_end_matches('/') {
                "https://api.openai.com/v1" => Self::OpenAi,
                "https://openrouter.ai/api/v1" => Self::OpenRouter,
                _ => Self::Compatible,
            },
        }
    }

    pub fn options<'a>(self, config: &'a Config, model: &str) -> Result<Options<'a>> {
        let mut options = Options::default();
        if let Some(tier) = config.service_tier.as_deref() {
            ensure!(
                matches!(self, Self::OpenAi | Self::Subscription),
                "service_tier requires an OpenAI provider profile"
            );
            ensure!(
                self != Self::Subscription
                    || ["auto", "default", "fast", "priority", "ultrafast"].contains(&tier),
                "Flex processing requires OpenAI API authentication"
            );
            options.service_tier = Some(if self == Self::Subscription && tier == "fast" {
                "priority"
            } else {
                tier
            });
        }
        if self == Self::OpenAi {
            if modern_cache(model) {
                ensure!(
                    config.prompt_cache_retention.is_none(),
                    "This model uses prompt_cache_ttl (30m or 12h), not legacy prompt_cache_retention"
                );
                options.prompt_cache_options = Some(CacheOptions {
                    mode: "implicit",
                    ttl: config.prompt_cache_ttl.as_deref().unwrap_or("30m"),
                });
            } else {
                ensure!(
                    config.prompt_cache_ttl.is_none(),
                    "prompt_cache_ttl requires a GPT-5.6 or later model"
                );
                ensure!(
                    !model.starts_with("gpt-5.5")
                        || config
                            .prompt_cache_retention
                            .as_deref()
                            .is_none_or(|v| v == "24h"),
                    "GPT-5.5 supports only 24h legacy prompt cache retention"
                );
                options.prompt_cache_retention =
                    config.prompt_cache_retention.as_deref().map(|value| {
                        if value == "in-memory" {
                            "in_memory"
                        } else {
                            value
                        }
                    });
            }
        } else {
            ensure!(
                config.prompt_cache_ttl.is_none() && config.prompt_cache_retention.is_none(),
                "OpenAI cache lifetime overrides require an OpenAI API provider profile"
            );
        }
        if self == Self::OpenRouter
            && config.openrouter_prompt_cache
            && model
                .strip_prefix('~')
                .unwrap_or(model)
                .starts_with("anthropic/")
        {
            options.cache_control = Some(CacheControl {
                r#type: "ephemeral",
            });
        }
        Ok(options)
    }
}

fn modern_cache(model: &str) -> bool {
    let Some(version) = model.strip_prefix("gpt-") else {
        return false;
    };
    let major = version
        .split(['.', '-'])
        .next()
        .and_then(|n| n.parse::<u32>().ok());
    major.is_some_and(|n| n >= 6)
        || (major == Some(5)
            && version
                .strip_prefix("5.")
                .and_then(|v| v.split('-').next())
                .and_then(|n| n.parse::<u32>().ok())
                .is_some_and(|n| n >= 6))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn endpoints_are_exact_and_custom_options_require_explicit_profile() {
        let config = Config::default();
        for url in [
            "https://api.openai.com.evil/v1",
            "http://127.0.0.1:1234/v1",
            "https://example.org/v1",
        ] {
            assert_eq!(Profile::resolve(&config, url, false), Profile::Compatible);
        }
        assert_eq!(
            Profile::resolve(&config, "https://openrouter.ai/api/v1/", false),
            Profile::OpenRouter
        );
        assert_eq!(
            Profile::resolve(&config, "https://api.openai.com/v1", true),
            Profile::Subscription
        );
        assert_eq!(
            serde_json::to_value(Profile::Compatible.options(&config, "gpt-6-luna").unwrap())
                .unwrap(),
            serde_json::json!({})
        );
    }
    #[test]
    fn cache_generations_and_paid_modes_are_explicit() {
        let mut config = Config::default();
        let value =
            serde_json::to_value(Profile::OpenAi.options(&config, "gpt-6-luna").unwrap()).unwrap();
        assert_eq!(
            value,
            serde_json::json!({"prompt_cache_options":{"mode":"implicit","ttl":"30m"}})
        );
        config.prompt_cache_retention = Some("in-memory".into());
        assert_eq!(
            Profile::OpenAi
                .options(&config, "gpt-5.4")
                .unwrap()
                .prompt_cache_retention,
            Some("in_memory")
        );
        assert!(Profile::OpenAi.options(&config, "gpt-5.6-sol").is_err());
        assert!(Profile::OpenAi.options(&config, "gpt-5.5").is_err());
        config.prompt_cache_retention = None;
        config.service_tier = Some("fast".into());
        assert_eq!(
            Profile::Subscription
                .options(&config, "gpt-6-luna")
                .unwrap()
                .service_tier,
            Some("priority")
        );
        assert_eq!(
            Profile::OpenAi
                .options(&config, "gpt-6-luna")
                .unwrap()
                .service_tier,
            Some("fast")
        );
        config.service_tier = Some("ultrafast".into());
        assert!(Profile::Compatible.options(&config, "model").is_err());
        assert_eq!(
            Profile::OpenAi
                .options(&config, "gpt-6.1-sol")
                .unwrap()
                .service_tier,
            Some("ultrafast")
        );
        assert!(
            Profile::OpenAi
                .options(&config, "gpt-6.1-sol")
                .unwrap()
                .prompt_cache_retention
                .is_none()
        );
    }
    #[test]
    fn router_cache_controls_are_model_specific() {
        let config = Config::default();
        assert!(
            Profile::OpenRouter
                .options(&config, "~anthropic/claude-sonnet-latest")
                .unwrap()
                .cache_control
                .is_some()
        );
        assert!(
            Profile::OpenRouter
                .options(&config, "anthropic/claude-sonnet-4.6")
                .unwrap()
                .cache_control
                .is_some()
        );
        assert!(
            Profile::OpenRouter
                .options(&config, "google/gemini-3-pro")
                .unwrap()
                .cache_control
                .is_none()
        );
        assert!(
            Profile::Compatible
                .options(&config, "anthropic/claude-sonnet-4.6")
                .unwrap()
                .cache_control
                .is_none()
        );
    }
}
