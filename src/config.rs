use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub auth: String,
    pub wire_api: String,
    pub api_key_env: String,
    pub model: String,
    pub base_url: String,
    pub reasoning_effort: Option<String>,
    pub prompt_cache_retention: Option<String>,
    pub state_dir: PathBuf,
    pub max_turn_steps: usize,
    pub max_subagent_depth: usize,
    pub max_subagents: usize,
    pub max_model_requests: usize,
    pub max_active_turns: usize,
    pub max_pending_prompt_bytes: usize,
    pub resident_history_bytes: usize,
    pub retained_agent_context_bytes: usize,
    pub max_mcp_connections: usize,
    pub max_searches: usize,
    pub max_jobs: usize,
    pub max_processes: usize,
    pub max_sessions: usize,
    pub context_bytes: usize,
    pub max_output_tokens: u32,
    pub tool_output_bytes: usize,
    pub command_timeout_secs: u64,
    pub request_timeout_secs: u64,
    pub computer_use: bool,
    pub computer_xdotool: String,
    pub computer_import: String,
    pub web_enabled: bool,
    pub web_search_api_key_env: String,
    pub web_search_url: String,
    pub browser_cdp_url: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            auth: "auto".into(),
            wire_api: "responses".into(),
            api_key_env: "OPENAI_API_KEY".into(),
            model: "gpt-6-luna".into(),
            base_url: "https://api.openai.com/v1".into(),
            reasoning_effort: None,
            prompt_cache_retention: None,
            state_dir: default_state_dir(),
            max_turn_steps: 80,
            max_subagent_depth: 2,
            max_subagents: 256,
            max_model_requests: 256,
            max_active_turns: 32,
            max_pending_prompt_bytes: 64 * 1024 * 1024,
            resident_history_bytes: 64 * 1024,
            retained_agent_context_bytes: 2 * 1024 * 1024,
            max_mcp_connections: 32,
            max_searches: 8,
            max_jobs: 16,
            max_processes: 32,
            max_sessions: 256,
            context_bytes: 384 * 1024,
            max_output_tokens: 16384,
            tool_output_bytes: 32 * 1024,
            command_timeout_secs: 120,
            request_timeout_secs: 300,
            computer_use: false,
            computer_xdotool: "xdotool".into(),
            computer_import: "import".into(),
            web_enabled: true,
            web_search_api_key_env: "BRAVE_SEARCH_API_KEY".into(),
            web_search_url: "https://api.search.brave.com/res/v1/web/search".into(),
            browser_cdp_url: None,
        }
    }
}

impl Config {
    pub async fn load(path: Option<&Path>) -> Result<Self> {
        // Never load executable settings from the model-controlled working tree.
        let mut config: Self = match path {
            Some(path) => toml::from_str(&tokio::fs::read_to_string(path).await?)
                .context("Invalid Piko configuration")?,
            None => Self::default(),
        };
        for (name, target) in [
            ("OPENAI_MODEL", &mut config.model),
            ("OPENAI_BASE_URL", &mut config.base_url),
        ] {
            if let Ok(value) = std::env::var(name) {
                *target = value;
            }
        }
        if let Some(path) = std::env::var_os("PIKO_STATE_DIR")
            .or_else(|| std::env::var_os("CODEX_HARNESS_STATE_DIR"))
        {
            config.state_dir = path.into();
        }
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            model_identifier(&self.model),
            "model must contain 1..=256 UTF-8 bytes and no control characters"
        );
        ensure!(
            (1..=256).contains(&self.max_mcp_connections),
            "max_mcp_connections must be 1..=256"
        );
        for program in [&self.computer_xdotool, &self.computer_import] {
            ensure!(
                !program.is_empty() && program.len() <= 4096 && !program.contains('\0'),
                "Invalid computer backend executable"
            );
        }
        ensure!(
            !self.web_search_api_key_env.is_empty()
                && self.web_search_api_key_env.len() <= 256
                && self
                    .web_search_api_key_env
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "Invalid search API key environment variable"
        );
        let search =
            reqwest::Url::parse(&self.web_search_url).context("Invalid search endpoint")?;
        let local = matches!(search.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
        ensure!(
            search.host_str().is_some()
                && search.username().is_empty()
                && search.password().is_none()
                && search.query().is_none()
                && search.fragment().is_none()
                && (search.scheme() == "https" || (local && search.scheme() == "http")),
            "Search endpoint requires HTTPS without URL credentials, query or fragment (except loopback)"
        );
        if let Some(endpoint) = &self.browser_cdp_url {
            crate::browser::validate_endpoint(endpoint)?;
        }
        ensure!(
            self.state_dir.is_absolute() && self.state_dir.parent().is_some(),
            "state_dir must be absolute and cannot be a filesystem root"
        );
        if let Some(retention) = &self.prompt_cache_retention {
            ensure!(
                ["in-memory", "24h"].contains(&retention.as_str()),
                "prompt_cache_retention must be in-memory or 24h (only for supported OpenAI models)"
            );
        }
        ensure!(
            ["auto", "chatgpt", "api_key", "none"].contains(&self.auth.as_str()),
            "auth must be auto, chatgpt, api_key, or none"
        );
        ensure!(
            ["responses", "chat_completions"].contains(&self.wire_api.as_str()),
            "wire_api must be responses or chat_completions"
        );
        ensure!(!self.api_key_env.is_empty(), "api_key_env cannot be empty");
        let url = reqwest::Url::parse(&self.base_url).context("Invalid OpenAI base URL")?;
        let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
        ensure!(
            url.username().is_empty() && url.password().is_none(),
            "Credentials must not be in the base URL"
        );
        ensure!(
            url.query().is_none() && url.fragment().is_none(),
            "Base URL cannot contain query or fragment"
        );
        ensure!(
            url.scheme() == "https" || (local && url.scheme() == "http"),
            "Provider URL requires HTTPS (except loopback)"
        );
        ensure!(!self.model.trim().is_empty(), "Model cannot be empty");
        ensure!(
            self.max_turn_steps > 0 && self.max_turn_steps <= 1000,
            "max_turn_steps must be 1..=1000"
        );
        ensure!(
            self.max_subagent_depth <= 4,
            "max_subagent_depth must be <= 4"
        );
        ensure!(
            (1..=1024).contains(&self.max_subagents),
            "max_subagents must be 1..=1024"
        );
        ensure!(
            (1..=1024).contains(&self.max_jobs),
            "max_jobs must be 1..=1024"
        );
        ensure!(
            (1..=1024).contains(&self.max_processes),
            "max_processes must be 1..=1024"
        );
        ensure!(
            (1..=1024).contains(&self.max_sessions),
            "max_sessions must be 1..=1024"
        );
        ensure!(
            (1..=1024).contains(&self.max_model_requests),
            "max_model_requests must be 1..=1024"
        );
        ensure!(
            (1..=1024).contains(&self.max_active_turns),
            "max_active_turns must be 1..=1024"
        );
        ensure!(
            (1024 * 1024..=1024 * 1024 * 1024).contains(&self.max_pending_prompt_bytes),
            "max_pending_prompt_bytes must be 1MiB..=1GiB"
        );
        ensure!(
            self.resident_history_bytes <= 16 * 1024 * 1024,
            "resident_history_bytes must be 0..=16MiB"
        );
        ensure!(
            (64 * 1024..=64 * 1024 * 1024).contains(&self.retained_agent_context_bytes),
            "retained_agent_context_bytes must be 64KiB..=64MiB"
        );
        ensure!(
            (1..=32).contains(&self.max_searches),
            "max_searches must be 1..=32"
        );
        ensure!(
            (16384..=4 * 1024 * 1024).contains(&self.context_bytes),
            "context_bytes must be 16KiB..=4MiB"
        );
        ensure!(
            (1024..=256 * 1024).contains(&self.tool_output_bytes),
            "tool_output_bytes must be 1KiB..=256KiB"
        );
        ensure!(
            (256..=65536).contains(&self.max_output_tokens),
            "max_output_tokens must be 256..=65536"
        );
        ensure!(
            (1..=3600).contains(&self.command_timeout_secs),
            "command_timeout_secs must be 1..=3600"
        );
        ensure!(
            (1..=3600).contains(&self.request_timeout_secs),
            "request_timeout_secs must be 1..=3600"
        );
        if let Some(effort) = &self.reasoning_effort
            && ![
                "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
            ]
            .contains(&effort.as_str())
        {
            bail!("Unsupported reasoning_effort");
        }
        Ok(())
    }
}

pub(crate) fn model_identifier(model: &str) -> bool {
    !model.trim().is_empty() && model.len() <= 256 && !model.chars().any(char::is_control)
}

// Keep the established XDG layout on Unix; Windows has no HOME by default.
fn default_state_dir() -> PathBuf {
    #[cfg(windows)]
    let state = std::env::var_os("LOCALAPPDATA")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("USERPROFILE")
                .filter(|path| !path.is_empty())
                .map(|home| PathBuf::from(home).join("AppData/Local"))
        });
    #[cfg(not(windows))]
    let state = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state")));
    state_directory(
        state
            .filter(|path| path.is_absolute())
            .unwrap_or_else(|| PathBuf::from(".")),
    )
}

// Reuse the established directory so a rename never strands credentials or sessions.
// An explicitly configured state_dir or PIKO_STATE_DIR takes precedence in Config::load.
fn state_directory(base: PathBuf) -> PathBuf {
    let current = base.join("piko");
    let legacy = base.join("codex-harness");
    if !current.exists() && legacy.is_dir() {
        legacy
    } else {
        current
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renamed_state_directory_preserves_existing_credentials_and_sessions() {
        let root = tempfile::tempdir().unwrap();
        let piko = root.path().join("piko");
        let legacy = root.path().join("codex-harness");
        assert_eq!(state_directory(root.path().into()), piko);
        std::fs::create_dir(&legacy).unwrap();
        std::fs::write(legacy.join("auth.json"), "existing credentials").unwrap();
        assert_eq!(state_directory(root.path().into()), legacy);
        std::fs::create_dir(&piko).unwrap();
        assert_eq!(state_directory(root.path().into()), piko);
        assert_eq!(
            std::fs::read_to_string(legacy.join("auth.json")).unwrap(),
            "existing credentials"
        );
    }

    #[test]
    fn configured_model_identifiers_have_byte_and_control_bounds() {
        for model in [
            String::new(),
            "   ".into(),
            "bad\nmodel".into(),
            "x".repeat(257),
            "é".repeat(129),
        ] {
            let config = Config {
                model,
                ..Config::default()
            };
            assert!(config.validate().is_err());
        }
        for model in [
            "provider/custom:model".into(),
            "x".repeat(256),
            "é".repeat(128),
        ] {
            Config {
                model,
                ..Config::default()
            }
            .validate()
            .unwrap();
        }
    }

    #[test]
    fn default_state_path_is_absolute_and_valid() {
        let config = Config::default();
        assert!(config.state_dir.is_absolute());
        config.validate().unwrap();
    }
}
