//! Settings from `config.json`. Loading them touches no network or database,
//! so commands that only read gzb files work while the tool DB is down.

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Where `config.json` is looked for, in order.
const CONFIG_PATHS: &[&str] = &[
    "config.json",
    "/data/project/glamtools/baglama2_rs/config.json",
];

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Pool settings for the tool DB (`url`, `max_connections`, …), passed to
    /// `ToolforgeDB::add_mysql_pool`.
    #[serde(default)]
    pub tooldb: Value,
    /// Pool settings for the Commons core replica.
    #[serde(default)]
    pub commons: Value,
    /// Pool settings for the Commons links cluster (`x4`).
    #[serde(default)]
    pub commons_links: Value,
    /// Root directory of the gzb view-data files, where the PHP API reads
    /// them (`viewdata/gzb` on Toolforge).
    pub gzb_data_root_path: PathBuf,
    /// Seconds to wait between query retries.
    #[serde(default = "default_hold_on")]
    hold_on: u64,
}

fn default_hold_on() -> u64 {
    5
}

impl Config {
    /// The first `config.json` of [`CONFIG_PATHS`] that exists.
    pub fn load() -> Result<Self> {
        let path = CONFIG_PATHS
            .iter()
            .map(Path::new)
            .find(|path| path.is_file())
            .ok_or_else(|| anyhow!("no config.json found (looked for {CONFIG_PATHS:?})"))?;
        Self::from_file(path)
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path).with_context(|| path.display().to_string())?;
        serde_json::from_reader(file).with_context(|| format!("bad {}", path.display()))
    }

    pub fn hold_on(&self) -> Duration {
        Duration::from_secs(self.hold_on)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_json() {
        let config: Config = serde_json::from_value(serde_json::json!({
            "_comment": "ignored",
            "tooldb": {"url": "mysql://u:p@h/db"},
            "gzb_data_root_path": "/x/gzb"
        }))
        .unwrap();
        assert_eq!(config.gzb_data_root_path, PathBuf::from("/x/gzb"));
        assert_eq!(config.tooldb["url"], "mysql://u:p@h/db");
        assert!(config.commons.is_null());
        assert_eq!(config.hold_on(), Duration::from_secs(5));
    }

    #[test]
    fn test_gzb_root_required() {
        let err = serde_json::from_value::<Config>(serde_json::json!({"tooldb": {}}))
            .unwrap_err()
            .to_string();
        assert!(err.contains("gzb_data_root_path"), "{err}");
    }
}
