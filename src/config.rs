use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ClickhouseConfig {
    pub url: String,
    pub user: String,
    pub password: String,
    pub database: String,
}

#[derive(Debug, Deserialize)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub addr: String,
}

#[derive(Debug, Deserialize)]
pub struct WebsocketConfig {
    pub enabled: bool,
    pub addr: String,
}

impl Default for WebsocketConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            addr: "0.0.0.0:8000".to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct IndexerConfig {
    pub checkpoints_dir: String,
    pub data_dir: String,
    pub storage: ClickhouseConfig,
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub websocket: WebsocketConfig,
}

pub fn load_config(config_path: PathBuf) -> Result<IndexerConfig, config::ConfigError> {
    config::Config::builder()
        .add_source(config::File::with_name(config_path.to_str().unwrap()))
        .build()?
        .try_deserialize()
}
