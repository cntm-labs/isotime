use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub engine: EngineSettings,
    pub dashboard: DashboardSettings,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EngineSettings {
    pub wal_path: String,
    pub cas_root: String,
    pub encryption_key: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DashboardSettings {
    pub enabled: bool,
    pub addr: String,
    pub admin_token: String,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let s = Config::builder()
            // Start with some internal defaults
            .set_default("engine.wal_path", "isotime.wal")?
            .set_default("engine.cas_root", "cas_store")?
            .set_default("dashboard.enabled", true)?
            .set_default("dashboard.addr", "127.0.0.1:9000")?
            .set_default("dashboard.admin_token", "isotime_default_secret")?
            // Layer on the config file
            .add_source(File::with_name("config").required(false))
            // Layer on environment variables (ISOTIME_DASHBOARD__ADMIN_TOKEN)
            .add_source(Environment::with_prefix("ISOTIME").separator("__"))
            .build()?;
        
        s.try_deserialize()
    }
}
