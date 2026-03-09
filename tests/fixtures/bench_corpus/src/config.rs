use std::collections::HashMap;
use std::path::Path;

pub struct ConfigManager {
    settings: HashMap<String, String>,
    config_path: String,
}

impl ConfigManager {
    pub fn new(path: &str) -> Self {
        ConfigManager {
            settings: HashMap::new(),
            config_path: path.to_string(),
        }
    }

    pub fn parse(&mut self, content: &str) -> Result<(), ConfigError> {
        for line in content.lines() {
            if let Some((key, val)) = line.split_once('=') {
                self.settings.insert(key.trim().to_string(), val.trim().to_string());
            }
        }
        Ok(())
    }

    pub fn reload(&mut self) -> Result<(), ConfigError> {
        self.settings.clear();
        Ok(())
    }

    pub fn get_setting(&self, key: &str) -> Option<&str> {
        self.settings.get(key).map(|s| s.as_str())
    }

    pub fn validate_schema(&self) -> Result<(), ConfigError> {
        if self.settings.is_empty() {
            return Err(ConfigError::EmptyConfig);
        }
        Ok(())
    }
}

pub enum ConfigError {
    ParseFailed,
    EmptyConfig,
    MissingRequired(String),
}

pub struct ConfigValidator {
    required_keys: Vec<String>,
}

impl ConfigValidator {
    pub fn validate_against(&self, manager: &ConfigManager) -> Result<(), ConfigError> {
        for key in &self.required_keys {
            if manager.get_setting(key).is_none() {
                return Err(ConfigError::MissingRequired(key.clone()));
            }
        }
        Ok(())
    }
}
