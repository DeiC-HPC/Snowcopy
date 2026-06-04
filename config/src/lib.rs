use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use directories::ProjectDirs;
use anyhow::{Result, Context};
use std::fs;

pub mod tui;

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct AppConfig {
    pub global: GlobalConfig,
    pub endpoints: Vec<Endpoint>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GlobalConfig {
    #[serde(default = "default_region")]
    pub s3_region: String,
    #[serde(flatten)]
    pub additional_properties: HashMap<String, serde_json::Value>,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            s3_region: default_region(),
            additional_properties: HashMap::new(),
        }
    }
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_region_option() -> Option<String> {
    Some(default_region())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Endpoint {
    pub name: String,
    pub config: Option<EndpointConfig>,
    pub settings: Option<HashMap<String, serde_json::Value>>,
}

impl Endpoint {
    pub fn is_complete(&self) -> bool {
        if self.name.trim().is_empty() {
            return false;
        }
        if let Some(config) = &self.config {
            !config.s3_access_key.trim().is_empty()
                && !config.s3_secret_key.trim().is_empty()
                && !config.s3_url.trim().is_empty()
        } else {
            false
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EndpointConfig {
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_url: String,
    #[serde(default = "default_region_option")]
    pub s3_region: Option<String>,
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_url: String::new(),
            s3_region: default_region_option(),
        }
    }
}

pub fn get_config_path() -> Result<PathBuf> {
    let proj_dirs = ProjectDirs::from("", "", "snowcopy")
        .context("Could not determine config directory")?;
    let config_dir = proj_dirs.config_dir();
    Ok(config_dir.join("config.json"))
}

pub fn load_config() -> Result<AppConfig> {
    let path = get_config_path()?;
    if !path.exists() {
        return Ok(AppConfig::default());
    }
    let content = fs::read_to_string(path)?;
    let config = serde_json::from_str(&content)?;
    Ok(config)
}

pub fn save_config(config: &AppConfig) -> Result<()> {
    let path = get_config_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let content = serde_json::to_string_pretty(config)?;
    fs::write(path, content)?;
    Ok(())
}

pub fn run_tui() -> Result<()> {
    tui::run()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endpoint_is_complete() {
        let mut endpoint = Endpoint {
            name: "test".to_string(),
            config: Some(EndpointConfig {
                s3_access_key: "key".to_string(),
                s3_secret_key: "secret".to_string(),
                s3_url: "url".to_string(),
                s3_region: None,
            }),
            settings: None,
        };
        assert!(endpoint.is_complete());

        endpoint.name = " ".to_string();
        assert!(!endpoint.is_complete());

        endpoint.name = "test".to_string();
        endpoint.config.as_mut().unwrap().s3_access_key = "".to_string();
        assert!(!endpoint.is_complete());

        endpoint.config = None;
        assert!(!endpoint.is_complete());
    }

    #[test]
    fn test_tui_data_leak_fix() {
        use crate::tui::{App, Field, Focus, InputMode};
        let config = AppConfig {
            global: GlobalConfig::default(),
            endpoints: vec![
                Endpoint {
                    name: "Endpoint A".to_string(),
                    config: Some(EndpointConfig {
                        s3_access_key: "key A".to_string(),
                        s3_secret_key: "secret A".to_string(),
                        s3_url: "url A".to_string(),
                        s3_region: Some("region A".to_string()),
                    }),
                    settings: None,
                },
                Endpoint {
                    name: "Endpoint B".to_string(),
                    config: Some(EndpointConfig {
                        s3_access_key: "key B".to_string(),
                        s3_secret_key: "secret B".to_string(),
                        s3_url: "url B".to_string(),
                        s3_region: Some("region B".to_string()),
                    }),
                    settings: None,
                },
            ],
        };

        let mut app = App::new(config);
        
        // 1. Select Endpoint A and Region field
        app.list_state.select(Some(0));
        app.selected_field = Field::Region;
        
        // 2. Start editing Region of A
        app.start_editing();
        app.edit_buffer = "new region A".to_string();
        
        // 4. Press Left in Editing mode (should save)
        app.save_edit().unwrap();
        app.focus = Focus::Endpoints;
        app.input_mode = InputMode::Normal;
        
        assert_eq!(app.config.endpoints[0].config.as_ref().unwrap().s3_region, Some("new region A".to_string()));
        
        // 5. Navigate in Normal mode (should NOT call save_edit automatically)
        app.next_endpoint(); // now at B
        // Simulating what was there before: another key press that used to call save_edit
        // In the fixed code, we don't call save_edit on KeyCode::Down in Normal mode.
        // If we did call it (accidentally), it would leak.
        
        // Verify that B is still B
        assert_eq!(
            app.config.endpoints[1].config.as_ref().unwrap().s3_region, 
            Some("region B".to_string()),
            "Endpoint B region should not be overwritten by navigation in Normal mode"
        );
    }
}
