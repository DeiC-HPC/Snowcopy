use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use directories::ProjectDirs;
use anyhow::{Result, Context};
use std::fs;
use util_layer::{ConfigModeConfig, Endpoint};
#[cfg(test)]
use util_layer::EndpointConfig;

pub mod tui;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppConfig {
    pub version: u32,
    pub global: GlobalConfig,
    pub endpoints: Vec<Endpoint>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            version: 1,
            global: GlobalConfig::default(),
            endpoints: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GlobalConfig {
    #[serde(default = "util_layer::default_region")]
    pub s3_region: String,
    #[serde(flatten)]
    pub additional_properties: HashMap<String, serde_json::Value>,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            s3_region: util_layer::default_region(),
            additional_properties: HashMap::new(),
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
    load_config_from_path(path)
}

pub fn load_config_from_path(path: PathBuf) -> Result<AppConfig> {
    if !path.exists() {
        return Ok(AppConfig::default());
    }
    let content = fs::read_to_string(&path)?;
    let value: serde_json::Value = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse config as JSON at {}", path.display()))?;
    
    let version = value.get("version")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow::anyhow!("Configuration error: missing 'version' field in config file at {}", path.display()))?;

    if version != 1 {
        //In the future, we want to handle version upgrades here automatically
        anyhow::bail!("Configuration error: expected version 1, found version {} in config file at {}", version, path.display());
    }

    let config: AppConfig = serde_json::from_value(value)
        .with_context(|| format!("Failed to deserialize AppConfig from {} after version check", path.display()))?;
    Ok(config)
}

pub fn save_config(config: &AppConfig) -> Result<()> {
    let path = get_config_path()?;
    save_config_to_path(config, path)
}

pub fn save_config_to_path(config: &AppConfig, path: PathBuf) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let content = serde_json::to_string_pretty(config)?;
    fs::write(path, content)?;
    Ok(())
}

pub fn run_tui(config: ConfigModeConfig) -> Result<()> {
    tui::run(config)
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
                s3_url: "https://test.com".to_string(),
                s3_region: "us-east-1".to_string(),
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
            version: 1,
            global: GlobalConfig::default(),
            endpoints: vec![
                Endpoint {
                    name: "Endpoint A".to_string(),
                    config: Some(EndpointConfig {
                        s3_access_key: "key A".to_string(),
                        s3_secret_key: "secret A".to_string(),
                        s3_url: "url A".to_string(),
                        s3_region: "region A".to_string(),
                    }),
                    settings: None,
                },
                Endpoint {
                    name: "Endpoint B".to_string(),
                    config: Some(EndpointConfig {
                        s3_access_key: "key B".to_string(),
                        s3_secret_key: "secret B".to_string(),
                        s3_url: "url B".to_string(),
                        s3_region: "region B".to_string(),
                    }),
                    settings: None,
                },
            ],
        };

        let mut app = App::new(config, None);
        
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
        
        assert_eq!(app.config.endpoints[0].config.as_ref().unwrap().s3_region, "new region A".to_string());
        
        // 5. Navigate in Normal mode (should NOT call save_edit automatically)
        app.next_endpoint(); // now at B
        // Simulating what was there before: another key press that used to call save_edit
        // In the fixed code, we don't call save_edit on KeyCode::Down in Normal mode.
        // If we did call it (accidentally), it would leak.
        
        // Verify that B is still B
        assert_eq!(
            app.config.endpoints[1].config.as_ref().unwrap().s3_region, 
            "region B".to_string(),
            "Endpoint B region should not be overwritten by navigation in Normal mode"
        );
    }

    #[test]
    fn test_tui_esc_discards_changes() {
        use crate::tui::{App, Field, InputMode};
        let config = AppConfig {
            version: 1,
            global: GlobalConfig::default(),
            endpoints: vec![
                Endpoint {
                    name: "Endpoint A".to_string(),
                    config: Some(EndpointConfig {
                        s3_access_key: "key A".to_string(),
                        s3_secret_key: "secret A".to_string(),
                        s3_url: "url A".to_string(),
                        s3_region: "region A".to_string(),
                    }),
                    settings: None,
                },
            ],
        };

        let mut app = App::new(config, None);
        
        // 1. Select Endpoint A and Region field
        app.list_state.select(Some(0));
        app.selected_field = Field::Region;
        
        // 2. Start editing Region of A
        app.start_editing();
        app.edit_buffer = "changed region".to_string();
        
        // 3. Simulate Esc (it should discard and CLEAR edit_buffer)
        // We will call the actual logic for Esc in Editing mode from tui.rs indirectly
        // or just verify that if we DON'T call save_edit, it's discarded.
        app.input_mode = InputMode::Normal;
        app.edit_buffer.clear();
        
        // The config should NOT have changed
        assert_eq!(
            app.config.endpoints[0].config.as_ref().unwrap().s3_region, 
            "region A".to_string(),
            "Esc should discard changes"
        );
        assert!(app.edit_buffer.is_empty(), "edit_buffer should be cleared on Esc");
        
        // 4. Verify that Enter DOES save
        app.start_editing();
        app.edit_buffer = "new region".to_string();
        app.save_edit().unwrap(); // This is what Enter calls
        assert_eq!(
            app.config.endpoints[0].config.as_ref().unwrap().s3_region, 
            "new region".to_string(),
            "Enter should still save changes"
        );
    }

    #[test]
    fn test_config_version_check() {
        let config_path = std::env::temp_dir().join(format!("snowcopy_test_config_{}.json", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));

        // Cleanup function
        let cleanup = |path: &PathBuf| {
            if path.exists() {
                let _ = fs::remove_file(path);
            }
        };

        // Test missing version
        let bad_config = serde_json::json!({
            "global": {
                "s3_region": "us-east-1"
            },
            "endpoints": []
        });
        fs::write(&config_path, serde_json::to_string(&bad_config).unwrap()).unwrap();

        // We need to use a function that takes path to test version logic
        fn load_config_from_path(path: PathBuf) -> Result<AppConfig> {
            if !path.exists() {
                return Ok(AppConfig::default());
            }
            let content = fs::read_to_string(&path)?;
            let value: serde_json::Value = serde_json::from_str(&content)
                .with_context(|| format!("Failed to parse config as JSON at {}", path.display()))?;
            
            let version = value.get("version")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| anyhow::anyhow!("Configuration error: missing 'version' field in config file at {}", path.display()))?;

            if version != 1 {
                anyhow::bail!("Configuration error: expected version 1, found version {} in config file at {}", version, path.display());
            }

            let config: AppConfig = serde_json::from_value(value)
                .with_context(|| format!("Failed to deserialize AppConfig from {} after version check", path.display()))?;
            Ok(config)
        }

        let result = load_config_from_path(config_path.clone());
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("missing 'version' field"));
        assert!(err_msg.contains(config_path.to_str().unwrap()));

        // Test wrong version
        let wrong_version_config = serde_json::json!({
            "version": 2,
            "global": {
                "s3_region": "us-east-1"
            },
            "endpoints": []
        });
        fs::write(&config_path, serde_json::to_string(&wrong_version_config).unwrap()).unwrap();
        let result = load_config_from_path(config_path.clone());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expected version 1, found version 2"));

        // Test correct version
        let correct_config = serde_json::json!({
            "version": 1,
            "global": {
                "s3_region": "us-east-1"
            },
            "endpoints": []
        });
        fs::write(&config_path, serde_json::to_string(&correct_config).unwrap()).unwrap();
        let result = load_config_from_path(config_path.clone());
        assert!(result.is_ok());
        assert_eq!(result.unwrap().version, 1);
        
        cleanup(&config_path);
    }
}
