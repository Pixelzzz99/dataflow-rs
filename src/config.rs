use serde::Deserialize;
use std::collections::HashMap;
use crate::error::EtlError;

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    pub source: SourceConfig,
    pub transforms: Vec<TransformConfig>,
    pub destination: DestinationConfig,
}

#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    #[serde(rename = "type")]
    pub source_type: String,
    pub connection_string: String,
    pub query: String,
    pub poll_interval_secs: u64,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransformConfig {
    Filter {
        column: String,
        value: String,
    },
    Map {
        rename: HashMap<String, String>,
    },
    Aggregate {
        group_by: Vec<String>,
        sum: String,
    }
}

#[derive(Debug, Deserialize)]
pub struct DestinationConfig {
    #[serde(rename = "type")]
    pub dest_type: String,
    pub connection_string: String,
    pub table: String,
}

pub fn load_config(path: &str) -> Result<PipelineConfig, EtlError> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| EtlError::ConfigError(format!("Cannot read file {}: {}", path, e)))?;


    let config: PipelineConfig = serde_json::from_str(&content)?;

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_load_config(){ 
        let config = load_config("config/pipeline.json")
            .expect("Failed to load config");

        assert_eq!(config.source.source_type, "postgres");
        assert_eq!(config.source.poll_interval_secs, 5);
        assert_eq!(config.transforms.len(), 3);
        assert_eq!(config.destination.table, "orders_summary");
    }

    #[test]
    fn test_transform_config_variants(){
        let config = load_config("config/pipeline.json").unwrap();

        match &config.transforms[0] {
            TransformConfig::Filter { column, value } => {
                assert_eq!(column, "status");
                assert_eq!(value, "active");
            },
            _ => panic!("Expected first transform to be Filter"),
        }

        match &config.transforms[1] {
            TransformConfig::Map { rename } => {
                assert_eq!(rename.get("user_id"), Some(&"client_id".to_string()));
                assert_eq!(rename.get("amount"), Some(&"total_amount".to_string()));
            },
            _ => panic!("Expected second transform to be Map"),
        }
    }

    #[test]
    fn test_invalid_config(){
        let result = load_config("non_existent_file.json");
        assert!(result.is_err());
    }
}
