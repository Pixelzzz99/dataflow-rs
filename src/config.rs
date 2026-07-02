use crate::error::EtlError;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    pub source: SourceConfig,
    pub transforms: Vec<TransformConfig>,
    pub destination: DestinationConfig,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    Postgres {
        connection_string: String,
        query: String,
        poll_interval_secs: u64,
    },
    Csv {
        watch_dir: String,
        processed_dir: String,
        #[serde(default = "default_delimiter")]
        delimiter: char,
        #[serde(default = "default_chunk_size")]
        chunk_size: usize,
        poll_interval_secs: u64,
    },
    #[serde(rename = "clickhouse")]
    ClickHouse {
        host: String,
        database: String,
        query: String,
        #[serde(default)]
        username: String,
        #[serde(default)]
        password: String,
        #[serde(default = "default_chunk_size")]
        chunk_size: usize,
        poll_interval_secs: u64,
    },
}

fn default_delimiter() -> char {
    ','
}
fn default_chunk_size() -> usize {
    10_000
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransformConfig {
    Filter { column: String, value: String },
    Map { rename: HashMap<String, String> },
    Aggregate { group_by: String, sum: String },
}

#[derive(Debug, Deserialize)]
pub struct DestinationConfig {
    #[serde(rename = "type")]
    pub dest_type: String,
    pub connection_string: String,
    pub table: String,
    pub unique_key: Option<String>,
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
    fn test_load_postgres_config() {
        let config = load_config("config/pipeline.json").expect("Failed to load config");

        match &config.source {
            SourceConfig::Postgres {
                poll_interval_secs, ..
            } => {
                assert_eq!(*poll_interval_secs, 5);
            }
            _ => panic!("Expected Postgres source"),
        }

        assert_eq!(config.transforms.len(), 3);
        assert_eq!(config.destination.table, "orders_summary");
    }

    #[test]
    fn test_load_csv_config() {
        let config = load_config("config/pipeline_csv.json").expect("Failed to load CSV config");

        match &config.source {
            SourceConfig::Csv {
                watch_dir,
                chunk_size,
                ..
            } => {
                assert_eq!(watch_dir, "data/watched");
                assert_eq!(*chunk_size, 10000);
            }
            _ => panic!("Expected CSV source"),
        }

        assert_eq!(
            config.destination.unique_key,
            Some("payment_id".to_string())
        );
    }

    #[test]
    fn test_load_clickhouse_config() {
        let config = load_config("config/pipeline_clickhouse.json")
            .expect("Failed to load ClickHouse config");

        match &config.source {
            SourceConfig::ClickHouse {
                host,
                database,
                poll_interval_secs,
                ..
            } => {
                assert_eq!(host, "http://localhost:8123");
                assert_eq!(database, "default");
                assert_eq!(*poll_interval_secs, 30);
            }
            _ => panic!("Expected ClickHouse source"),
        }
    }

    #[test]
    fn test_invalid_config() {
        let result = load_config("non_existent_file.json");
        assert!(result.is_err());
    }
}
