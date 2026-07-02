use super::Extractor;
use crate::error::EtlError;
use crate::state::PersistentState;
use crate::types::{Row, Value};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub struct ClickHouseExtractor {
    pub host: String,
    pub database: String,
    pub username: String,
    pub password: String,
    pub chunk_size: usize,
}

impl ClickHouseExtractor {
    pub fn new(
        host: String,
        database: String,
        query: String,
        username: String,
        password: String,
        chunk_size: usize,
    ) -> Result<Self, EtlError> {
        // Здесь можно добавить проверку соединения с ClickHouse, если необходимо
        Ok(Self {
            host,
            database,
            username,
            password,
            chunk_size,
        })
    }
}

#[async_trait]
impl Extractor for ClickHouseExtractor {
    async fn extract(&self, _last_run: DateTime<Utc>) -> Result<Vec<Row>, EtlError> {
        Ok(vec![])
    }
}
