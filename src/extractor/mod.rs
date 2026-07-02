use crate::error::EtlError;
use crate::types::Row;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

pub mod clickhouse;
pub mod csv;
pub mod postgres;

#[async_trait]
pub trait Extractor: Send + Sync {
    async fn extract(&self, last_run: DateTime<Utc>) -> Result<Vec<Row>, EtlError>;
}
