use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crate::types::Row;
use crate::error::EtlError;


pub mod postgres;

#[async_trait]
pub trait Extractor: Send + Sync {
    async fn extract(&self, last_run: DateTime<Utc>) -> Result<Vec<Row>, EtlError>;
}
