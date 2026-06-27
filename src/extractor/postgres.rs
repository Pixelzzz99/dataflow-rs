use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row as SqlxRow, Column};
use crate::types::{Row, Value};
use crate::error::EtlError;
use super::Extractor;


pub struct PostgresExtractor {
    pool: PgPool,
    query: String,
}

impl PostgresExtractor {
    pub fn new(pool: PgPool, query: String) -> Self {
        Self { pool, query }
    }

    pub async fn connect(connection_string: &str, query: String) -> Result<Self, EtlError> {
        let pool = PgPool::connect(connection_string).await?;
        Ok(Self::new(pool, query))
    }
}

#[async_trait]
impl Extractor for PostgresExtractor {
    async fn extract(&self, last_run: DateTime<Utc>) -> Result<Vec<Row>, EtlError> {
        let sqlx_rows = sqlx::query(&self.query)
            .bind(last_run)
            .fetch_all(&self.pool)
            .await?;

        let rows = sqlx_rows
            .iter()
            .map(|sqlx_row| convert_row(sqlx_row))
            .collect();


        Ok(rows)
    }
}

fn convert_row(sqlx_row: &sqlx::postgres::PgRow) -> Row{
    let mut row = std::collections::HashMap::new();

    for (i, column) in sqlx_row.columns().iter().enumerate() {
        let col_name = column.name().to_string();

        let value = if let Ok(v) = sqlx_row.try_get::<i64, _>(i) {
            Value::Int(v)
        } else if let Ok(v) = sqlx_row.try_get::<f64, _>(i) {
            Value::Float(v)
        } else if let Ok(v) = sqlx_row.try_get::<bool, _>(i) {
            Value::Bool(v)
        } else if let Ok(v) = sqlx_row.try_get::<String, _>(i) {
            Value::Text(v)
        } else {
            Value::Null
        };

        row.insert(col_name, value);
    }
    row
}
