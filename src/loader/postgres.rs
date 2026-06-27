use async_trait::async_trait;
use sqlx::PgPool;
use crate::types::{Row, Value};
use crate::error::EtlError;
use super::Loader;

pub struct PostgresLoader {
    pool: PgPool,
    table: String,
}

impl PostgresLoader {
    pub fn new(pool: PgPool, table: String) -> Self {
        Self { pool, table }
    }

    pub async fn connect(connection_string: &str, table: String) -> Result<Self, EtlError> {
        let pool = PgPool::connect(connection_string).await?;
        Ok(Self::new(pool, table))
    }
}

#[async_trait]
impl Loader for PostgresLoader {
    async fn load(&self, rows: Vec<Row>) -> Result<(), EtlError> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut tx: sqlx::Transaction<'_, sqlx::Postgres> = self.pool.begin().await?;

        for row in &rows{
            if row.is_empty() {
                continue;
            }

            let columns: Vec<String> = row.keys().cloned().collect();
            let col_list = columns.join(", ");
            
            let placeholders: Vec<String> = (1..=columns.len())
                .map(|i| format!("${}", i))
                .collect();

            let ph_list = placeholders.join(", ");

            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING",
                self.table, col_list, ph_list
            );

            let mut query = sqlx::query(&sql);
            for col in &columns {
                query = match row.get(col){
                    Some(Value::Int(v)) => query.bind(v),
                    Some(Value::Float(v)) => query.bind(v),
                    Some(Value::Text(v)) => query.bind(v),
                    Some(Value::Bool(v)) => query.bind(v),
                    _ => query.bind(Option::<String>::None), //NULL
                };
            }

            query.execute(&mut *tx).await
                .map_err(|e| EtlError::LoadError(e.to_string()))?;

        }

        tx.commit().await.map_err(|e| EtlError::LoadError(e.to_string()))?;

        Ok(())
    }
}
