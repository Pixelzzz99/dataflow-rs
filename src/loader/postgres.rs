use super::Loader;
use crate::error::EtlError;
use crate::types::{Row, Value};
use async_trait::async_trait;
use sqlx::PgPool;

pub struct PostgresLoader {
    pool: PgPool,
    table: String,

    chunk_size: usize,
    unique_key: Option<String>,
}

impl PostgresLoader {
    pub fn new(pool: PgPool, table: String, chunk_size: usize, unique_key: Option<String>) -> Self {
        Self {
            pool,
            table,
            chunk_size,
            unique_key,
        }
    }

    pub async fn connect(
        connection_string: &str,
        table: String,
        chunk_size: usize,
        unique_key: Option<String>,
    ) -> Result<Self, EtlError> {
        let pool = PgPool::connect(connection_string).await?;
        Ok(Self::new(pool, table, chunk_size, unique_key))
    }

    async fn load_chunk(&self, rows: &[Row]) -> Result<(), EtlError> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut tx: sqlx::Transaction<'_, sqlx::Postgres> = self.pool.begin().await?;

        for row in rows {
            if row.is_empty() {
                continue;
            }

            //Тут мы должны сформировать SQL-запрос для вставки данных в таблицу.
            let columns: Vec<String> = row.keys().cloned().collect();
            let col_list = columns.join(", ");
            let placeholders: Vec<String> =
                (1..=columns.len()).map(|i| format!("${}", i)).collect();
            let ph_list = placeholders.join(", ");

            //Идемпотентность: если unique_key задан, используем ON CONFLICT DO NOTHING
            let conflict_clause = match &self.unique_key {
                Some(key) => format!("ON CONFLICT ({}) DO NOTHING", key),
                None => "ON CONFLICT DO NOTHING".to_string(),
            };

            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({}) {}",
                self.table, col_list, ph_list, conflict_clause
            );

            let mut query = sqlx::query(&sql);
            for col in &columns {
                query = match row.get(col) {
                    Some(Value::Int(v)) => query.bind(v),
                    Some(Value::Float(v)) => query.bind(v),
                    Some(Value::Text(v)) => query.bind(v),
                    Some(Value::Bool(v)) => query.bind(v),
                    _ => query.bind(Option::<String>::None), //NULL
                };
            }

            query
                .execute(&mut *tx)
                .await
                .map_err(|e: sqlx::Error| EtlError::LoadError(e.to_string()))?;
        }

        tx.commit()
            .await
            .map_err(|e: sqlx::Error| EtlError::LoadError(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl Loader for PostgresLoader {
    async fn load(&self, rows: Vec<Row>) -> Result<(), EtlError> {
        if rows.is_empty() {
            return Ok(());
        }

        let total = rows.len();
        let mut loaded = 0usize;

        for chunk in rows.chunks(self.chunk_size) {
            self.load_chunk(chunk).await?;
            loaded += chunk.len();
            log::info!("Loaded {}/{} rows into {}", loaded, total, self.table);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_split_into_chunks() {
        let data: Vec<i32> = (0..7).collect();
        let chunks: Vec<&[i32]> = data.chunks(3).collect();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], &[0, 1, 2]);
        assert_eq!(chunks[2], &[6]);
    }

    #[test]
    fn test_empty_exact_multiple() {
        let data: Vec<i32> = (0..6).collect();
        let chunks: Vec<&[i32]> = data.chunks(3).collect();
        assert_eq!(chunks.len(), 2);
    }

    #[test]
    fn test_chunks_smaller_than_size() {
        let data: Vec<i32> = vec![1, 2];
        let chunks: Vec<&[i32]> = data.chunks(10).collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], &[1, 2]);
    }
}
