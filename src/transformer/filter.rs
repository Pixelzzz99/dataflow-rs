use crate::types::{Row, Value};
use crate::error::EtlError;
use super::Transformer;

pub struct FilterTransformer {
    pub column: String,
    pub value: String,
}

impl FilterTransformer {
    pub fn new(column: String, value: String) -> Self {
        Self { column, value }
    }
}

impl Transformer for FilterTransformer {
    fn transform(&self, mut rows: Vec<Row>) -> Result<Vec<Row>, EtlError> {
        rows.retain(|row| {
            match row.get(&self.column) {
                Some(Value::Text(v)) => v == &self.value,
                _ => false,
            }
        });

        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Value, make_row};

    fn sample_rows() -> Vec<Row> {
        vec![
            make_row(vec![
                ("id", Value::Int(1)),
                ("status", Value::Text("active".to_string())),
            ]),
            make_row(vec![
                ("id", Value::Int(2)),
                ("status", Value::Text("inactive".to_string())),
            ]),
            make_row(vec![
                ("id", Value::Int(3)),
                ("status", Value::Text("active".to_string())),
            ])
        ]
    }

    /*
     * Тест для проверки работы фильтра, который оставляет только строки с определенным значением в
     * указанной колонке.
     */
    #[test]
    fn test_filter_keeps_matching_rows() {
        let filter = FilterTransformer::new("status".to_string(), "active".to_string());
        let result = filter.transform(sample_rows()).unwrap();
        assert_eq!(result.len(), 2);
        for row in result {
            assert_eq!(row.get("status"), Some(&Value::Text("active".to_string())));
        }
    }

    #[test]
    fn test_filter_empty_result(){
        let filter = FilterTransformer::new("status".to_string(), "deleted".to_string());
        let result = filter.transform(sample_rows()).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_filter_missing_column(){
        let filter = FilterTransformer::new("nenexistent".to_string(), "active".to_string());
        let result = filter.transform(sample_rows()).unwrap();
        assert_eq!(result.len(), 0);
    }
}
