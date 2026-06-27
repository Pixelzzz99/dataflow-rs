use std::collections::HashMap;
use crate::types::{Row, Value};
use crate::error::EtlError;
use super::Transformer;

pub struct AggregateTransformer {
    pub group_by: String,
    pub sum: String,
}

impl AggregateTransformer {
    pub fn new(group_by: String, sum: String) -> Self {
        Self { group_by, sum }
    }
}

impl Transformer for AggregateTransformer {
    fn transform(&self, rows: Vec<Row>) -> Result<Vec<Row>, EtlError> {
        let mut sums: HashMap<String, f64> = HashMap::new();

        for row in rows {
            let group_key = match row.get(&self.group_by) {
                Some(Value::Text(v)) => v.clone(),
                Some(Value::Int(v)) => v.to_string(),
                _ => continue,
            };

            let amount = match row.get(&self.sum) {
                Some(Value::Float(v)) => *v,
                Some(Value::Int(v)) => *v as f64,
                _ => 0.0,
            };
            
            *sums.entry(group_key).or_insert(0.0) += amount;
        }


        let result = sums
            .into_iter()
            .map(|(key, total)| {
                let mut row: Row = HashMap::new();
                row.insert(self.group_by.clone(), Value::Text(key));
                row.insert(self.sum.clone(), Value::Float(total));
                row
            })
            .collect();

        Ok(result)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate ::types::{Value, make_row};

    #[test]
    fn test_aggregate_sums() {
        let agg = AggregateTransformer::new(
            "client_id".to_string(),
            "total_amount".to_string(),
        );

        let rows = vec![
            make_row(vec![
                ("client_id", Value::Text("A".to_string())),
                ("total_amount", Value::Float(100.0)),
            ]),
            make_row(vec![
                ("client_id", Value::Text("B".to_string())),
                ("total_amount", Value::Float(200.0)),
            ]),
            make_row(vec![
                ("client_id", Value::Text("A".to_string())),
                ("total_amount", Value::Float(150.0)),
            ]),
        ];

        let mut result = agg.transform(rows).unwrap();

        result.sort_by_key(|row| match row.get("client_id") {
            Some(Value::Text(v)) => v.clone(),
            _ => String::new(),
        });

        assert_eq!(result.len(), 2);

        assert_eq!(result[0].get("total_amount"), Some(&Value::Float(250.0)));
        assert_eq!(result[1].get("total_amount"), Some(&Value::Float(200.0)));
    }

}
