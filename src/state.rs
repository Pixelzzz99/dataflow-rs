use crate::error::EtlError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::collections::VecDeque;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PersistentState {
    pub last_run: DateTime<Utc>,
    pub processed_files: HashSet<String>,
    pub total_rows_processed: u64,
    pub total_errors: u64,
}

impl PersistentState {
    pub fn new() -> Self {
        Self {
            last_run: Utc::now() - chrono::Duration::days(1),
            processed_files: HashSet::new(),
            total_rows_processed: 0,
            total_errors: 0,
        }
    }

    pub fn load(path: &str) -> Self {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_else(Self::new)
    }

    pub fn save(&self, path: &str) -> Result<(), EtlError> {
        let json =
            serde_json::to_string_pretty(self).map_err(|e| EtlError::ConfigError(e.to_string()))?;
        std::fs::write(path, json).map_err(|e| {
            EtlError::ConfigError(format!("Cannot write state file {}: {}", path, e))
        })?;

        Ok(())
    }

    pub fn mark_file_processed(&mut self, filename: &str) {
        self.processed_files.insert(filename.to_string());
    }

    pub fn is_file_processed(&self, filename: &str) -> bool {
        self.processed_files.contains(filename)
    }
}

#[derive(Debug, Clone)]
pub struct LogBuffer {
    logs: VecDeque<String>,
    capacity: usize,
}

impl LogBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            logs: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, line: String) {
        if self.logs.len() >= self.capacity {
            self.logs.pop_front();
        }
        self.logs.push_back(line);
    }

    pub fn get_all(&self) -> Vec<String> {
        self.logs.iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_state() {
        let state = PersistentState::new();
        assert_eq!(state.total_rows_processed, 0);
        assert!(state.processed_files.is_empty());
    }

    #[test]
    fn test_mark_and_check_file() {
        let mut state = PersistentState::new();
        assert!(!state.is_file_processed("2026-01.csv"));

        state.mark_file_processed("2026-01.csv");
        assert!(state.is_file_processed("2026-01.csv"));
        assert!(!state.is_file_processed("2026-02.csv"));
    }

    #[test]
    fn test_save_and_load() {
        let mut state = PersistentState::new();
        state.mark_file_processed("test.csv");
        state.total_rows_processed = 42;

        let tmp = "/tmp/etl_test_state.json";
        state.save(tmp).unwrap();

        let loaded = PersistentState::load(tmp);
        assert!(loaded.is_file_processed("test.csv"));
        assert_eq!(loaded.total_rows_processed, 42);

        std::fs::remove_file(tmp).ok();
    }

    #[test]
    fn test_load_missing_file() {
        let state = PersistentState::load("/tmp/non_existent__etl_state.json");
        assert_eq!(state.total_rows_processed, 0);
    }

    #[test]
    fn test_log_buffer_capacity() {
        let mut buf = LogBuffer::new(3);
        buf.push("line1".to_string());
        buf.push("line2".to_string());
        buf.push("line3".to_string());
        buf.push("line4".to_string());
        buf.push("line5".to_string());

        let logs = buf.get_all();
        assert_eq!(logs.len(), 3);
        assert_eq!(logs[0], "line3");
        assert_eq!(logs[2], "line5");
    }
}
