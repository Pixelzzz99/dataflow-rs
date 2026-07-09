pub mod handlers;
pub mod ws;

use crate::state::LogBuffer;
use serde::Serialize;
use std::sync::{Arc, RwLock};
use std::time::Instant;

#[derive(Clone)]
pub struct AppState {
    pub inner: Arc<RwLock<AppStateInner>>,
}

#[derive(Debug)]
pub struct AppStateInner {
    pub status: PipelineStatus,
    pub started_at: Instant,
    pub total_rows: u64,
    pub total_errors: u64,
    pub logs: LogBuffer,
    pub config_path: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PipelineStatus {
    Running,
    Idle,
    Error(String),
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub status: PipelineStatus,
    pub uptime_secs: u64,
    pub total_rows: u64,
    pub total_errors: u64,
    pub config_path: String,
}

impl AppState {
    pub fn new(config_path: String) -> Self {
        Self {
            inner: Arc::new(RwLock::new(AppStateInner {
                status: PipelineStatus::Idle,
                started_at: Instant::now(),
                total_rows: 0,
                total_errors: 0,
                logs: LogBuffer::new(100),
                config_path,
            })),
        }
    }

    pub fn log(&self, line: String) {
        if let Ok(mut inner) = self.inner.write() {
            inner.logs.push(line);
        }
    }

    pub fn set_status(&self, status: PipelineStatus) {
        if let Ok(mut inner) = self.inner.write() {
            inner.status = status;
        }
    }

    pub fn restore_stats(&self, total_rows: u64, total_errors: u64) {
        if let Ok(mut inner) = self.inner.write() {
            inner.total_rows = total_rows;
            inner.total_errors = total_errors;
        }
    }

    pub fn add_rows(&self, count: u64) {
        if let Ok(mut inner) = self.inner.write() {
            inner.total_rows += count;
        }
    }

    pub fn add_error(&self) {
        if let Ok(mut inner) = self.inner.write() {
            inner.total_errors += 1;
        }
    }

    pub fn get_status_response(&self) -> StatusResponse {
        let inner = self.inner.read().unwrap();
        StatusResponse {
            status: inner.status.clone(),
            uptime_secs: inner.started_at.elapsed().as_secs(),
            total_rows: inner.total_rows,
            total_errors: inner.total_errors,
            config_path: inner.config_path.clone(),
        }
    }

    pub fn get_logs(&self) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        inner.logs.get_all()
    }
}

pub async fn start_server(state: AppState, port: u16) -> Result<(), std::io::Error> {
    use axum::{Router, routing::get};
    use tower_http::cors::CorsLayer;

    let app = Router::new()
        .route("/", get(handlers::dashboard))
        .route("/api/status", get(handlers::status))
        .route("/api/logs", get(handlers::logs))
        .route("/ws/logs", get(ws::ws_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            log::error!(
                "Port {} is already in use (another process is listening). \
                 Web UI will not start. Try a different port: \
                 cargo run -- <config> <state> <port>",
                port
            );
            return Err(e);
        }
        Err(e) => return Err(e),
    };

    log::info!("Web UI running at http://localhost:{}", port);
    axum::serve(listener, app).await
}
