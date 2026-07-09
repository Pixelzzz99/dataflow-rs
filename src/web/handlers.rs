use axum::{
    extract::State,
    response::{Html, Json},
};

use super::AppState;

pub async fn dashboard() -> Html<&'static str> {
    Html(include_str!("dashboard.html"))
}

pub async fn status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let response = state.get_status_response();
    Json(serde_json::json!(
            {
                "status": response.status,
                "uptime_secs": response.uptime_secs,
                "total_rows": response.total_rows,
                "total_errors": response.total_errors,
                "config_path": response.config_path,
            }
    ))
}

pub async fn logs(State(state): State<AppState>) -> Json<serde_json::Value> {
    let logs = state.get_logs();
    Json(serde_json::json!({"logs": logs}))
}
