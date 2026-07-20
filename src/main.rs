mod config;
mod error;
mod extractor;
mod loader;
mod pipeline;
mod retry;
mod state;
mod transformer;
mod types;
mod web;

use std::sync::{Arc, Mutex};
use tokio::time::{Duration, sleep};

use config::{PipelineConfig, SourceConfig, TransformConfig, load_config};
use extractor::Extractor;
use extractor::clickhouse::ClickHouseExtractor;
use extractor::csv::CsvExtractor;
use extractor::postgres::PostgresExtractor;
use loader::postgres::PostgresLoader;
use pipeline::{Pipeline, PipelineState};
use retry::retry_with_backoff;
use state::PersistentState;
use transformer::{
    Transformer, aggregator::AggregateTransformer, filter::FilterTransformer,
    mapper::MapTransformer,
};
use web::{AppState, PipelineStatus, start_server};

// ---------------------------------------------------------------------------
// Entry point — high-level flow only
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    env_logger::init();

    // 1. CLI args: <config> <state> <port>
    let args = parse_args();
    log::info!("Loading config from: {}", args.config_path);
    log::info!("Loading state from {}", args.state_path);
    log::info!("Web UI: http://localhost:{}", args.web_port);

    // 2. Config + persisted state
    let config = load_config(&args.config_path).unwrap_or_else(|e| {
        log::error!("Failed to load config: {}", e);
        std::process::exit(1);
    });

    let app_state = AppState::new(args.config_path.clone());
    let persistent_state = Arc::new(Mutex::new(PersistentState::load(&args.state_path)));
    restore_app_stats(&app_state, &persistent_state);

    // 3. Build ETL pipeline (extract → transform → load)
    let (poll_interval, extractor) =
        build_extractor(&config, &persistent_state, &args.state_path).await;
    let transformers = build_transformers(&config);
    let loader = build_loader(&config).await;
    let pipeline = Pipeline::new(extractor, transformers, Box::new(loader));
    let pipeline_state = pipeline_state_from(&persistent_state);

    // 4. Start Web UI in background
    spawn_web_server(app_state.clone(), args.web_port);

    // 5. Poll forever
    log::info!("ETL Engine started. Polling every {}s...", poll_interval);
    run_loop(
        &pipeline,
        &pipeline_state,
        &persistent_state,
        &app_state,
        &args.state_path,
        poll_interval,
    )
    .await;
}

// ---------------------------------------------------------------------------
// Args
// ---------------------------------------------------------------------------

struct Args {
    config_path: String,
    state_path: String,
    web_port: u16,
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    Args {
        config_path: args
            .get(1)
            .cloned()
            .unwrap_or_else(|| "config/pipeline.json".into()),
        state_path: args
            .get(2)
            .cloned()
            .unwrap_or_else(|| "etl_state.json".into()),
        web_port: args
            .get(3)
            .and_then(|p| p.parse().ok())
            .unwrap_or(3000),
    }
}

// ---------------------------------------------------------------------------
// State helpers
// ---------------------------------------------------------------------------

fn restore_app_stats(app_state: &AppState, persistent_state: &Arc<Mutex<PersistentState>>) {
    let ps = persistent_state.lock().unwrap();
    log::info!(
        "Loaded state: {} files already processed, last_run: {}",
        ps.processed_files.len(),
        ps.last_run
    );
    app_state.restore_stats(ps.total_rows_processed, ps.total_errors);
}

fn pipeline_state_from(persistent_state: &Arc<Mutex<PersistentState>>) -> Arc<Mutex<PipelineState>> {
    let ps = persistent_state.lock().unwrap();
    Arc::new(Mutex::new(PipelineState {
        last_run: ps.last_run,
        rows_processed: ps.total_rows_processed,
        errors_count: ps.total_errors,
    }))
}

fn save_state(persistent_state: &Arc<Mutex<PersistentState>>, path: &str) {
    let snapshot = persistent_state.lock().unwrap().clone();
    if let Err(e) = snapshot.save(path) {
        log::warn!("Failed to save state: {}", e);
    }
}

// ---------------------------------------------------------------------------
// Pipeline builders
// ---------------------------------------------------------------------------

async fn build_extractor(
    config: &PipelineConfig,
    persistent_state: &Arc<Mutex<PersistentState>>,
    state_path: &str,
) -> (u64, Box<dyn Extractor>) {
    match &config.source {
        SourceConfig::Postgres {
            connection_string,
            query,
            poll_interval_secs,
        } => {
            log::info!("Source: PostgreSQL");
            let e = PostgresExtractor::connect(connection_string, query.clone())
                .await
                .unwrap_or_else(|e| {
                    log::error!("Failed to connect to source: {}", e);
                    std::process::exit(1);
                });
            (*poll_interval_secs, Box::new(e))
        }
        SourceConfig::Csv {
            watch_dir,
            processed_dir,
            delimiter,
            chunk_size,
            poll_interval_secs,
        } => {
            log::info!("Source: CSV files from {}", watch_dir);
            let e = CsvExtractor::new(
                watch_dir,
                processed_dir,
                *delimiter,
                *chunk_size,
                Arc::clone(persistent_state),
                state_path.to_string(),
            )
            .unwrap_or_else(|e| {
                log::error!("Failed to initialize CSV extractor: {}", e);
                std::process::exit(1);
            });
            (*poll_interval_secs, Box::new(e))
        }
        SourceConfig::ClickHouse {
            host,
            database,
            query,
            username,
            password,
            chunk_size,
            poll_interval_secs,
        } => {
            log::info!("Source: ClickHouse at {} database {}", host, database);
            let e = ClickHouseExtractor::new(
                host.clone(),
                database.clone(),
                query.clone(),
                username.clone(),
                password.clone(),
                *chunk_size,
            )
            .unwrap_or_else(|e| {
                log::error!("Failed to initialize ClickHouse extractor: {}", e);
                std::process::exit(1);
            });
            (*poll_interval_secs, Box::new(e))
        }
    }
}

fn build_transformers(config: &PipelineConfig) -> Vec<Box<dyn Transformer>> {
    config
        .transforms
        .iter()
        .map(|tc| -> Box<dyn Transformer> {
            match tc {
                TransformConfig::Filter { column, value } => {
                    Box::new(FilterTransformer::new(column.clone(), value.clone()))
                }
                TransformConfig::Map { rename } => Box::new(MapTransformer::new(rename.clone())),
                TransformConfig::Aggregate { group_by, sum } => {
                    Box::new(AggregateTransformer::new(group_by.clone(), sum.clone()))
                }
            }
        })
        .collect()
}

fn chunk_size_from(config: &PipelineConfig) -> usize {
    match &config.source {
        SourceConfig::Csv { chunk_size, .. } => *chunk_size,
        SourceConfig::ClickHouse { chunk_size, .. } => *chunk_size,
        _ => 10_000,
    }
}

async fn build_loader(config: &PipelineConfig) -> PostgresLoader {
    log::info!("Connecting to destination DB...");
    PostgresLoader::connect(
        &config.destination.connection_string,
        config.destination.table.clone(),
        chunk_size_from(config),
        config.destination.unique_key.clone(),
    )
    .await
    .unwrap_or_else(|e| {
        log::error!("Failed to connect to destination: {}", e);
        std::process::exit(1);
    })
}

// ---------------------------------------------------------------------------
// Web + poll loop
// ---------------------------------------------------------------------------

fn spawn_web_server(app_state: AppState, port: u16) {
    tokio::spawn(async move {
        if let Err(e) = start_server(app_state, port).await {
            log::error!("Web server stopped: {}", e);
        }
    });
}

async fn run_loop(
    pipeline: &Pipeline,
    pipeline_state: &Arc<Mutex<PipelineState>>,
    persistent_state: &Arc<Mutex<PersistentState>>,
    app_state: &AppState,
    state_path: &str,
    poll_interval: u64,
) {
    loop {
        app_state.set_status(PipelineStatus::Running);
        let result = retry_with_backoff(3, 2, "pipeline", || pipeline.run(pipeline_state)).await;

        match result {
            // Nothing new this cycle
            Ok(0) => {
                app_state.set_status(PipelineStatus::Idle);
                app_state.log("[INFO] No new data".into());
                log::info!("No new data");
            }
            // Rows loaded — update UI + persist last_run / totals
            Ok(count) => {
                log::info!("Processed {} rows", count);
                app_state.add_rows(count);
                app_state.set_status(PipelineStatus::Idle);
                app_state.log(format!("[INFO] Processed {} rows", count));

                {
                    let mut s = persistent_state.lock().unwrap();
                    s.total_rows_processed += count;
                    s.last_run = pipeline_state.lock().unwrap().last_run;
                }
                save_state(persistent_state, state_path);
            }
            // Failure — count error and keep going next poll
            Err(e) => {
                log::error!("Pipeline error: {}", e);
                app_state.add_error();
                app_state.set_status(PipelineStatus::Error(e.to_string()));
                app_state.log(format!("[ERROR] {}", e));

                persistent_state.lock().unwrap().total_errors += 1;
                save_state(persistent_state, state_path);
            }
        }

        sleep(Duration::from_secs(poll_interval)).await;
    }
}
