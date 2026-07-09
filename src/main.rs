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

use config::{SourceConfig, TransformConfig, load_config};
use extractor::clickhouse::ClickHouseExtractor;
use extractor::csv::CsvExtractor;
use extractor::postgres::PostgresExtractor;
use loader::postgres::PostgresLoader;
use pipeline::{Pipeline, PipelineState};
use retry::retry_with_backoff;
use state::PersistentState;
use transformer::{
    aggregator::AggregateTransformer, filter::FilterTransformer, mapper::MapTransformer,
};
use web::{AppState, PipelineStatus, start_server};

#[tokio::main]
async fn main() {
    env_logger::init();

    let args: Vec<String> = std::env::args().collect();
    let config_path = args
        .get(1)
        .map(String::as_str)
        .unwrap_or("config/pipeline.json");
    let state_path = args.get(2).map(String::as_str).unwrap_or("etl_state.json");

    let web_port: u16 = args
        .get(3)
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(3000);

    log::info!("Loading config from: {}", config_path);
    log::info!("Loading state from {}", state_path);
    log::info!("Web UI: http://localhost:{}", web_port);

    let app_state = AppState::new(config_path.to_string());

    let config = match load_config(config_path) {
        Ok(c) => c,
        Err(e) => {
            log::error!("Failed to load config: {}", e);
            std::process::exit(1);
        }
    };

    let persistent_state = Arc::new(Mutex::new(PersistentState::load(state_path)));
    {
        let ps = persistent_state.lock().unwrap();
        log::info!(
            "Loaded state: {} files already processed, last_run: {}",
            ps.processed_files.len(),
            ps.last_run
        );
        app_state.restore_stats(ps.total_rows_processed, ps.total_errors);
    }

    let (poll_interval, extractor): (u64, Box<dyn extractor::Extractor>) = match &config.source {
        SourceConfig::Postgres {
            connection_string,
            query,
            poll_interval_secs,
        } => {
            log::info!("Source:PostgreSQL");
            let e = match PostgresExtractor::connect(connection_string, query.clone()).await {
                Ok(extractor) => extractor,
                Err(e) => {
                    log::error!("Failed to connect to source: {}", e);
                    std::process::exit(1);
                }
            };
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
            let e = match CsvExtractor::new(
                watch_dir,
                processed_dir,
                *delimiter,
                *chunk_size,
                Arc::clone(&persistent_state),
                state_path.to_string(),
            ) {
                Ok(extractor) => extractor,
                Err(e) => {
                    log::error!("Failed to initialize CSV extractor: {}", e);
                    std::process::exit(1);
                }
            };
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
            let e = match ClickHouseExtractor::new(
                host.clone(),
                database.clone(),
                query.clone(),
                username.clone(),
                password.clone(),
                *chunk_size,
            ) {
                Ok(extractor) => extractor,
                Err(e) => {
                    log::error!("Failed to initialize ClickHouse extractor: {}", e);
                    std::process::exit(1);
                }
            };
            (*poll_interval_secs, Box::new(e))
        }
    };

    let transformers: Vec<Box<dyn transformer::Transformer>> = config
        .transforms
        .iter()
        .map(|tc| -> Box<dyn transformer::Transformer> {
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
        .collect();

    let chunk_size = match &config.source {
        SourceConfig::Csv { chunk_size, .. } => *chunk_size,
        SourceConfig::ClickHouse { chunk_size, .. } => *chunk_size,
        _ => 10_000,
    };

    log::info!("Connecting to destination DB...");
    let loader = match PostgresLoader::connect(
        &config.destination.connection_string,
        config.destination.table.clone(),
        chunk_size,
        config.destination.unique_key.clone(),
    )
    .await
    {
        Ok(l) => l,
        Err(e) => {
            log::error!("Failed to connect to destination: {}", e);
            std::process::exit(1);
        }
    };

    let pipeline = Pipeline::new(extractor, transformers, Box::new(loader));
    let pipeline_state = {
        let ps = persistent_state.lock().unwrap();
        Arc::new(Mutex::new(PipelineState {
            last_run: ps.last_run,
            rows_processed: ps.total_rows_processed,
            errors_count: ps.total_errors,
        }))
    };

    let app_state_web = app_state.clone();
    let app_state_pipeline = app_state.clone();

    tokio::spawn(async move {
        if let Err(e) = start_server(app_state_web, web_port).await {
            log::error!("Web server stopped: {}", e);
        }
    });

    log::info!("ETL Engine started. Polling every {}s...", poll_interval);

    loop {
        app_state_pipeline.set_status(PipelineStatus::Running);
        let result = retry_with_backoff(3, 2, "pipeline", || pipeline.run(&pipeline_state)).await;

        match result {
            Ok(0) => {
                app_state_pipeline.set_status(PipelineStatus::Idle);
                let msg = "No new data".to_string();
                app_state_pipeline.log(format!("[INFO] {}", msg));
                log::info!("{}", msg);
            }
            Ok(count) => {
                log::info!("Processed {} rows", count);
                app_state_pipeline.add_rows(count);
                app_state_pipeline.set_status(PipelineStatus::Idle);
                app_state_pipeline.log(format!("[INFO] Processed {} rows", count));

                let mut s = persistent_state.lock().unwrap();
                s.total_rows_processed += count;
                s.last_run = pipeline_state.lock().unwrap().last_run;
                let snapshot = s.clone();
                drop(s);
                if let Err(e) = snapshot.save(state_path) {
                    log::warn!("Failed to save state: {}", e);
                }
            }
            Err(e) => {
                log::error!("Pipeline error: {}", e);
                app_state_pipeline.add_error();
                app_state_pipeline.set_status(PipelineStatus::Error(e.to_string()));
                app_state_pipeline.log(format!("[ERROR] {}", e));

                let mut s = persistent_state.lock().unwrap();
                s.total_errors += 1;
                let snapshot = s.clone();
                drop(s);
                if let Err(save_err) = snapshot.save(state_path) {
                    log::warn!("Failed to save state: {}", save_err);
                }
            }
        }

        sleep(Duration::from_secs(poll_interval)).await;
    }
}
