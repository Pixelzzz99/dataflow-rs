mod config;
mod error;
mod extractor;
mod loader;
mod pipeline;
mod retry;
mod state;
mod transformer;
mod types;

use std::sync::{Arc, Mutex};
use tokio::time::{Duration, sleep};

use config::{SourceConfig, TransformConfig, load_config};
use extractor::csv::CsvExtractor;
use extractor::postgres::PostgresExtractor;
use loader::postgres::PostgresLoader;
use pipeline::{Pipeline, PipelineState};
use retry::retry_with_backoff;
use state::PersistentState;
use transformer::{
    aggregator::AggregateTransformer, filter::FilterTransformer, mapper::MapTransformer,
};

#[tokio::main]
async fn main() {
    env_logger::init();

    let args: Vec<String> = std::env::args().collect();
    let config_path = args
        .get(1)
        .map(String::as_str)
        .unwrap_or("config/pipeline.json");
    let state_path = args.get(2).map(String::as_str).unwrap_or("etl_state.json");

    log::info!("Loading config from: {}", config_path);
    log::info!("Loading state from {}", state_path);

    let config = match load_config(config_path) {
        Ok(c) => c,
        Err(e) => {
            log::error!("Failed to load config: {}", e);
            std::process::exit(1);
        }
    };

    let persistent_state = Arc::new(Mutex::new(PersistentState::load(state_path)));
    log::info!(
        "Loaded state: {} files already processed",
        persistent_state.lock().unwrap().processed_files.len()
    );

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
        _ => 10_000,
    };

    log::info!("Connecting to destination DB...");
    let loader = match PostgresLoader::connect(
        &config.destination.connection_string,
        config.destination.table.clone(),
        10_000,
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
    let state = Arc::new(Mutex::new(PipelineState::new()));

    log::info!("ETL Engine started. Polling every {}s...", poll_interval);

    loop {
        match pipeline.run(&state).await {
            Ok(0) => log::info!("No new data"),
            Ok(count) => {
                log::info!("Processed {} rows", count);
                let mut s = persistent_state.lock().unwrap();
                s.total_rows_processed += count;
                let snapshot = s.clone();
                drop(s);
                if let Err(e) = snapshot.save(state_path) {
                    log::warn!("Failed to save state: {}", e);
                }
            }
            Err(e) => {
                log::error!("Pipeline error: {}", e);
                persistent_state.lock().unwrap().total_errors += 1;
            }
        }

        sleep(Duration::from_secs(poll_interval)).await;
    }
}
