mod types;
mod error;
mod config;
mod extractor;
mod loader;
mod transformer;
mod pipeline;
mod state;

use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

use config::{SourceConfig, TransformConfig, load_config};
use extractor::postgres::PostgresExtractor;
use transformer::{
    filter::FilterTransformer,
    mapper::MapTransformer,
    aggregator::AggregateTransformer,
};
use loader::postgres::PostgresLoader;
use pipeline::{Pipeline, PipelineState};

#[tokio::main]
async fn main() {
    env_logger::init();

    let args: Vec<String> = std::env::args().collect();
    let config_path = args.get(1).map(String::as_str).unwrap_or("config/pipeline.json");

    log::info!("Loading config from: {}", config_path);

    let config = match load_config(config_path) {
        Ok(c) => c,
        Err(e) => {
            log::error!("Failed to load config: {}", e);
            std::process::exit(1);
        }
    };



    let (poll_interval, connection_string, query) = match &config.source {
        SourceConfig::Postgres { poll_interval_secs, connection_string, query } => {
            (*poll_interval_secs, connection_string.clone(), query.clone())
        }
        SourceConfig::Csv {poll_interval_secs, ..} => {
            log::error!("CSV source not yet supported in main use v2");
            std::process::exit(1);
        }
    };

    let extractor = match PostgresExtractor::connect(
        &connection_string, 
        query,
    ).await {
        Ok(e) => e,
        Err(e) => {
            log::error!("Failed to connect to source: {}", e);
            std::process::exit(1);
        }
    };

    let transformers: Vec<Box<dyn transformer::Transformer>> = config.transforms
        .iter().map(|tc| -> Box<dyn transformer::Transformer> {
            match tc {
                TransformConfig::Filter { column, value } => 
                    Box::new(FilterTransformer::new(column.clone(), value.clone())),
                TransformConfig::Map { rename }  => 
                    Box::new(MapTransformer::new(rename.clone())),
                TransformConfig::Aggregate { group_by , sum } => 
                    Box::new(AggregateTransformer::new(group_by.clone(), sum.clone())), 
            }
        }).collect();

    log::info!("Connecting to destination DB...");
    let loader = match PostgresLoader::connect(
        &config.destination.connection_string,
        config.destination.table.clone(),
    ).await {
        Ok(l) => l,
        Err(e) => {
            log::error!("Failed to connect to destination: {}", e);
            std::process::exit(1);
        }
    };

    let pipeline = Pipeline::new(
        Box::new(extractor),
        transformers,
        Box::new(loader),
    );

    let state = Arc::new(Mutex::new(PipelineState::new()));

    log::info!("ETL Engine started. Polling every {}s...", poll_interval);

    loop {
        match pipeline.run(&state).await {
            Ok(0) => log::info!("No new data"),
            Ok(count) => log::info!("Processed {} rows", count),
            Err(e) => {
                log::error!("Pipeline error: {}", e);
                if let Ok(mut s) = state.lock(){
                    s.errors_count += 1;
                }

            }
        }

        sleep(Duration::from_secs(poll_interval)).await;
    }


    


}
