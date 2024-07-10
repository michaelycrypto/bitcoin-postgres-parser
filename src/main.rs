use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use chrono::Local;
use dotenv::dotenv;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time::interval;
use tokio_postgres::NoTls;

mod block_processor;
mod database;
mod file_reader;
mod models;

use database::{setup_database, insert_block};
use file_reader::FileReader;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL")?;
    let blocks_path = env::var("BLOCKS_PATH")?;
    let verbose = env::var("VERBOSE").unwrap_or_else(|_| "false".to_string()) == "true";

    println!("Connecting to the database...");
    let config = database_url.parse::<tokio_postgres::Config>()?;
    let manager = PostgresConnectionManager::new(config, NoTls);
    let pool = Pool::builder().max_size(100).build(manager).await?;

    println!("Connected to the database.");
    setup_database(&pool).await?;
    println!("Database schema setup complete.");

    let file_reader = FileReader::new(PathBuf::from(blocks_path), verbose);

    // Metrics tracking
    let total_blocks = Arc::new(AtomicUsize::new(0));
    let total_txs = Arc::new(AtomicUsize::new(0));
    let total_files_read = Arc::new(AtomicUsize::new(0));
    let runtime = Instant::now();

    let total_blocks_clone = Arc::clone(&total_blocks);
    let total_txs_clone = Arc::clone(&total_txs);
    let total_files_read_clone = Arc::clone(&total_files_read);
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(3));
        loop {
            interval.tick().await;

            let total_blocks = total_blocks_clone.load(Ordering::Relaxed);
            let total_txs = total_txs_clone.load(Ordering::Relaxed);
            let total_files_read = total_files_read_clone.load(Ordering::Relaxed);
            let elapsed = runtime.elapsed().as_secs();
            let tx_per_second = if elapsed > 0 { total_txs as f64 / elapsed as f64 } else { 0.0 };

            println!(
                "{} Files Read: {} Blocks: {} Tx: {} Tx/s {:.2} Runtime {}s",
                Local::now().format("%Y-%m-%d %H:%M:%S"),
                total_files_read,
                total_blocks,
                total_txs,
                tx_per_second,
                elapsed
            );
        }
    });

    // Initialize semaphore with 10 permits
    let semaphore = Arc::new(Semaphore::new(10));

    for file_index in 0..file_reader.file_paths.len() {
        match file_reader.read_file(file_index).await {
            Ok(blocks) => {
                

                let start_time = Instant::now();
                let mut processed_blocks = Vec::new();

                for block in blocks {
                    total_txs.fetch_add(block.transactions.len(), Ordering::Relaxed);
                    total_blocks.fetch_add(1, Ordering::Relaxed);

                    let processed_block = block_processor::process_block(block).await;
                    processed_blocks.push(processed_block);
                }

                total_files_read.fetch_add(1, Ordering::Relaxed);

                if verbose {
                    println!("Time taken to process blocks: {:?}", start_time.elapsed());
                }

                let insert_futures = FuturesUnordered::new();
                for block in processed_blocks.iter() {
                    let pool = pool.clone();
                    let block = block.clone();
                    let semaphore = semaphore.clone();

                    insert_futures.push(tokio::task::spawn(async move {
                        let _permit = semaphore.acquire().await.unwrap();

                        if let Err(e) = insert_block(&pool, &block).await {
                            eprintln!("Failed to insert block: {}", e);
                        }
                    }));
                }

                insert_futures.collect::<Vec<_>>().await;

                if verbose {
                    println!("Done in {:?}", start_time.elapsed());
                }

            }
            Err(e) => {
                eprintln!("Failed to read file at index {}: {}", file_index, e);
            }
        }
    }

    println!("All blocks processed.");
    Ok(())
}
