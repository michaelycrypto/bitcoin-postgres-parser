use std::path::PathBuf;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use dotenv::dotenv;
use std::env;
use tokio_postgres::NoTls;
use std::time::Instant;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::{Semaphore, OwnedSemaphorePermit};
use std::sync::Arc;

mod database;
mod models;
mod file_reader;
mod block_processor;

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
    let pool = Pool::builder().build(manager).await?;

    println!("Connected to the database.");
    setup_database(&pool).await?;
    println!("Database schema setup complete.");

    let file_reader = FileReader::new(PathBuf::from(blocks_path), verbose);

    let semaphore = Arc::new(Semaphore::new(5)); // Limit concurrency to 5 tasks
    let mut insert_futures = FuturesUnordered::new();

    for file_index in 0..file_reader.file_paths.len() {
        match file_reader.read_file(file_index).await {
            Ok(blocks) => {
                let start_time = Instant::now();
                let mut processed_blocks = Vec::new();
                for block in blocks {
                    let processed_block = block_processor::process_block(block).await;
                    processed_blocks.push(processed_block);
                }
                for block in processed_blocks {
                    let pool = pool.clone();
                    let semaphore = semaphore.clone();
                    insert_futures.push(tokio::spawn(async move {
                        let permit: OwnedSemaphorePermit = semaphore.acquire_owned().await.unwrap();
                        
                        if let Err(e) = insert_block(&pool, &block).await {
                            eprintln!("Failed to insert block: {}", e);
                        }
                        
                        drop(permit); // Release the permit
                    }));
                }

                if verbose {
                    println!("Time taken to process blocks: {:?}", start_time.elapsed());
                }

                let insert_start_time = Instant::now();

                // Wait for all insertion tasks to complete
                while let Some(result) = insert_futures.next().await {
                    if let Err(e) = result {
                        eprintln!("Task failed: {}", e);
                    }
                }
                println!("Time taken to insert blocks: {:?}", insert_start_time.elapsed());
                println!("Time taken to process & insert blocks: {:?}", start_time.elapsed());

            }
            Err(e) => {
                eprintln!("Failed to read file at index {}: {}", file_index, e);
            }
        }
    }

    println!("All blocks processed.");
    Ok(())
}
