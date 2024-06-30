use std::fs::File;
use std::io::{self, BufReader};
use std::path::PathBuf;
use tokio::task;
use tokio::sync::{mpsc, Semaphore};
use std::sync::Arc;
use std::time::Instant;
use futures::stream::{self, StreamExt};
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
use dotenv::dotenv;
use std::env;

mod database;
mod models;
mod processing;
mod utils;

use database::{setup_database, insert_block};
use models::Block;
use processing::process_file;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    // Retrieve and handle errors for DATABASE_URL and BLOCKS_PATH
    let database_url = env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL must be set in .env file")?;
    let blocks_path = env::var("BLOCKS_PATH")
        .map_err(|_| "BLOCKS_PATH must be set in .env file")?;

    println!("Connecting to the database...");

    let config = database_url
        .parse::<tokio_postgres::Config>()
        .map_err(|_| "Failed to parse DATABASE_URL")?;
    let manager = PostgresConnectionManager::new(config, NoTls);
    let pool = Pool::builder().build(manager).await?;

    println!("Connected to the database.");

    println!("Setting up the database schema...");
    setup_database(&pool).await?;
    println!("Database schema setup complete.");

    let paths = std::fs::read_dir(&blocks_path)?
        .collect::<Result<Vec<_>, io::Error>>()?;

    let (tx, mut rx) = mpsc::channel(100);
    let semaphore = Arc::new(Semaphore::new(1));

    let pool_clone = pool.clone();
    let handle = task::spawn(async move {
        let mut block_counter = 0;
        let mut start_time = Instant::now();

        while let Some((path, block)) = rx.recv().await {
            if let Err(e) = insert_block(&pool_clone, &block).await {
                eprintln!("Failed to insert block {:?}: {}", &block.block_hash, e);
            }

            block_counter += 1;
            if block_counter % 250 == 0 {
                let duration = start_time.elapsed();
                println!("Processed 250 blocks in {:?}", duration);
                start_time = Instant::now();
            }
        }
    });

    stream::iter(paths)
        .for_each_concurrent(None, |entry| {
            let tx = tx.clone();
            let path = entry.path().clone();
            let semaphore = Arc::clone(&semaphore);
            async move {
                let permit = semaphore.acquire().await.unwrap();
                if path.file_name().unwrap_or_default().to_str().unwrap().starts_with("blk") &&
                    path.extension().unwrap_or_default() == "dat" {
                    println!("Processing file: {:?}", path);
                    if let Err(e) = process_file(path, tx.clone()).await {
                        eprintln!("Failed to process file: {}", e);
                    }
                }
                drop(permit);
            }
        })
        .await;

    drop(tx);
    handle.await?;

    println!("All blocks processed.");
    Ok(())
}
