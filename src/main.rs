use std::fs::File;
use std::io::{self, BufReader};
use std::path::PathBuf;
use tokio::task;
use tokio::sync::{mpsc, Semaphore};
use std::sync::Arc;
use std::time::Instant;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
use dotenv::dotenv;
use std::env;

mod database;
mod models;
mod utils;

use database::{setup_database, insert_block};
use crate::models::Block;
use crate::utils::{read_block, calculate_tx, calculate_block_hash, calculate_block_difficulty};

pub async fn process_file(path: PathBuf, raw_tx: mpsc::Sender<Block>) -> io::Result<()> {
    let file = File::open(&path)?;
    let mut reader = BufReader::new(file);

    while let Ok(block) = read_block(&mut reader) {
        raw_tx.send(block).await.unwrap();
    }

    Ok(())
}

pub async fn calculate_hashes(mut raw_rx: mpsc::Receiver<Block>, processed_tx: mpsc::Sender<Block>) {
    while let Some(mut block) = raw_rx.recv().await {
        let mut transactions_size = 0;
        let _txids: Vec<_> = block.transactions.iter_mut().map(|tx| {
            let (txid, size) = calculate_tx(&tx);
            tx.txid = txid.clone();
            tx.size = size as i32;
            transactions_size += tx.size;
            txid
        }).collect();

        let block_header_size = 4 + 32 + 32 + 4 + 4 + 4;
        block.size = block_header_size + transactions_size;
        block.difficulty = calculate_block_difficulty(&block.bits).expect("Failed to calculate difficulty");
        block.block_hash = calculate_block_hash(
            block.version,
            &block.previous_block,
            &block.merkle_root,
            block.time,
            &block.bits,
            block.nonce,
        );
        
        processed_tx.send(block).await.unwrap();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").map_err(|_| "DATABASE_URL must be set in .env file")?;
    let blocks_path = env::var("BLOCKS_PATH").map_err(|_| "BLOCKS_PATH must be set in .env file")?;

    println!("Connecting to the database...");
    let config = database_url.parse::<tokio_postgres::Config>().map_err(|_| "Failed to parse DATABASE_URL")?;
    let manager = PostgresConnectionManager::new(config, NoTls);
    let pool = Pool::builder().max_size(200).build(manager).await?;

    println!("Connected to the database.");
    println!("Setting up the database schema...");
    setup_database(&pool).await?;
    println!("Database schema setup complete.");

    let mut paths: Vec<_> = std::fs::read_dir(&blocks_path)?.collect::<Result<Vec<_>, io::Error>>()?;
    paths.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    let (raw_tx, raw_rx) = mpsc::channel(100);
    let (processed_tx, mut processed_rx) = mpsc::channel(100);
    let semaphore = Arc::new(Semaphore::new(96));

    let hash_handle = task::spawn(calculate_hashes(raw_rx, processed_tx.clone()));
    let pool_clone = pool.clone();
    let insert_handle = task::spawn(async move {
        let mut block_counter = 0;
        let mut start_time = Instant::now();

        while let Some(block) = processed_rx.recv().await {
            let pool = pool_clone.clone();
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            task::spawn(async move {
                if let Err(e) = insert_block(&pool, &block).await {
                    eprintln!("Failed to insert block {:?}: {}", &block.block_hash, e);
                }
                drop(permit);
            });

            block_counter += 1;
            if block_counter % 50 == 0 {
                let duration = start_time.elapsed();
                println!("Processed 50 blocks in {:?}", duration);
                start_time = Instant::now();
            }
        }
    });

    for entry in paths {
        let raw_tx = raw_tx.clone();
        let path = entry.path().clone();
        if path.file_name().unwrap_or_default().to_str().unwrap().starts_with("blk") &&
            path.extension().unwrap_or_default() == "dat" {
            println!("Processing file: {:?}", path);
            if let Err(e) = process_file(path, raw_tx.clone()).await {
                eprintln!("Failed to process file: {}", e);
            }
        }
    }

    drop(raw_tx);
    drop(processed_tx);
    hash_handle.await?;
    insert_handle.await?;

    println!("All blocks processed.");
    Ok(())
}
