use dotenv::dotenv;
use tokio;
use tokio_postgres::{NoTls, Transaction, Error, Client};
use serde::{Deserialize, Serialize};
use reqwest::Client as HttpClient;
use std::time::Duration;
use tokio::time::sleep;
use tokio_postgres::CopyInSink;
use futures::sink::SinkExt;
use std::time::Instant;
use std::env;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Block {
    hash: String,
    height: i32,
    time: i64,
    difficulty: f64,
    merkleroot: String,
    nonce: i64,
    size: i32,
    version: i32,
    bits: String,
    previousblockhash: Option<String>,
    tx: Vec<Tx>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Tx {
    txid: String,
    size: i32,
    version: i32,
    locktime: i64,
    vin: Vec<Vin>,
    vout: Vec<Vout>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Vin {
    txid: Option<String>,
    vout: Option<i32>,
    scriptSig: Option<ScriptSig>,
    sequence: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ScriptSig {
    asm: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Vout {
    value: f64,
    n: i32,
    scriptPubKey: ScriptPubKey,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ScriptPubKey {
    asm: String,
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let rpc_user = env::var("RPC_USER").expect("RPC_USER must be set");
    let rpc_password = env::var("RPC_PASSWORD").expect("RPC_PASSWORD must be set");
    let rpc_host = env::var("RPC_HOST").expect("RPC_HOST must be set");
    let rpc_port: u16 = env::var("RPC_PORT")
        .expect("RPC_PORT must be set")
        .parse()
        .expect("RPC_PORT must be a number");
    let pg_connection_string = env::var("PG_CONNECTION_STRING").expect("PG_CONNECTION_STRING must be set");

    let (mut client, connection) = tokio_postgres::connect(&pg_connection_string, NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    if let Err(e) = setup_database(&client).await {
        eprintln!("Failed to set up database: {}", e);
        return;
    }

    match fetch_best_block_hash(&rpc_host, &rpc_user, &rpc_password, rpc_port).await {
        Ok(latest_block_hash) => {
            if let Err(e) = sync_blockchain(&mut client, &latest_block_hash, &rpc_host, &rpc_user, &rpc_password, rpc_port).await {
                eprintln!("Failed to sync blockchain: {}", e);
            }
            if let Err(e) = keep_up_to_date(&mut client, &rpc_host, &rpc_user, &rpc_password, rpc_port).await {
                eprintln!("Failed to keep up to date: {}", e);
            }
        },
        Err(e) => eprintln!("Failed to fetch best block hash: {}", e),
    }
}

async fn setup_database(client: &Client) -> Result<(), Error> {
    println!("Setting up the database");
    client.batch_execute("
        DROP TABLE IF EXISTS inputs;
        DROP TABLE IF EXISTS outputs;
        DROP TABLE IF EXISTS transactions;
        DROP TABLE IF EXISTS blocks;

        CREATE TABLE IF NOT EXISTS blocks (
            block_hash VARCHAR(64) PRIMARY KEY,
            height INT,
            time TIMESTAMP,
            difficulty DOUBLE PRECISION,
            merkle_root VARCHAR(64),
            nonce BIGINT,
            size INT,
            version INT,
            bits VARCHAR(16),
            previous_block VARCHAR(64),
            active BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS transactions (
            txid VARCHAR(64) PRIMARY KEY,
            block_hash VARCHAR(64) REFERENCES blocks(block_hash),
            size INT,
            version INT,
            locktime BIGINT
        );

        CREATE TABLE IF NOT EXISTS inputs (
            txid VARCHAR(64) REFERENCES transactions(txid),
            input_index INT,
            previous_txid VARCHAR(64),
            previous_output_index INT,
            script_sig TEXT,
            sequence BIGINT,
            PRIMARY KEY (txid, input_index)
        );

        CREATE TABLE IF NOT EXISTS outputs (
            txid VARCHAR(64) REFERENCES transactions(txid),
            output_index INT,
            value NUMERIC(20, 8),
            script_pub_key TEXT,
            PRIMARY KEY (txid, output_index)
        );
    ").await.map_err(|e| {
        eprintln!("Error setting up database schema: {}", e);
        e
    })
}

async fn fetch_best_block_hash(rpc_host: &str, rpc_user: &str, rpc_password: &str, rpc_port: u16) -> Result<String, reqwest::Error> {
    let url = format!("http://{}:{}/", rpc_host, rpc_port);
    let client = HttpClient::new();
    let response = client
        .post(&url)
        .basic_auth(rpc_user, Some(rpc_password))
        .json(&serde_json::json!({
            "jsonrpc": "1.0",
            "id": "curltest",
            "method": "getbestblockhash",
            "params": []
        }))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    Ok(response["result"].as_str().unwrap().to_string())
}

async fn fetch_block(block_hash: &str, rpc_host: &str, rpc_user: &str, rpc_password: &str, rpc_port: u16) -> Result<Block, reqwest::Error> {
    let url = format!("http://{}:{}/", rpc_host, rpc_port);
    let client = HttpClient::new();
    let response = client
        .post(&url)
        .basic_auth(rpc_user, Some(rpc_password))
        .json(&serde_json::json!({
            "jsonrpc": "1.0",
            "id": "curltest",
            "method": "getblock",
            "params": [block_hash, 2] // Using verbosity 2 to get full transaction data
        }))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    Ok(serde_json::from_value(response["result"].clone()).unwrap())
}

async fn process_block(client: &mut Client, block: &Block) -> Result<(), Box<dyn std::error::Error>> {
    store_block(client, block, false).await?;
    
    let mut transactions = Vec::new();
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();

    for tx in &block.tx {
        transactions.push(tx.clone());
        for (index, vin) in tx.vin.iter().enumerate() {
            inputs.push((tx.txid.clone(), index as i32, vin.txid.clone(), vin.vout, vin.scriptSig.as_ref().map(|s| s.asm.clone()), vin.sequence));
        }
        for (index, vout) in tx.vout.iter().enumerate() {
            outputs.push((tx.txid.clone(), index as i32, vout.value, vout.scriptPubKey.asm.clone()));
        }
    }

    let transaction = client.transaction().await?;
    store_transactions(&transaction, &transactions, &block.hash).await?;
    store_inputs(&transaction, &inputs).await?;
    store_outputs(&transaction, &outputs).await?;
    transaction.commit().await?;

    Ok(())
}

async fn sync_blockchain(client: &mut Client, latest_block_hash: &str, rpc_host: &str, rpc_user: &str, rpc_password: &str, rpc_port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let mut block_hash = latest_block_hash.to_string();

    while !block_hash.is_empty() {
        // println!("Processing block {}", block_hash);
        let block = fetch_block(&block_hash, rpc_host, rpc_user, rpc_password, rpc_port).await?;

        let start = Instant::now();
        process_block(client, &block).await?;
        let duration = start.elapsed();
        println!("Block processed ({:?}) {}", duration, block_hash);

        block_hash = match block.previousblockhash {
            Some(hash) => hash,
            None => "".to_string(),
        };
    }

    Ok(())
}

async fn store_block(client: &Client, block: &Block, active: bool) -> Result<u64, Error> {
    client.execute(
        "INSERT INTO blocks (block_hash, height, time, difficulty, merkle_root, nonce, size, version, bits, previous_block, active) VALUES ($1, $2, to_timestamp($3), $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (block_hash) DO NOTHING",
        &[
            &block.hash,
            &block.height,
            &(block.time as f64),
            &(block.difficulty as f64),
            &block.merkleroot,
            &block.nonce,
            &block.size,
            &block.version,
            &block.bits,
            &block.previousblockhash,
            &active
        ]
    ).await.map_err(|e| {
        eprintln!("Error storing block {}: {}", block.hash, e);
        e
    })
}

async fn store_transactions(transaction: &Transaction<'_>, transactions: &[Tx], block_hash: &str) -> Result<(), Error> {
    let sink: CopyInSink<bytes::Bytes> = transaction.copy_in("COPY transactions (txid, block_hash, size, version, locktime) FROM STDIN WITH DELIMITER ',' CSV").await?;
    let mut sink = Box::pin(sink);
    for tx in transactions {
        let line = format!("{},{},{},{},{}\n", tx.txid, block_hash, tx.size, tx.version, tx.locktime);
        sink.send(line.into()).await.map_err(|e| {
            eprintln!("Error sending transaction {}: {}", tx.txid, e);
            e
        })?;
    }
    sink.close().await.map_err(|e| {
        eprintln!("Error closing transaction sink: {}", e);
        e
    })
}

async fn store_inputs(transaction: &Transaction<'_>, inputs: &[(String, i32, Option<String>, Option<i32>, Option<String>, i64)]) -> Result<(), Error> {
    let sink: CopyInSink<bytes::Bytes> = transaction.copy_in("COPY inputs (txid, input_index, previous_txid, previous_output_index, script_sig, sequence) FROM STDIN WITH DELIMITER ',' CSV").await?;
    let mut sink = Box::pin(sink);
    for input in inputs {
        let line = format!("{},{},{},{},{},{}\n", input.0, input.1, input.2.as_deref().unwrap_or(""), input.3.map_or("".to_string(), |i| i.to_string()), input.4.as_deref().unwrap_or(""), input.5);
        sink.send(line.into()).await.map_err(|e| {
            eprintln!("Error sending input for txid {}: {}", input.0, e);
            e
        })?;
    }
    sink.close().await.map_err(|e| {
        eprintln!("Error closing input sink: {}", e);
        e
    })
}

async fn store_outputs(transaction: &Transaction<'_>, outputs: &[(String, i32, f64, String)]) -> Result<(), Error> {
    let sink: CopyInSink<bytes::Bytes> = transaction.copy_in("COPY outputs (txid, output_index, value, script_pub_key) FROM STDIN WITH DELIMITER ',' CSV").await?;
    let mut sink = Box::pin(sink);
    for output in outputs {
        let line = format!("{},{},{},{}\n", output.0, output.1, output.2, output.3);
        sink.send(line.into()).await.map_err(|e| {
            eprintln!("Error sending output for txid {}: {}", output.0, e);
            e
        })?;
    }
    sink.close().await.map_err(|e| {
        eprintln!("Error closing output sink: {}", e);
        e
    })
}

async fn keep_up_to_date(client: &mut Client, rpc_host: &str, rpc_user: &str, rpc_password: &str, rpc_port: u16) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let latest_block_hash = fetch_best_block_hash(rpc_host, rpc_user, rpc_password, rpc_port).await?;
        let block = fetch_block(&latest_block_hash, rpc_host, rpc_user, rpc_password, rpc_port).await?;
        
        // Check if the block is already stored and marked as active
        let row = client.query_one("SELECT block_hash FROM blocks WHERE block_hash = $1 AND active = TRUE", &[&block.hash]).await;
        
        if row.is_err() {
            // Block is not active or does not exist, handle reorg
            handle_reorg(client, &block, rpc_host, rpc_user, rpc_password, rpc_port).await?;
        }

        sleep(Duration::from_secs(10)).await;
    }
}

async fn handle_reorg(client: &mut Client, new_block: &Block, rpc_host: &str, rpc_user: &str, rpc_password: &str, rpc_port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let mut block_hash = new_block.hash.clone();

    // Deactivate blocks from the current chain until we find the common ancestor
    while client.query_one("SELECT block_hash FROM blocks WHERE block_hash = $1 AND active = TRUE", &[&block_hash]).await.is_err() {
        let block = fetch_block(&block_hash, rpc_host, rpc_user, rpc_password, rpc_port).await?;
        client.execute("UPDATE blocks SET active = FALSE WHERE block_hash = $1", &[&block_hash]).await?;
        block_hash = block.previousblockhash.clone().unwrap_or_default();
    }

    // Activate blocks from the new chain
    block_hash = new_block.hash.clone();
    while !block_hash.is_empty() {
        let block = fetch_block(&block_hash, rpc_host, rpc_user, rpc_password, rpc_port).await?;
        process_block(client, &block).await?;
        block_hash = block.previousblockhash.clone().unwrap_or_default();
    }

    Ok(())
}
