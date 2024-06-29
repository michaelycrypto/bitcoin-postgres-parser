use tokio;
use tokio_postgres::{NoTls, Transaction, Error, Client};
use serde::{Deserialize, Serialize};
use reqwest::Client as HttpClient;
use std::time::Duration;
use tokio::time::sleep;
use tokio_postgres::CopyInSink;
use futures::sink::SinkExt;

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
    tx: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Tx {
    txid: String,
    blockhash: String,
    size: i32,
    version: i32,
    locktime: i32,
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

const RPC_USER: &str = "michaelycrypto";
const RPC_PASSWORD: &str = "123456";
const RPC_PORT: u16 = 8332;
const PG_CONNECTION_STRING: &str = "host=localhost user=postgres password=postgres dbname=postgres";

#[tokio::main]
async fn main() {
    let (mut client, connection) = tokio_postgres::connect(PG_CONNECTION_STRING, NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    if let Err(e) = setup_database(&client).await {
        eprintln!("Failed to set up database: {}", e);
        return;
    }

    match fetch_best_block_hash().await {
        Ok(latest_block_hash) => {
            if let Err(e) = sync_blockchain(&mut client, &latest_block_hash).await {
                eprintln!("Failed to sync blockchain: {}", e);
            }
            if let Err(e) = keep_up_to_date(&mut client).await {
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
            locktime INT
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
            value BIGINT,
            script_pub_key TEXT,
            PRIMARY KEY (txid, output_index)
        );
    ").await.map_err(|e| {
        eprintln!("Error setting up database schema: {}", e);
        e
    })
}

async fn fetch_best_block_hash() -> Result<String, reqwest::Error> {
    let url = format!("http://207.180.219.123:{}/", RPC_PORT);
    let client = HttpClient::new();
    let response = client
        .post(&url)
        .basic_auth(RPC_USER, Some(RPC_PASSWORD))
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

async fn fetch_block(block_hash: &str) -> Result<Block, reqwest::Error> {
    let url = format!("http://207.180.219.123:{}/", RPC_PORT);
    let client = HttpClient::new();
    let response = client
        .post(&url)
        .basic_auth(RPC_USER, Some(RPC_PASSWORD))
        .json(&serde_json::json!({
            "jsonrpc": "1.0",
            "id": "curltest",
            "method": "getblock",
            "params": [block_hash]
        }))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    // Print the response
    // println!("{}", response["result"]);
    
    Ok(serde_json::from_value(response["result"].clone()).unwrap())
}

async fn fetch_transaction(txid: &str) -> Result<Tx, reqwest::Error> {
    let url = format!("http://207.180.219.123:{}/", RPC_PORT);
    let client = HttpClient::new();
    let response = client
        .post(&url)
        .basic_auth(RPC_USER, Some(RPC_PASSWORD))
        .json(&serde_json::json!({
            "jsonrpc": "1.0",
            "id": "curltest",
            "method": "getrawtransaction",
            "params": [txid, 1]
        }))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    Ok(serde_json::from_value(response["result"].clone()).unwrap())
}

async fn sync_blockchain(client: &mut Client, latest_block_hash: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut block_hash = latest_block_hash.to_string();

    while !block_hash.is_empty() {
        println!("Fetching block {}", block_hash);
        let block = fetch_block(&block_hash).await.map_err(|e| {
            eprintln!("Failed to fetch block {}: {}", block_hash, e);
            e
        })?;

        println!("Storing block {}", block.hash);
        store_block(client, &block, true).await.map_err(|e| {
            eprintln!("Failed to store block {}: {}", block.hash, e);
            e
        })?;
        
        let mut transactions = Vec::new();
        // let mut inputs = Vec::new();
        // let mut outputs = Vec::new();

        println!("Indexing transactions for block {}", block.hash);

        for txid in &block.tx {
            let tx = fetch_transaction(txid).await.map_err(|e| {
                eprintln!("Failed to fetch transaction {}: {}", txid, e);
                e
            })?;
            transactions.push(tx.clone());
            // for (index, vin) in tx.vin.iter().enumerate() {
            //     inputs.push((tx.txid.clone(), index as i32, vin.txid.clone(), vin.vout, vin.scriptSig.as_ref().map(|s| s.asm.clone()), vin.sequence));
            // }
            // for (index, vout) in tx.vout.iter().enumerate() {
            //     outputs.push((tx.txid.clone(), index as i32, vout.value, vout.scriptPubKey.asm.clone()));
            // }
        }

        println!("Storing transactions for block {}", block.hash);

        let transaction = client.transaction().await.map_err(|e| {
            eprintln!("Failed to start transaction for block {}: {}", block.hash, e);
            e
        })?;
        store_transactions(&transaction, &transactions).await.map_err(|e| {
            eprintln!("Failed to store transactions for block {}: {}", block.hash, e);
            e
        })?;
        // store_inputs(&transaction, &inputs).await.map_err(|e| {
        //     eprintln!("Failed to store inputs for block {}: {}", block.hash, e);
        //     e
        // })?;
        // store_outputs(&transaction, &outputs).await.map_err(|e| {
        //     eprintln!("Failed to store outputs for block {}: {}", block.hash, e);
        //     e
        // })?;
        transaction.commit().await.map_err(|e| {
            eprintln!("Failed to commit transaction for block {}: {}", block.hash, e);
            e
        })?;

        block_hash = match block.previousblockhash {
            Some(hash) => hash,
            None => "".to_string(),
        };

        println!("Finished processing block {}", block.hash);
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

async fn store_transactions(transaction: &Transaction<'_>, transactions: &[Tx]) -> Result<(), Error> {
    let sink: CopyInSink<bytes::Bytes> = transaction.copy_in("COPY transactions (txid, block_hash, size, version, locktime) FROM STDIN WITH DELIMITER ',' CSV").await?;
    let mut sink = Box::pin(sink);
    for tx in transactions {
        let line = format!("{},{},{},{},{}\n", tx.txid, tx.blockhash, tx.size, tx.version, tx.locktime);
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

async fn keep_up_to_date(client: &mut Client) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let latest_block_hash = fetch_best_block_hash().await.map_err(|e| {
            eprintln!("Failed to fetch latest block hash: {}", e);
            e
        })?;
        let block = fetch_block(&latest_block_hash).await.map_err(|e| {
            eprintln!("Failed to fetch latest block {}: {}", latest_block_hash, e);
            e
        })?;
        
        // Check if the block is already stored and marked as active
        let row = client.query_one("SELECT block_hash FROM blocks WHERE block_hash = $1 AND active = TRUE", &[&block.hash]).await;
        
        if row.is_err() {
            // Block is not active or does not exist, handle reorg
            handle_reorg(client, &block).await.map_err(|e| {
                eprintln!("Failed to handle reorg for block {}: {}", block.hash, e);
                e
            })?;
        }

        sleep(Duration::from_secs(10)).await;
    }
}

async fn handle_reorg(client: &mut Client, new_block: &Block) -> Result<(), Box<dyn std::error::Error>> {
    let mut block_hash = new_block.hash.clone();

    // Deactivate blocks from the current chain until we find the common ancestor
    while client.query_one("SELECT block_hash FROM blocks WHERE block_hash = $1 AND active = TRUE", &[&block_hash]).await.is_err() {
        let block = fetch_block(&block_hash).await.map_err(|e| {
            eprintln!("Failed to fetch block {} during reorg: {}", block_hash, e);
            e
        })?;
        client.execute("UPDATE blocks SET active = FALSE WHERE block_hash = $1", &[&block_hash]).await.map_err(|e| {
            eprintln!("Failed to deactivate block {}: {}", block_hash, e);
            e
        })?;
        block_hash = block.previousblockhash.clone().unwrap_or_default();
    }

    // Activate blocks from the new chain
    let mut block_hash = new_block.hash.clone();
    while !block_hash.is_empty() {
        let block = fetch_block(&block_hash).await.map_err(|e| {
            eprintln!("Failed to fetch block {} during reorg activation: {}", block_hash, e);
            e
        })?;
        store_block(client, &block, true).await.map_err(|e| {
            eprintln!("Failed to store block {} during reorg activation: {}", block.hash, e);
            e
        })?;

        let mut transactions = Vec::new();
        let mut inputs = Vec::new();
        let mut outputs = Vec::new();

        for txid in &block.tx {
            let tx = fetch_transaction(txid).await.map_err(|e| {
                eprintln!("Failed to fetch transaction {} during reorg activation: {}", txid, e);
                e
            })?;
            transactions.push(tx.clone());
            for (index, vin) in tx.vin.iter().enumerate() {
                inputs.push((tx.txid.clone(), index as i32, vin.txid.clone(), vin.vout, vin.scriptSig.as_ref().map(|s| s.asm.clone()), vin.sequence));
            }
            for (index, vout) in tx.vout.iter().enumerate() {
                outputs.push((tx.txid.clone(), index as i32, vout.value, vout.scriptPubKey.asm.clone()));
            }
        }

        let transaction = client.transaction().await.map_err(|e| {
            eprintln!("Failed to start transaction during reorg activation for block {}: {}", block.hash, e);
            e
        })?;
        store_transactions(&transaction, &transactions).await.map_err(|e| {
            eprintln!("Failed to store transactions during reorg activation for block {}: {}", block.hash, e);
            e
        })?;
        store_inputs(&transaction, &inputs).await.map_err(|e| {
            eprintln!("Failed to store inputs during reorg activation for block {}: {}", block.hash, e);
            e
        })?;
        store_outputs(&transaction, &outputs).await.map_err(|e| {
            eprintln!("Failed to store outputs during reorg activation for block {}: {}", block.hash, e);
            e
        })?;
        transaction.commit().await.map_err(|e| {
            eprintln!("Failed to commit transaction during reorg activation for block {}: {}", block.hash, e);
            e
        })?;

        block_hash = block.previousblockhash.clone().unwrap_or_default();
    }

    Ok(())
}
