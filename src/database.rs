use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
use crate::models::Block;
use futures::SinkExt;
use tokio_postgres::CopyInSink;

pub async fn setup_database(pool: &Pool<PostgresConnectionManager<NoTls>>) -> Result<(), Box<dyn std::error::Error>> {
    let schema = "
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
            nonce DOUBLE PRECISION,
            size INT,
            version INT,
            bits VARCHAR(16),
            previous_block VARCHAR(64),
            active BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS transactions (
            txid VARCHAR(64) PRIMARY KEY,
            block_hash VARCHAR(64),
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
            value DOUBLE PRECISION,
            script_pub_key TEXT,
            PRIMARY KEY (txid, output_index)
        );
    ";

    let conn = pool.get().await?;
    conn.batch_execute(schema).await?;
    Ok(())
}

fn is_bip30_conflict(txid: &str) -> bool {
    txid == "ef412cf1f8ff44bbf0bede1ea30a0ce741d625425edbf53883d53f7c682a0548" ||
    txid == "4a4780f0046f0f69d429a32b0307aabaf2fd437685ee18d28274f4cda1e3d40b"
}

pub async fn insert_block(pool: &Pool<PostgresConnectionManager<NoTls>>, block: &Block) -> Result<(), Box<dyn std::error::Error>> {
    let conn = pool.get().await?;

    let mut block_sink: std::pin::Pin<Box<CopyInSink<bytes::Bytes>>> = Box::pin(conn.copy_in("COPY blocks (block_hash, height, time, difficulty, merkle_root, nonce, size, version, bits, previous_block, active) FROM STDIN WITH DELIMITER ',' CSV").await?);
    let block_line = format!("{},{},{},{},{},{},{},{},{},{},{}\n", block.block_hash, block.height, block.time, block.difficulty as f64, block.merkle_root, block.nonce as f64, block.size, block.version, block.bits, block.previous_block, block.active);
    block_sink.as_mut().send(block_line.into()).await?;
    block_sink.as_mut().close().await?;
    
    let mut tx_lines = Vec::new();
    let mut input_lines = Vec::new();
    let mut output_lines = Vec::new();

    for tx in &block.transactions {
        if is_bip30_conflict(&tx.txid) {
            continue;
        }

        let tx_line = format!("{},{},{},{},{}\n", tx.txid, block.block_hash, tx.size, tx.version, tx.locktime);
        tx_lines.push(tx_line);

        for input in &tx.inputs {
            let input_line = format!("{},{},{},{},{},{}\n", tx.txid, input.input_index, input.previous_txid, input.previous_output_index, input.script_sig, input.sequence);
            input_lines.push(input_line);
        }

        for output in &tx.outputs {
            let output_line = format!("{},{},{},{}\n", tx.txid, output.output_index, output.value as f64, output.script_pub_key);
            output_lines.push(output_line);
        }
    }

    // Process transactions
    let mut tx_sink: std::pin::Pin<Box<CopyInSink<bytes::Bytes>>> = Box::pin(conn.copy_in("COPY transactions (txid, block_hash, size, version, locktime) FROM STDIN WITH DELIMITER ',' CSV").await?);
    for line in tx_lines {
        tx_sink.as_mut().send(line.into()).await?;
    }
    tx_sink.as_mut().close().await?;

    // Process inputs
    let mut input_sink: std::pin::Pin<Box<CopyInSink<bytes::Bytes>>> = Box::pin(conn.copy_in("COPY inputs (txid, input_index, previous_txid, previous_output_index, script_sig, sequence) FROM STDIN WITH DELIMITER ',' CSV").await?);
    for line in input_lines {
        input_sink.as_mut().send(line.into()).await?;
    }
    input_sink.as_mut().close().await?;

    // Process outputs
    let mut output_sink: std::pin::Pin<Box<CopyInSink<bytes::Bytes>>> = Box::pin(conn.copy_in("COPY outputs (txid, output_index, value, script_pub_key) FROM STDIN WITH DELIMITER ',' CSV").await?);
    for line in output_lines {
        output_sink.as_mut().send(line.into()).await?;
    }
    output_sink.as_mut().close().await?;

    Ok(())
}
