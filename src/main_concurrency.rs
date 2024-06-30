use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};
use tokio_postgres::{NoTls, Client, Config, CopyInSink};
use byteorder::{LittleEndian, ReadBytesExt};
use sha2::{Sha256, Digest};
use hex::encode;
use futures::stream::{self, StreamExt};
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use futures::SinkExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Connecting to the database...");
    
    let config = "host=localhost user=postgres password=postgres dbname=postgres"
        .parse::<Config>()
        .unwrap();
    let manager = PostgresConnectionManager::new(config, NoTls);
    let pool = Pool::builder().build(manager).await?;
    
    println!("Connected to the database.");

    println!("Setting up the database schema...");
    setup_database(&pool).await?;
    println!("Database schema setup complete.");

    let path = "/home/singular/.bitcoin/blocks/";
    let paths = std::fs::read_dir(path)?.collect::<Result<Vec<_>, io::Error>>()?;

    stream::iter(paths)
        .for_each_concurrent(None, |entry| {
            let pool = pool.clone();
            let path = entry.path().clone();
            async move {
                if path.file_name().unwrap_or_default().to_str().unwrap().starts_with("blk") &&
                    path.extension().unwrap_or_default() == "dat" {
                    println!("Processing file: {:?}", path);
                    if let Err(e) = process_file(&pool, path).await {
                        eprintln!("Failed to process file: {}", e);
                    }
                }
            }
        })
        .await;

    println!("All blocks processed.");
    Ok(())
}

async fn setup_database(pool: &Pool<PostgresConnectionManager<NoTls>>) -> Result<(), Box<dyn std::error::Error>> {
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
            value DOUBLE PRECISION,
            script_pub_key TEXT,
            PRIMARY KEY (txid, output_index)
        );
    ";

    let conn = pool.get().await?;
    conn.batch_execute(schema).await?;
    Ok(())
}

async fn process_file(pool: &Pool<PostgresConnectionManager<NoTls>>, path: PathBuf) -> io::Result<()> {
    let file = File::open(&path)?;
    let mut reader = BufReader::new(file);

    while let Ok(block) = read_block(&mut reader) {
        let start_time = std::time::Instant::now();
        if let Err(e) = insert_block(pool, &block).await {
            eprintln!("Failed to insert block: {}", e);
        } else {
            let duration = start_time.elapsed();
            println!("Block processed in {:?} - {}", duration, block.block_hash);
        }
    }

    Ok(())
}

#[derive(Debug)]
struct Block {
    block_hash: String,
    height: i32,
    time: chrono::NaiveDateTime,
    difficulty: i64,
    merkle_root: String,
    nonce: i64,
    size: i32,
    version: i32,
    bits: String,
    previous_block: String,
    active: bool,
    transactions: Vec<Transaction>,
}

#[derive(Debug)]
struct Transaction {
    txid: String,
    block_hash: String,
    size: i32,
    version: i32,
    locktime: i32,
    inputs: Vec<Input>,
    outputs: Vec<Output>,
}

#[derive(Debug)]
struct Input {
    input_index: i32,
    previous_txid: String,
    previous_output_index: i32,
    script_sig: String,
    sequence: i64,
}

#[derive(Debug)]
struct Output {
    output_index: i32,
    value: i64,
    script_pub_key: String,
}

fn read_block<R: Read>(reader: &mut R) -> io::Result<Block> {
    let _magic = reader.read_u32::<LittleEndian>()?;
    let _size = reader.read_u32::<LittleEndian>()?;

    let version = reader.read_i32::<LittleEndian>()?;
    let previous_block = read_hash(reader)?;
    let merkle_root = read_hash(reader)?;
    let time = chrono::NaiveDateTime::from_timestamp(reader.read_u32::<LittleEndian>()? as i64, 0);
    let bits = format!("{:x}", reader.read_u32::<LittleEndian>()?);
    let nonce = reader.read_u32::<LittleEndian>()? as i64;

    let height = 0;
    let difficulty = 0;
    let size = 0;
    let active = true;

    let block_hash = calculate_block_hash(version, &previous_block, &merkle_root, time, &bits, nonce);

    let mut transactions = Vec::new();
    let tx_count = read_var_int(reader)?;

    for _ in 0..tx_count {
        transactions.push(read_transaction(reader, &block_hash)?);
    }

    Ok(Block {
        block_hash,
        height,
        time,
        difficulty,
        merkle_root,
        nonce,
        size,
        version,
        bits,
        previous_block,
        active,
        transactions,
    })
}

fn read_transaction<R: Read>(reader: &mut R, block_hash: &str) -> io::Result<Transaction> {
    let version = reader.read_i32::<LittleEndian>()?;
    let _input_count = read_var_int(reader)?;

    let mut inputs = Vec::new();

    for i in 0.._input_count {
        inputs.push(read_input(reader, i as i32)?);
    }

    let _output_count = read_var_int(reader)?;

    let mut outputs = Vec::new();

    for i in 0.._output_count {
        outputs.push(read_output(reader, i as i32)?);
    }

    let locktime = reader.read_u32::<LittleEndian>()?;
    let txid = calculate_txid(&inputs, &outputs, version, locktime as i32);

    Ok(Transaction {
        txid,
        block_hash: block_hash.to_string(),
        size: 0,
        version,
        locktime: locktime as i32,
        inputs,
        outputs,
    })
}

fn read_input<R: Read>(reader: &mut R, index: i32) -> io::Result<Input> {
    let previous_txid = read_hash(reader)?;
    let previous_output_index = reader.read_i32::<LittleEndian>()?;
    let script_sig_length = read_var_int(reader)? as usize;

    let mut script_sig = vec![0; script_sig_length];
    reader.read_exact(&mut script_sig)?;
    let sequence = reader.read_u32::<LittleEndian>()? as i64;

    Ok(Input {
        input_index: index,
        previous_txid,
        previous_output_index,
        script_sig: encode(script_sig),
        sequence,
    })
}

fn read_output<R: Read>(reader: &mut R, index: i32) -> io::Result<Output> {
    let value = reader.read_i64::<LittleEndian>()?;
    let script_pub_key_length = read_var_int(reader)? as usize;

    let mut script_pub_key = vec![0; script_pub_key_length];
    reader.read_exact(&mut script_pub_key)?;

    Ok(Output {
        output_index: index,
        value,
        script_pub_key: encode(script_pub_key),
    })
}

fn read_var_int<R: Read>(reader: &mut R) -> io::Result<u64> {
    let mut first = [0; 1];
    reader.read_exact(&mut first)?;

    match first[0] {
        0xFD => Ok(reader.read_u16::<LittleEndian>()? as u64),
        0xFE => Ok(reader.read_u32::<LittleEndian>()? as u64),
        0xFF => Ok(reader.read_u64::<LittleEndian>()?),
        _ => Ok(first[0] as u64),
    }
}

fn read_hash<R: Read>(reader: &mut R) -> io::Result<String> {
    let mut hash = [0; 32];
    reader.read_exact(&mut hash)?;
    Ok(encode(hash.iter().rev().cloned().collect::<Vec<u8>>()))
}

fn calculate_block_hash(
    version: i32,
    previous_block: &str,
    merkle_root: &str,
    time: chrono::NaiveDateTime,
    bits: &str,
    nonce: i64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(&version.to_le_bytes());
    hasher.update(&hex::decode(previous_block).unwrap().iter().rev().cloned().collect::<Vec<u8>>());
    hasher.update(&hex::decode(merkle_root).unwrap().iter().rev().cloned().collect::<Vec<u8>>());
    hasher.update(&(time.timestamp() as u32).to_le_bytes());
    hasher.update(&u32::from_str_radix(bits, 16).unwrap().to_le_bytes());
    hasher.update(&(nonce as u32).to_le_bytes());
    let first_hash = hasher.finalize();

    let mut hasher = Sha256::new();
    hasher.update(first_hash);
    encode(hasher.finalize().iter().rev().cloned().collect::<Vec<u8>>())
}

fn calculate_txid(inputs: &[Input], outputs: &[Output], version: i32, locktime: i32) -> String {
    let mut hasher = Sha256::new();
    hasher.update(&version.to_le_bytes());

    for input in inputs {
        hasher.update(&hex::decode(&input.previous_txid).unwrap().iter().rev().cloned().collect::<Vec<u8>>());
        hasher.update(&(input.previous_output_index as u32).to_le_bytes());
        hasher.update(&hex::decode(&input.script_sig).unwrap());
        hasher.update(&(input.sequence as u32).to_le_bytes());
    }

    for output in outputs {
        hasher.update(&output.value.to_le_bytes());
        hasher.update(&hex::decode(&output.script_pub_key).unwrap());
    }

    hasher.update(&(locktime as u32).to_le_bytes());

    let first_hash = hasher.finalize();
    let mut hasher = Sha256::new();
    hasher.update(first_hash);
    encode(hasher.finalize().iter().rev().cloned().collect::<Vec<u8>>())
}

async fn insert_block(pool: &Pool<PostgresConnectionManager<NoTls>>, block: &Block) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = pool.get().await?;
    let transaction = conn.transaction().await?;

    // Insert block using COPY
    let mut block_sink: std::pin::Pin<Box<CopyInSink<bytes::Bytes>>> = Box::pin(transaction.copy_in("COPY blocks (block_hash, height, time, difficulty, merkle_root, nonce, size, version, bits, previous_block, active) FROM STDIN WITH DELIMITER ',' CSV").await?);
    let block_line = format!("{},{},{},{},{},{},{},{},{},{},{}\n", block.block_hash, block.height, block.time, block.difficulty as f64, block.merkle_root, block.nonce as f64, block.size, block.version, block.bits, block.previous_block, block.active);
    block_sink.as_mut().send(block_line.into()).await?;
    block_sink.as_mut().close().await?;

    // Insert transactions using COPY
    let mut tx_sink: std::pin::Pin<Box<CopyInSink<bytes::Bytes>>> = Box::pin(transaction.copy_in("COPY transactions (txid, block_hash, size, version, locktime) FROM STDIN WITH DELIMITER ',' CSV").await?);
    for tx in &block.transactions {
        let tx_line = format!("{},{},{},{},{}\n", tx.txid, block.block_hash, tx.size, tx.version, tx.locktime);
        tx_sink.as_mut().send(tx_line.into()).await?;
    }
    tx_sink.as_mut().close().await?;

    // Insert inputs using COPY
    let mut input_sink: std::pin::Pin<Box<CopyInSink<bytes::Bytes>>> = Box::pin(transaction.copy_in("COPY inputs (txid, input_index, previous_txid, previous_output_index, script_sig, sequence) FROM STDIN WITH DELIMITER ',' CSV").await?);
    for tx in &block.transactions {
        for input in &tx.inputs {
            let input_line = format!("{},{},{},{},{},{}\n", tx.txid, input.input_index, input.previous_txid, input.previous_output_index, input.script_sig, input.sequence);
            input_sink.as_mut().send(input_line.into()).await?;
        }
    }
    input_sink.as_mut().close().await?;

    // Insert outputs using COPY
    let mut output_sink: std::pin::Pin<Box<CopyInSink<bytes::Bytes>>> = Box::pin(transaction.copy_in("COPY outputs (txid, output_index, value, script_pub_key) FROM STDIN WITH DELIMITER ',' CSV").await?);
    for tx in &block.transactions {
        for output in &tx.outputs {
            let output_line = format!("{},{},{},{}\n", tx.txid, output.output_index, output.value as f64, output.script_pub_key);
            output_sink.as_mut().send(output_line.into()).await?;
        }
    }
    output_sink.as_mut().close().await?;

    transaction.commit().await?;
    Ok(())
}
