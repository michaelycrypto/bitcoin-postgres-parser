use tokio_postgres::{Client, CopyInSink, Transaction};
use futures::SinkExt;
use crate::models::{Block, Transaction as Tx, Input, Output, BlockTransaction};
use time::OffsetDateTime;
use hex::encode;

pub struct Database {
    client: Client,
    block_id: i32,
    tx_id: i32,
    input_id: i32,
    output_id: i32,
    block_tx_id: i32,
}

impl Database {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            block_id: 1,
            tx_id: 1,
            input_id: 1,
            output_id: 1,
            block_tx_id: 1,
        }
    }

    pub async fn setup_schema(&self) -> Result<(), Box<dyn std::error::Error>> {
        let schema = "
            DROP TABLE IF EXISTS block_transactions;
            DROP TABLE IF EXISTS inputs;
            DROP TABLE IF EXISTS outputs;
            DROP TABLE IF EXISTS transactions;
            DROP TABLE IF EXISTS blocks;

            CREATE TABLE IF NOT EXISTS blocks (
                id INT PRIMARY KEY,
                block_hash BYTEA,
                height INT,
                time TIMESTAMP,
                difficulty DOUBLE PRECISION,
                merkle_root BYTEA,
                nonce DOUBLE PRECISION,
                size INT,
                version INT,
                bits BYTEA,
                previous_block BYTEA,
                active BOOLEAN
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id INT PRIMARY KEY,
                txid BYTEA,
                block_hash BYTEA,
                size INT,
                version INT,
                locktime INT
            );

            CREATE TABLE IF NOT EXISTS inputs (
                id INT PRIMARY KEY,
                txid BYTEA,
                input_index INT,
                previous_txid BYTEA,
                previous_output_index INT,
                script_sig TEXT,
                sequence BIGINT
            );

            CREATE TABLE IF NOT EXISTS outputs (
                id INT PRIMARY KEY,
                txid BYTEA,
                output_index INT,
                value DOUBLE PRECISION,
                script_pub_key TEXT
            );

            CREATE TABLE IF NOT EXISTS block_transactions (
                id INT PRIMARY KEY,
                block_id INT,
                transaction_id INT
            );
        ";

        self.client.batch_execute(schema).await?;
        Ok(())
    }

    pub async fn insert_blocks(&mut self, blocks: &[Block]) -> Result<(), Box<dyn std::error::Error>> {
        let transaction = self.client.transaction().await?;

        let mut block_lines = Vec::new();
        let mut tx_lines = Vec::new();
        let mut input_lines = Vec::new();
        let mut output_lines = Vec::new();
        let mut block_tx_lines = Vec::new();

        for block in blocks {
            let block_line = format!(
                "{}, \\x{}, {}, '{}', {}, \\x{}, {}, {}, {}, \\x{}, \\x{}, {}\n",
                self.block_id, encode(&block.block_hash), block.height, block.time, block.difficulty as f64,
                encode(&block.merkle_root), block.nonce as f64, block.size, block.version,
                encode(&block.bits), encode(&block.previous_block), block.active
            );
            block_lines.push(block_line);

            for tx in &block.transactions {
                if Self::is_bip30_conflict(&tx.txid) {
                    continue;
                }

                let tx_line = format!(
                    "{}, \\x{}, \\x{}, {}, {}, {}\n",
                    self.tx_id, encode(&tx.txid), encode(&block.block_hash),
                    tx.size, tx.version, tx.locktime
                );
                tx_lines.push(tx_line);

                for input in &tx.inputs {
                    let input_line = format!(
                        "{}, \\x{}, {}, \\x{}, {}, '{}', {}\n",
                        self.input_id, encode(&tx.txid), input.input_index,
                        encode(&input.previous_txid), input.previous_output_index,
                        input.script_sig, input.sequence
                    );
                    input_lines.push(input_line);
                    self.input_id += 1;
                }

                for output in &tx.outputs {
                    let output_line = format!(
                        "{}, \\x{}, {}, {}, '{}'\n",
                        self.output_id, encode(&tx.txid), output.output_index,
                        output.value as f64, output.script_pub_key
                    );
                    output_lines.push(output_line);
                    self.output_id += 1;
                }

                let block_tx_line = format!(
                    "{}, {}, {}\n",
                    self.block_tx_id, self.block_id, self.tx_id
                );
                block_tx_lines.push(block_tx_line);
                self.block_tx_id += 1;

                self.tx_id += 1;
            }

            self.block_id += 1;
        }

        Self::copy_data(&transaction, "blocks (id, block_hash, height, time, difficulty, merkle_root, nonce, size, version, bits, previous_block, active)", &block_lines).await?;
        Self::copy_data(&transaction, "transactions (id, txid, block_hash, size, version, locktime)", &tx_lines).await?;
        Self::copy_data(&transaction, "inputs (id, txid, input_index, previous_txid, previous_output_index, script_sig, sequence)", &input_lines).await?;
        Self::copy_data(&transaction, "outputs (id, txid, output_index, value, script_pub_key)", &output_lines).await?;
        Self::copy_data(&transaction, "block_transactions (id, block_id, transaction_id)", &block_tx_lines).await?;

        transaction.commit().await?;
        Ok(())
    }

    async fn copy_data(transaction: &Transaction<'_>, table: &str, lines: &[String]) -> Result<(), Box<dyn std::error::Error>> {
        let mut sink: std::pin::Pin<Box<CopyInSink<bytes::Bytes>>> = Box::pin(transaction.copy_in(&format!("COPY {} FROM STDIN WITH (FORMAT csv, DELIMITER ',', ESCAPE '\\', NULL '')", table)).await?);
        for line in lines {
            let formatted_line = line.replace("\\x", "\\\\x"); // Ensure backslashes are correctly escaped
            sink.as_mut().send(formatted_line.into()).await?;
        }
        sink.as_mut().close().await?;
        Ok(())
    }

    fn is_bip30_conflict(txid: &Vec<u8>) -> bool {
        let bip30_conflicts = [
            hex::decode("ef412cf1f8ff44bbf0bede1ea30a0ce741d625425edbf53883d53f7c682a0548").unwrap(),
            hex::decode("4a4780f0046f0f69d429a32b0307aabaf2fd437685ee18d28274f4cda1e3d40b").unwrap(),
        ];

        bip30_conflicts.iter().any(|conflict| conflict == txid)
    }
}
