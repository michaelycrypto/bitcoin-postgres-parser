#[derive(Debug)]
pub struct Block {
    pub block_hash: String,
    pub height: i32,
    pub time: chrono::NaiveDateTime,
    pub difficulty: i64,
    pub merkle_root: String,
    pub nonce: i64,
    pub size: i32,
    pub version: i32,
    pub bits: String,
    pub previous_block: String,
    pub active: bool,
    pub transactions: Vec<Transaction>,
}

#[derive(Debug)]
pub struct Transaction {
    pub txid: String,
    pub block_hash: String,
    pub size: i32,
    pub version: i32,
    pub locktime: i32,
    pub inputs: Vec<Input>,
    pub outputs: Vec<Output>,
}

#[derive(Debug)]
pub struct Input {
    pub input_index: i32,
    pub previous_txid: String,
    pub previous_output_index: i32,
    pub script_sig: String,
    pub sequence: i64,
}

#[derive(Debug)]
pub struct Output {
    pub output_index: i32,
    pub value: i64,
    pub script_pub_key: String,
}
