#[derive(Debug, Clone)]
pub struct Block {
    pub block_hash: String,
    pub height: i32,
    pub time: time::OffsetDateTime,
    pub difficulty: f64,
    pub merkle_root: String,
    pub nonce: i64,
    pub size: i32,
    pub version: i32,
    pub bits: String,
    pub previous_block: String,
    pub active: bool,
    pub transactions: Vec<Transaction>,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub txid: String,
    pub block_hash: String,
    pub size: i32,
    pub version: i32,
    pub locktime: i32,
    pub inputs: Vec<Input>,
    pub outputs: Vec<Output>,
    pub witness: Option<Vec<Vec<Vec<u8>>>>, // Optional witness data for SegWit transactions
}

#[derive(Debug, Clone)]
pub struct Input {
    pub input_index: i32,
    pub previous_txid: String,
    pub previous_output_index: i32,
    pub script_sig: String,
    pub sequence: i64,
}

#[derive(Debug, Clone)]
pub struct Output {
    pub output_index: i32,
    pub value: i64,
    pub script_pub_key: String,
}
