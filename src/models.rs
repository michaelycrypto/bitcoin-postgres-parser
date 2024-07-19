#[derive(Debug, Clone)]
pub struct Block {
    pub id: i32,
    pub block_hash: Vec<u8>,
    pub height: i32,
    pub time: time::OffsetDateTime,
    pub difficulty: f64,
    pub merkle_root: Vec<u8>,
    pub nonce: i64,
    pub size: i32,
    pub version: i32,
    pub bits: i32,
    pub previous_block: Vec<u8>,
    pub active: bool,
    pub transactions: Vec<Transaction>,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub id: i32,
    pub txid: Vec<u8>,
    pub block_hash: Vec<u8>,
    pub size: i32,
    pub version: i32,
    pub locktime: i32,
    pub inputs: Vec<Input>,
    pub outputs: Vec<Output>,
    pub witness: Option<Vec<Vec<Vec<u8>>>>, // Optional witness data for SegWit transactions
}

#[derive(Debug, Clone)]
pub struct Input {
    pub id: i32,
    pub txid: Vec<u8>,
    pub input_index: i32,
    pub previous_txid: Vec<u8>,
    pub previous_output_index: i32,
    pub script_sig: String,
    pub sequence: i64,
}

#[derive(Debug, Clone)]
pub struct Output {
    pub id: i32,
    pub txid: Vec<u8>,
    pub output_index: i32,
    pub value: i64,
    pub script_pub_key: String,
}

#[derive(Debug, Clone)]
pub struct BlockTransaction {
    pub id: i32,
    pub block_id: i32,
    pub transaction_id: i32,
}
