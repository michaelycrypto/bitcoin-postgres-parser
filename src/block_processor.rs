use hex::encode;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use crate::models::{Block, Transaction};

pub async fn process_block(mut block: Block) -> Block {
    let transactions_size: i32 = block.transactions.par_iter_mut().map(|tx| {
        let (txid, size) = calculate_tx(tx);
        tx.txid = txid.clone();
        tx.size = size as i32;
        size as i32
    }).sum();

    let block_header_size: i32 = 4 + 32 + 32 + 4 + 4 + 4;
    block.size = block_header_size + transactions_size;
    block.block_hash = calculate_block_hash(&block);

    block
}

pub fn calculate_block_hash(block: &Block) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(&block.version.to_le_bytes());
    hasher.update(&block.previous_block.iter().rev().cloned().collect::<Vec<u8>>());
    hasher.update(&block.merkle_root.iter().rev().cloned().collect::<Vec<u8>>());
    hasher.update(&(block.time.unix_timestamp() as u32).to_le_bytes());
    hasher.update(&(block.bits as u32).to_le_bytes());
    hasher.update(&(block.nonce as u32).to_le_bytes());
    let first_hash = hasher.finalize();

    let mut hasher = Sha256::new();
    hasher.update(first_hash);
    let second_hash = hasher.finalize();

    second_hash.iter().rev().cloned().collect::<Vec<u8>>()
}

pub fn calculate_tx(tx: &Transaction) -> (Vec<u8>, usize) {
    let inputs_size: usize = tx.inputs.iter().map(|input| {
        32 + 4 + varint_size(input.script_sig.len() as u64) + (input.script_sig.len() / 2) + 4
    }).sum();

    let outputs_size: usize = tx.outputs.iter().map(|output| {
        8 + varint_size(output.script_pub_key.len() as u64) + (output.script_pub_key.len() / 2)
    }).sum();

    let witness_size: usize = if let Some(witness) = &tx.witness {
        witness.iter().map(|witnesses| {
            witnesses.iter().map(|w| varint_size(w.len() as u64) + w.len()).sum::<usize>()
        }).sum()
    } else {
        0
    };

    let size = 4 + varint_size(tx.inputs.len() as u64) + inputs_size + varint_size(tx.outputs.len() as u64) + outputs_size + 4 + witness_size;

    let mut hasher = Sha256::new();
    hasher.update(&tx.version.to_le_bytes());

    for input in &tx.inputs {
        hasher.update(&input.previous_txid.iter().rev().cloned().collect::<Vec<u8>>());
        hasher.update(&(input.previous_output_index as u32).to_le_bytes());
        hasher.update(&hex::decode(&input.script_sig).unwrap());
        hasher.update(&(input.sequence as u32).to_le_bytes());
    }

    for output in &tx.outputs {
        hasher.update(&output.value.to_le_bytes());
        hasher.update(&hex::decode(&output.script_pub_key).unwrap());
    }

    hasher.update(&(tx.locktime as u32).to_le_bytes());

    let first_hash = hasher.finalize();
    let mut hasher = Sha256::new();
    hasher.update(first_hash);
    let txid = hasher.finalize().iter().rev().cloned().collect::<Vec<u8>>();

    (txid, size)
}

fn varint_size(value: u64) -> usize {
    match value {
        0..=0xFC => 1,
        0xFD..=0xFFFF => 3,
        0x10000..=0xFFFFFFFF => 5,
        _ => 9,
    }
}
