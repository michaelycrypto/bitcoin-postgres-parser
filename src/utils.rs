use std::io::{self, Read};
use byteorder::{LittleEndian, ReadBytesExt};
use sha2::{Sha256, Digest};
use hex::encode;
use num_bigint::BigUint;
use num_traits::{CheckedMul, FromPrimitive, ToPrimitive};
use crate::models::{Block, Transaction, Input, Output};
use std::error::Error;

pub fn read_block<R: Read>(reader: &mut R) -> io::Result<Block> {
    let _magic = reader.read_u32::<LittleEndian>()?;
    let _size = reader.read_u32::<LittleEndian>()?;

    let version = reader.read_i32::<LittleEndian>()?;
    let previous_block = read_hash(reader)?;
    let merkle_root = read_hash(reader)?;
    let time = chrono::NaiveDateTime::from_timestamp(reader.read_u32::<LittleEndian>()? as i64, 0);
    let bits = format!("{:x}", reader.read_u32::<LittleEndian>()?);
    let nonce = reader.read_u32::<LittleEndian>()? as i64;

    let height = 0;
    let active = true;

    let block_hash = calculate_block_hash(version, &previous_block, &merkle_root, time, &bits, nonce);
    let difficulty = calculate_block_difficulty(&bits)
        .expect("Failed to calculate difficulty");

    let mut transactions = Vec::new();
    let tx_count = read_var_int(reader)?;

    let mut transactions_size = 0;
    for _ in 0..tx_count {
        let tx = read_transaction(reader, &block_hash)?;
        transactions_size += calculate_transaction_size(&tx);
        transactions.push(tx);
    }

    // Calculate the block header size
    let block_header_size = 4 + 32 + 32 + 4 + 4 + 4;
    let size = block_header_size + transactions_size;

    Ok(Block {
        block_hash,
        height,
        time,
        difficulty,
        merkle_root,
        nonce,
        size: size as i32,
        version,
        bits,
        previous_block,
        active,
        transactions,
    })
}

pub fn read_transaction<R: Read>(reader: &mut R, block_hash: &str) -> io::Result<Transaction> {
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

    let transaction = Transaction {
        txid,
        block_hash: block_hash.to_string(),
        size: 0, // This will be recalculated
        version,
        locktime: locktime as i32,
        inputs,
        outputs,
    };

    // Calculate the size of the transaction
    let size = calculate_transaction_size(&transaction);

    Ok(Transaction {
        size: size as i32,
        ..transaction
    })
}


pub fn read_input<R: Read>(reader: &mut R, index: i32) -> io::Result<Input> {
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

pub fn read_output<R: Read>(reader: &mut R, index: i32) -> io::Result<Output> {
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

pub fn read_var_int<R: Read>(reader: &mut R) -> io::Result<u64> {
    let mut first = [0; 1];
    reader.read_exact(&mut first)?;

    match first[0] {
        0xFD => Ok(reader.read_u16::<LittleEndian>()? as u64),
        0xFE => Ok(reader.read_u32::<LittleEndian>()? as u64),
        0xFF => Ok(reader.read_u64::<LittleEndian>()?),
        _ => Ok(first[0] as u64),
    }
}

pub fn read_hash<R: Read>(reader: &mut R) -> io::Result<String> {
    let mut hash = [0; 32];
    reader.read_exact(&mut hash)?;
    Ok(encode(hash.iter().rev().cloned().collect::<Vec<u8>>()))
}

fn varint_size(value: u64) -> usize {
    match value {
        0..=0xFC => 1,
        0xFD..=0xFFFF => 3,
        0x10000..=0xFFFFFFFF => 5,
        _ => 9,
    }
}

fn calculate_transaction_size(tx: &Transaction) -> usize {
    let inputs_size: usize = tx.inputs.iter().map(|input| {
        32 + 4 + varint_size(input.script_sig.len() as u64) + (input.script_sig.len() / 2) + 4
    }).sum();

    let outputs_size: usize = tx.outputs.iter().map(|output| {
        8 + varint_size(output.script_pub_key.len() as u64) + (output.script_pub_key.len() / 2)
    }).sum();

    4 + varint_size(tx.inputs.len() as u64) + inputs_size + varint_size(tx.outputs.len() as u64) + outputs_size + 4
}

pub fn calculate_block_hash(
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

pub fn calculate_block_difficulty(bits: &str) -> Result<f64, Box<dyn Error>> {
    // Convert bits to a 32-bit unsigned integer
    let bits = u32::from_str_radix(bits, 16)?;

    // Extract the exponent and coefficient from bits
    let exp = (bits >> 24) as u32;
    let coef = bits & 0x00ffffff;

    // Calculate the current target using BigUint to prevent overflow
    let coef = BigUint::from_u32(coef).ok_or("Invalid coefficient")?;
    let base: BigUint = BigUint::from_u32(256).ok_or("Invalid base value")?;
    let exp = exp.checked_sub(3).ok_or("Exponent underflow")?;
    let current_target = coef.checked_mul(&base.pow(exp))
        .ok_or("Overflow when calculating current target")?;

    // Difficulty_1_target is usually 0x1d00ffff
    let difficulty_1_target = BigUint::from_u32(0x00ffff).ok_or("Invalid coefficient for difficulty_1_target")?
        .checked_mul(&base.pow(0x1d - 3))
        .ok_or("Overflow when calculating difficulty_1_target")?;

    // Calculate the difficulty
    let current_target_f64 = current_target.to_f64().ok_or("Conversion to f64 failed for current target")?;
    let difficulty_1_target_f64 = difficulty_1_target.to_f64().ok_or("Conversion to f64 failed for difficulty_1_target")?;
    Ok(difficulty_1_target_f64 / current_target_f64)
}

pub fn calculate_txid(inputs: &[Input], outputs: &[Output], version: i32, locktime: i32) -> String {
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

