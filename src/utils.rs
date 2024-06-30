use std::io::{self, Read};
use byteorder::{LittleEndian, ReadBytesExt};
use sha2::{Sha256, Digest};
use hex::encode;
use crate::models::{Block, Transaction, Input, Output};

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
