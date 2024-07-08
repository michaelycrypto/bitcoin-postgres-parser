use std::io::{self, Read, Seek};
use byteorder::{LittleEndian, ReadBytesExt};
use sha2::{Sha256, Digest};
use hex::encode;
use num_bigint::BigUint;
use num_traits::{CheckedMul, FromPrimitive, ToPrimitive};
use crate::models::{Block, Transaction, Input, Output};
use std::error::Error;
use time::OffsetDateTime;

pub fn read_block<R: Read + Seek>(reader: &mut R) -> io::Result<Block> {
    let _magic = reader.read_u32::<LittleEndian>()?;
    let _size = reader.read_u32::<LittleEndian>()?;

    let version = reader.read_i32::<LittleEndian>()?;
    let previous_block = read_hash(reader)?;
    let merkle_root = read_hash(reader)?;
    let time = OffsetDateTime::from_unix_timestamp(reader.read_u32::<LittleEndian>()? as i64)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let bits = format!("{:x}", reader.read_u32::<LittleEndian>()?);
    let nonce = reader.read_u32::<LittleEndian>()? as i64;

    let tx_count = read_var_int(reader)?;
    let mut transactions = Vec::with_capacity(tx_count.min(1_000_000) as usize); // Limit to prevent overflow

    for _ in 0..tx_count {
        let tx = read_transaction(reader)?;
        transactions.push(tx);
    }

    Ok(Block {
        block_hash: String::new(), // Placeholder, to be calculated later
        height: 0,
        time,
        difficulty: 0.0,
        merkle_root,
        nonce,
        size: 0,
        version,
        bits,
        previous_block,
        active: true,
        transactions,
    })
}

pub fn read_transaction<R: Read + Seek>(reader: &mut R) -> io::Result<Transaction> {
    let version = reader.read_i32::<LittleEndian>()?;

    // Check for SegWit marker and flag
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    let mut witness_data = None;
    let mut segwit = false;

    let marker = reader.read_u8()?;
    if marker == 0 {
        let flag = reader.read_u8()?;
        if flag != 1 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid SegWit flag"));
        }
        segwit = true;
    } else {
        // Not a SegWit transaction, rewind one byte
        reader.seek(io::SeekFrom::Current(-1))?;
    }

    let input_count = read_var_int(reader)?;

    for i in 0..input_count {
        inputs.push(read_input(reader, i as i32)?);
    }

    let output_count = read_var_int(reader)?;
    for i in 0..output_count {
        outputs.push(read_output(reader, i as i32)?);
    }

    if segwit {
        let mut witnesses = Vec::with_capacity(input_count as usize);
        for _ in 0..input_count {
            witnesses.push(read_witness_data(reader)?);
        }
        witness_data = Some(witnesses);
    }

    let locktime = reader.read_u32::<LittleEndian>()?;

    Ok(Transaction {
        txid: String::new(), // Placeholder, to be calculated later
        block_hash: String::new(),
        size: 0, // Placeholder, to be recalculated later
        version,
        locktime: locktime as i32,
        inputs,
        outputs,
        witness: witness_data,
    })
}

fn read_witness_data<R: Read>(reader: &mut R) -> io::Result<Vec<Vec<u8>>> {
    let witness_count = read_var_int(reader)?;
    let mut witness_fields = Vec::with_capacity(witness_count as usize);

    for _ in 0..witness_count {
        let length = read_var_int(reader)? as usize;
        let mut field = vec![0; length];
        reader.read_exact(&mut field)?;
        witness_fields.push(field);
    }

    Ok(witness_fields)
}

pub fn read_input<R: Read>(reader: &mut R, index: i32) -> io::Result<Input> {
    let previous_txid = read_hash(reader)?;
    let previous_output_index = reader.read_i32::<LittleEndian>()?;
    let script_sig_length = read_var_int(reader)? as usize;

    if script_sig_length > 1_000_000 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "scriptSig length too large"));
    }

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

    if script_pub_key_length > 1_000_000 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "scriptPubKey length too large"));
    }

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

pub fn calculate_block_hash(
    version: i32,
    previous_block: &str,
    merkle_root: &str,
    time: OffsetDateTime,
    bits: &str,
    nonce: i64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(&version.to_le_bytes());
    hasher.update(&hex::decode(previous_block).unwrap().iter().rev().cloned().collect::<Vec<u8>>());
    hasher.update(&hex::decode(merkle_root).unwrap().iter().rev().cloned().collect::<Vec<u8>>());
    hasher.update(&(time.unix_timestamp() as u32).to_le_bytes());
    hasher.update(&u32::from_str_radix(bits, 16).unwrap().to_le_bytes());
    hasher.update(&(nonce as u32).to_le_bytes());
    let first_hash = hasher.finalize();

    let mut hasher = Sha256::new();
    hasher.update(first_hash);
    encode(hasher.finalize().iter().rev().cloned().collect::<Vec<u8>>())
}

pub fn calculate_block_difficulty(bits: &str) -> Result<f64, Box<dyn Error>> {
    let bits = u32::from_str_radix(bits, 16)?;

    let exp = (bits >> 24) as u32;
    let coef = bits & 0x00ffffff;

    let coef = BigUint::from_u32(coef).ok_or("Invalid coefficient")?;
    let base: BigUint = BigUint::from_u32(256).ok_or("Invalid base value")?;
    let exp = exp.checked_sub(3).ok_or("Exponent underflow")?;
    let current_target = coef.checked_mul(&base.pow(exp))
        .ok_or("Overflow when calculating current target")?;

    let difficulty_1_target = BigUint::from_u32(0x00ffff).ok_or("Invalid coefficient for difficulty_1_target")?
        .checked_mul(&base.pow(0x1d - 3))
        .ok_or("Overflow when calculating difficulty_1_target")?;

    let current_target_f64 = current_target.to_f64().ok_or("Conversion to f64 failed for current target")?;
    let difficulty_1_target_f64 = difficulty_1_target.to_f64().ok_or("Conversion to f64 failed for difficulty_1_target")?;
    Ok(difficulty_1_target_f64 / current_target_f64)
}

pub fn calculate_tx(tx: &Transaction) -> (String, usize) {
    // Calculate the size
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

    // Calculate the txid
    let mut hasher = Sha256::new();
    hasher.update(&tx.version.to_le_bytes());

    for input in &tx.inputs {
        hasher.update(&hex::decode(&input.previous_txid).unwrap().iter().rev().cloned().collect::<Vec<u8>>());
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
    let txid = encode(hasher.finalize().iter().rev().cloned().collect::<Vec<u8>>());

    (txid, size)
}
