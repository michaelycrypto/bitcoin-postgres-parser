use byteorder::{LittleEndian, ReadBytesExt};
use hex::encode;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek};
use std::path::{Path, PathBuf};
use std::time::Instant;
use time::OffsetDateTime;
use tokio::task::spawn_blocking;

use crate::models::{Block, Transaction, Input, Output};

pub struct FileReader {
    path: PathBuf,
    verbose: bool,
    pub file_paths: Vec<PathBuf>,
}

impl FileReader {
    pub fn new(path: PathBuf, verbose: bool) -> Self {
        let mut file_reader = Self { path, verbose, file_paths: Vec::new() };
        file_reader.index_files().expect("Failed to index files");
        file_reader
    }

    fn index_files(&mut self) -> io::Result<()> {
        let mut paths: Vec<_> = std::fs::read_dir(&self.path)?.collect::<Result<Vec<_>, io::Error>>()?;
        paths.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        self.file_paths = paths.into_iter()
            .filter(|entry| {
                let path = entry.path();
                path.file_name().unwrap_or_default().to_str().unwrap().starts_with("blk")
                    && path.extension().unwrap_or_default() == "dat"
            })
            .map(|entry| entry.path())
            .collect();

        Ok(())
    }

    pub async fn read_file(&self, file_index: usize) -> io::Result<Vec<Block>> {
        if file_index >= self.file_paths.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "File index out of bounds"));
        }

        let path = &self.file_paths[file_index];
        if self.verbose {
            println!("Processing file: {:?}", path);
        }

        let start_time = Instant::now();
        let file_blocks = self.process_file(path).await?;
        let read_time = start_time.elapsed();

        if self.verbose {
            println!("Time taken to read file {:?}: {:?}", path, read_time);
        }

        Ok(file_blocks)
    }

    async fn process_file(&self, path: &Path) -> io::Result<Vec<Block>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut blocks = Vec::new();
        let mut block_tasks = Vec::new();

        while let Ok(block) = self.read_block(&mut reader) {
            let block_task = spawn_blocking(move || block);
            block_tasks.push(block_task);
        }

        for task in block_tasks {
            match task.await {
                Ok(block) => blocks.push(block),
                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
            }
        }

        Ok(blocks)
    }

    fn read_block<R: Read + Seek + Send + 'static>(&self, reader: &mut R) -> io::Result<Block> {
        let _magic = reader.read_u32::<LittleEndian>()?;
        let _size = reader.read_u32::<LittleEndian>()?;

        let version = reader.read_i32::<LittleEndian>()?;
        let previous_block = self.read_hash(reader)?;
        let merkle_root = self.read_hash(reader)?;
        let time = OffsetDateTime::from_unix_timestamp(reader.read_u32::<LittleEndian>()? as i64)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let bits = reader.read_u32::<LittleEndian>()?.to_le_bytes().to_vec();
        let nonce = reader.read_u32::<LittleEndian>()? as i64;

        let tx_count = self.read_var_int(reader)?;
        let mut transactions = Vec::with_capacity(tx_count.min(1_000_000) as usize);

        for _ in 0..tx_count {
            let tx = self.read_transaction(reader)?;
            transactions.push(tx);
        }

        Ok(Block {
            id: 0, // Placeholder, should be assigned later
            block_hash: Vec::new(), // Placeholder, to be calculated later
            height: 0, // Placeholder, to be assigned later
            time,
            difficulty: 0.0, // Placeholder, to be calculated later
            merkle_root,
            nonce,
            size: 0, // Placeholder, to be calculated later
            version,
            bits,
            previous_block,
            active: true,
            transactions,
        })
    }

    fn read_transaction<R: Read + Seek + Send + 'static>(&self, reader: &mut R) -> io::Result<Transaction> {
        let version = reader.read_i32::<LittleEndian>()?;

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
            reader.seek(io::SeekFrom::Current(-1))?;
        }

        let input_count = self.read_var_int(reader)?;
        for i in 0..input_count {
            inputs.push(self.read_input(reader, i as i32)?);
        }

        let output_count = self.read_var_int(reader)?;
        for i in 0..output_count {
            outputs.push(self.read_output(reader, i as i32)?);
        }

        if segwit {
            let mut witnesses = Vec::with_capacity(input_count as usize);
            for _ in 0..input_count {
                witnesses.push(self.read_witness_data(reader)?);
            }
            witness_data = Some(witnesses);
        }

        let locktime = reader.read_u32::<LittleEndian>()?;

        Ok(Transaction {
            id: 0, // Placeholder, should be assigned later
            txid: Vec::new(), // Placeholder, to be calculated later
            block_hash: Vec::new(), // Placeholder, to be assigned later
            size: 0, // Placeholder, to be recalculated later
            version,
            locktime: locktime as i32,
            inputs,
            outputs,
            witness: witness_data,
        })
    }

    fn read_witness_data<R: Read + Seek + Send + 'static>(&self, reader: &mut R) -> io::Result<Vec<Vec<u8>>> {
        let witness_count = self.read_var_int(reader)?;
        let mut witness_fields = Vec::with_capacity(witness_count as usize);

        for _ in 0..witness_count {
            let length = self.read_var_int(reader)? as usize;
            let mut field = vec![0; length];
            reader.read_exact(&mut field)?;
            witness_fields.push(field);
        }

        Ok(witness_fields)
    }

    fn read_input<R: Read + Seek + Send + 'static>(&self, reader: &mut R, index: i32) -> io::Result<Input> {
        let previous_txid = self.read_hash(reader)?;
        let previous_output_index = reader.read_i32::<LittleEndian>()?;
        let script_sig_length = self.read_var_int(reader)? as usize;

        if script_sig_length > 1_000_000 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "scriptSig length too large"));
        }

        let mut script_sig = vec![0; script_sig_length];
        reader.read_exact(&mut script_sig)?;
        let sequence = reader.read_u32::<LittleEndian>()? as i64;

        Ok(Input {
            id: 0, // Placeholder, should be assigned later
            txid: Vec::new(), // Placeholder, to be assigned later
            input_index: index,
            previous_txid,
            previous_output_index,
            script_sig: encode(script_sig),
            sequence,
        })
    }

    fn read_output<R: Read + Seek + Send + 'static>(&self, reader: &mut R, index: i32) -> io::Result<Output> {
        let value = reader.read_i64::<LittleEndian>()?;
        let script_pub_key_length = self.read_var_int(reader)? as usize;

        if script_pub_key_length > 1_000_000 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "scriptPubKey length too large"));
        }

        let mut script_pub_key = vec![0; script_pub_key_length];
        reader.read_exact(&mut script_pub_key)?;

        Ok(Output {
            id: 0, // Placeholder, should be assigned later
            txid: Vec::new(), // Placeholder, to be assigned later
            output_index: index,
            value,
            script_pub_key: encode(script_pub_key),
        })
    }

    fn read_var_int<R: Read + Seek + Send + 'static>(&self, reader: &mut R) -> io::Result<u64> {
        let mut first = [0; 1];
        reader.read_exact(&mut first)?;

        match first[0] {
            0xFD => Ok(reader.read_u16::<LittleEndian>()? as u64),
            0xFE => Ok(reader.read_u32::<LittleEndian>()? as u64),
            0xFF => Ok(reader.read_u64::<LittleEndian>()?),
            _ => Ok(first[0] as u64),
        }
    }

    fn read_hash<R: Read + Seek + Send + 'static>(&self, reader: &mut R) -> io::Result<Vec<u8>> {
        let mut hash = [0; 32];
        reader.read_exact(&mut hash)?;
        Ok(hash.to_vec())
    }
}
