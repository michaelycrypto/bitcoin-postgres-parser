use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::PathBuf;
use tokio::sync::mpsc;
use crate::models::Block;
use crate::utils::{read_block};

pub async fn process_file(path: PathBuf, tx: mpsc::Sender<(PathBuf, Block)>) -> io::Result<()> {
    let file = File::open(&path)?;
    let mut reader = BufReader::new(file);

    while let Ok(block) = read_block(&mut reader) {
        tx.send((path.clone(), block)).await.unwrap();
    }

    Ok(())
}
