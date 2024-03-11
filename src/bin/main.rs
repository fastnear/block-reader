mod xblock;
mod common;

use std::collections::HashMap;
use std::{env};
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use dotenv::dotenv;
use flate2::read::GzDecoder;
use near_primitives::borsh::BorshDeserialize;
use near_primitives::types::BlockHeight;
use tokio::sync::{mpsc, Semaphore};
use crate::xblock::XBlock;
use futures::stream::FuturesOrdered;
use futures::StreamExt;

const PROJECT_ID: &str = "block_reader";
const SAVE_EVERY_N: BlockHeight = 1000;

#[derive(Debug, Clone)]
pub struct ReadConfig {
    pub from_block: BlockHeight,
    pub to_block: BlockHeight,
    pub path: String,
    pub num_reader_threads: usize,
}

fn read_archive(path: &str) -> HashMap<String, Vec<u8>> {
    if !std::path::Path::new(path).exists() {
        return HashMap::new();
    }
    tar::Archive::new(GzDecoder::new(std::fs::File::open(path).unwrap()))
        .entries()
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|mut e| {
            let path = e.path().unwrap().to_string_lossy().to_string();
            let mut content = Vec::new();
            e.read_to_end(&mut content).unwrap();
            (path, content)
        })
        .collect()
}


pub fn start(from_block: BlockHeight, to_block: BlockHeight, path: String) -> Vec<XBlock> {
    let mut current_file = "".to_string();
    let mut blocks = HashMap::new();
    let mut all_blocks = vec![];
    tracing::log::info!(target: PROJECT_ID, "Reading blocks from {} to {}", from_block, to_block);

    for block_height in from_block..to_block {
        tracing::log::debug!(target: PROJECT_ID, "Processing block: {}", block_height);
        let rounded_padded_block_height = format!("{:0>12}", block_height / SAVE_EVERY_N * SAVE_EVERY_N);
        let padded_block_height = format!("{:0>12}", block_height);
        let file_path = format!(
            "{}/{}/{}.tgz",
            path,
            &rounded_padded_block_height[..6],
            rounded_padded_block_height
        );
        if file_path != current_file {
            tracing::log::debug!(target: PROJECT_ID, "Reading archive: {}", file_path);
            current_file = file_path;
            blocks = read_archive(&current_file);
        }
        let block_bytes = blocks.get(&format!("{}.borsh", padded_block_height));
        if let Some(block_bytes) = block_bytes {
            let block = XBlock::try_from_slice(&block_bytes).expect("Failed to decode block");
            all_blocks.push(block)
        } else {
            tracing::log::debug!(target: PROJECT_ID, "Block not found: {}", block_height);
        }
    }
    all_blocks
}

pub fn streamer(config: ReadConfig) -> mpsc::Receiver<XBlock> {
    let (sender, receiver) = mpsc::channel(SAVE_EVERY_N as usize * (config.num_reader_threads + 2));
    let mut batches = vec![(config.from_block, std::cmp::min(SAVE_EVERY_N, config.to_block - config.from_block))];
    for i in (config.from_block / SAVE_EVERY_N + 1)..(config.to_block / SAVE_EVERY_N) {
        batches.push((i * SAVE_EVERY_N, SAVE_EVERY_N));
    }
    if (config.to_block % SAVE_EVERY_N) != 0 {
        batches.push((config.to_block / SAVE_EVERY_N * SAVE_EVERY_N, config.to_block % SAVE_EVERY_N));
    }
    let semaphore = Arc::new(Semaphore::new(config.num_reader_threads));
    let current_batch_index = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tasks = FuturesOrdered::new();

    tokio::spawn(async move {
        // spawning reader threads up to the limit in the config
        for (batch_index, (from_block, num_blocks)) in batches.into_iter().enumerate() {
            let semaphore_permit = semaphore.clone().acquire_owned().await.unwrap(); // Acquire a permit for semaphore
            let path = config.path.clone();
            let current_batch_index = current_batch_index.clone();
            let sender = sender.clone();

            let task = tokio::spawn(async move {
                let _permit = semaphore_permit; // Drop the permit when the task is done
                let my_batch_index = batch_index;
                let blocks = start(from_block, from_block + num_blocks, path);
                loop {
                    if current_batch_index.load(Ordering::SeqCst) == my_batch_index {
                        for block in blocks {
                            sender.send(block).await.unwrap();
                        }
                        current_batch_index.fetch_add(1, Ordering::SeqCst);
                        break;
                    } else {
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await; // Wait before retrying
                    }
                }
            });

            tasks.push_back(task);
        }

        while let Some(result) = tasks.next().await {
            match result {
                Ok(_) => {

                },
                Err(e) => panic!("Task failed: {}", e),
            }
        }
    });

    receiver
}

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    common::setup_tracing( &format!("{}=info", PROJECT_ID));

    let read_config = ReadConfig {
        from_block: env::var("FROM_BLOCK")
            .expect("FROM_BLOCK is required")
            .parse::<BlockHeight>()
            .unwrap(),
        to_block: env::var("TO_BLOCK")
            .expect("TO_BLOCK is required")
            .parse::<BlockHeight>()
            .unwrap(),
        path: env::var("BLOCKS_DATA_PATH").expect("BLOCKS_DATA_PATH is required"),
        num_reader_threads: env::var("NUM_READER_THREADS")
            .map(|s| s.parse::<usize>().expect("NUM_READER_THREADS must be a number"))
            .unwrap_or(1),
    };

    let consumer_config = ConsumerConfig {
    };

    let stream = streamer(read_config);
    listen_blocks(stream, consumer_config).await;
}

pub struct ConsumerConfig {
}

async fn listen_blocks(mut stream: mpsc::Receiver<XBlock>, _config: ConsumerConfig) {
    while let Some(xblock) = stream.recv().await {
        let block_height = xblock.block.header.height;

        if block_height % 1000 == 0 {
            tracing::log::info!(target: PROJECT_ID, "Processing block: {}", block_height);
        }
        tracing::log::debug!(target: PROJECT_ID, "Processing block: {}", block_height);
    }
}
