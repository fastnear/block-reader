mod xblock;
mod common;

use std::collections::HashMap;
use std::{env};
use std::io::Read;
use dotenv::dotenv;
use flate2::read::GzDecoder;
use near_primitives::borsh::BorshDeserialize;
use near_primitives::types::BlockHeight;
use tokio::sync::mpsc;
use crate::xblock::XBlock;

const PROJECT_ID: &str = "block_reader";
const SAVE_EVERY_N: BlockHeight = 1000;

pub struct ReadConfig {
    pub from_block: BlockHeight,
    pub to_block: BlockHeight,
    pub path: String,
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


pub async fn start(config: ReadConfig, blocks_sink: mpsc::Sender<XBlock>) {
    let mut current_file = "".to_string();
    let mut blocks = HashMap::new();

    for block_height in config.from_block..config.to_block {
        tracing::log::debug!(target: PROJECT_ID, "Processing block: {}", block_height);
        let rounded_padded_block_height = format!("{:0>12}", block_height / SAVE_EVERY_N * SAVE_EVERY_N);
        let padded_block_height = format!("{:0>12}", block_height);
        let file_path = format!(
            "{}/{}/{}.tgz",
            config.path,
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
            blocks_sink.send(block).await.unwrap();
        } else {
            tracing::log::debug!(target: PROJECT_ID, "Block not found: {}", block_height);
        }
    }
}

pub fn streamer(config: ReadConfig) -> mpsc::Receiver<XBlock> {
    let (sender, receiver) = mpsc::channel(100);
    tokio::spawn(start(config, sender));
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
        path: env::var("BLOCKS_DATA_PATH").expect("BLOCKS_DATA_PATH is required")
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
