extern crate protobuf;
extern crate sawtooth_validator;
extern crate uuid;

use std::path::Path;
use std::time::{Duration, Instant};

use protobuf::Message;
use sawtooth_validator::block::Block;
use sawtooth_validator::database::lmdb::{self, DatabaseReader};
use sawtooth_validator::journal::block_store::BlockStore;
use sawtooth_validator::journal::commit_store::CommitStore;
use sawtooth_validator::journal::NULL_BLOCK_IDENTIFIER;
use sawtooth_validator::proto::block::BlockHeader;

pub fn main() -> Result<(), i32> {
    let lmdb_context = lmdb::LmdbContext::new(
        Path::new("/tmp/test_db.lmdb"),
        3,
        Some(128 * 1024usize.pow(2)),
    )
    .map_err(|e| {
        println!("Unable to create db context: {}", e);
        -1
    })?;
    let lmdb_db = lmdb::LmdbDatabase::new(
        lmdb_context,
        &["index_batch", "index_transaction", "index_block_num"],
    )
    .map_err(|e| {
        println!("Unable to create db: {}", e);
        -2
    })?;

    let mut commit_store = CommitStore::new(lmdb_db.clone());

    let mut chain_head_id = NULL_BLOCK_IDENTIFIER.to_string();

    let mut get_last_sum = Duration::default();
    let mut get_by_block_num = Duration::default();
    for i in 1u64..10000 {
        let block = make_block(i, chain_head_id);
        chain_head_id = block.header_signature.clone();

        commit_store.put(vec![block]).map_err(|e| {
            println!("Unable to put block: {:?}", e);
            -3
        })?;

        let start = Instant::now();

        // let indexed_chain_head = {
        //     let reader = lmdb_db.reader().map_err(|e| {
        //         println!("Unable to get reader: {}", e);
        //         -4
        //     })?;
        //     let cursor = reader.index_cursor("index_block_num").map_err(|e| {
        //         println!("Unable to create cursor: {}", e);
        //         -44
        //     })?;
        //     let (_, val) = cursor.last().expect("Should have returned a value");
        //     val
        // };

        let chain_head = commit_store.get_chain_head().map_err(|e| {
            println!("Unable to get chain head: {}", e);
            -4
        })?;
        get_last_sum += Instant::now().duration_since(start);
        // assert_eq!(indexed_chain_head, chain_head_id.as_bytes());

        let start = Instant::now();
        let _ = commit_store.get_by_block_num(i).map_err(|e| {
            println!("Unable to get by block num: {}", e);
            -5
        })?;
        get_by_block_num += Instant::now().duration_since(start);

        if i % 100 == 0 {
            println!(
                "Block {:>5}, average chain head get: {:>10} nanos; get by block num: {:>10} nanos",
                i,
                get_last_sum.as_nanos() / 100,
                get_by_block_num.as_nanos() / 100,
            );
            get_last_sum = Duration::default();
            get_by_block_num = Duration::default();
        }

        assert_eq!(chain_head.header_signature, chain_head_id);
    }

    Ok(())
}

fn make_block(block_num: u64, previous_block_id: String) -> Block {
    let mut block_header = BlockHeader::new();
    block_header.set_state_root_hash(uuid::Uuid::new_v4().to_string());
    block_header.set_signer_public_key("test_value".into());
    block_header.set_previous_block_id(previous_block_id.clone());
    block_header.set_block_num(block_num);

    Block {
        header_signature: uuid::Uuid::new_v4().to_string(),
        state_root_hash: block_header.state_root_hash.clone(),
        signer_public_key: block_header.signer_public_key.clone(),
        previous_block_id,
        block_num,
        batches: vec![],
        consensus: vec![],
        batch_ids: vec![],

        header_bytes: block_header.write_to_bytes().unwrap(),
    }
}
