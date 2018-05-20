/*
 * Copyright 2018 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ------------------------------------------------------------------------------
 */

use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::cell::RefCell;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvError;
use std::thread;

use batch::Batch;
use block::Block;
use journal;

use proto::transaction_receipt::TransactionReceipt;

#[derive(Debug)]
pub enum ChainControllerError {
    QueueRecvError(RecvError),
    ChainIdError(io::Error),
    BlockValidationError(ValidationError),
}

impl From<RecvError> for ChainControllerError {
    fn from(err: RecvError) -> Self {
        ChainControllerError::QueueRecvError(RecvError)
    }
}

impl From<io::Error> for ChainControllerError {
    fn from(err: io::Error) -> Self {
        ChainControllerError::ChainIdError(err)
    }
}

impl From<ValidationError> for ChainControllerError {
    fn from(err: ValidationError) -> Self {
        ChainControllerError::BlockValidationError(err)
    }
}

pub trait ChainObserver {
    fn chain_update(&mut self, block: &Block, receipts: &[&TransactionReceipt]);
}

pub trait BlockCache {
    fn contains(&self, block_id: &str) -> bool;

    fn put(&mut self, block: Block);

    fn get(&self, block_id: &str) -> Option<Block>;
}

#[derive(Debug)]
pub enum ValidationError {
    BlockValidationFailure(String),
}

pub trait BlockValidator {
    fn in_process(&self, block_id: &str) -> bool;
    fn in_pending(&self, block_id: &str) -> bool;
    fn validate_block(&self, block: Block) -> Result<(), ValidationError>;
}

// This should be in a validation module
pub struct BlockValidationResult {
    pub chain_head: Block,
    pub block: Block,

    pub execution_results: Vec<ExecutionResults>,

    pub new_chain: Vec<Block>,
    pub current_chain: Vec<Block>,

    pub committed_batches: Vec<Batch>,
    pub uncommitted_batches: Vec<Batch>
}

// This should be in a validation Module
pub struct ExecutionResults {
}

pub trait ChainWriter {
    fn update_chain(
        &mut self,
        new_chain: &[Block],
        old_chain: &[Block],
    ) -> Result<(), ChainControllerError>;
}

#[derive(Clone)]
struct ChainControllerState<BC: BlockCache, BV: BlockValidator, CW: ChainWriter> {
    block_cache: BC,
    block_validator: BV,
    chain_writer: CW,
    chain_head: Option<Block>,
    chain_id_manager: ChainIdManager,
    observers: Arc<RefCell<Vec<Box<ChainObserver>>>>,
}

#[derive(Clone)]
pub struct ChainController<BC: BlockCache, BV: BlockValidator, CW: ChainWriter> {
    state: Arc<RwLock<ChainControllerState<BC, BV, CW>>>,
}

impl<BC: BlockCache, BV: BlockValidator, CW: ChainWriter> ChainController<BC, BV, CW> {
    pub fn new(
        block_cache: BC,
        block_validator: BV,
        chain_writer: CW,
        data_dir: String,
        observers: Vec<Box<ChainObserver>>,
    ) -> Self {
        ChainController {
            state: Arc::new(RwLock::new(
                    ChainControllerState {
                        block_cache,
                        block_validator,
                        chain_writer,
                        chain_id_manager: ChainIdManager::new(data_dir),
                        observers: Arc::new(RefCell::new(observers)),
                        chain_head: None,
                    }))
        }
    }

    fn on_block_received(&mut self, block: Block) -> Result<(), ChainControllerError> {
        let mut state = self.state
            .write()
            .expect("No lock holder should have poisoned the lock");

        if has_block_no_lock(&state, &block.header_signature) {
            return Ok(());
        }

        if state.chain_head.is_none() {
            if let Err(err) = set_genesis(&mut state, block.clone()) {
                warn!(
                    "Unable to set chain head; genesis block {} is not valid: {:?}",
                    block.header_signature, err
                );
            }
            return Ok(());
        }

        state.block_cache.put(block.clone());
        submit_blocks_for_verification(&mut state, &[block])?;
        Ok(())
    }


    pub fn has_block(&self, block_id: &str) -> bool {
        let state = self.state.read().expect("No lock holder should have poisoned the lock");
        has_block_no_lock(&state, block_id)
    }

    fn on_block_validated(&mut self, commit_new_block: bool, result: BlockValidationResult) {
        let mut state = self.state
            .write()
            .expect("No lock holder should have poisoned the lock");

        let new_block = result.block;
        if state.chain_head
            .as_ref()
            .map(|block| block.header_signature != new_block.header_signature)
            .unwrap_or(false)
        {
            info!(
                "Chain head updated from {} to {} while processing block {}",
                result.chain_head,
                state.chain_head.as_ref().unwrap(),
                new_block
            );
            debug!("Verify block again: {}", new_block);
            submit_blocks_for_verification(&mut state, &[new_block]);
        } else if commit_new_block {
            state.chain_head = Some(new_block);

            state.chain_writer
                .update_chain(&result.new_chain, &result.current_chain);

            info!(
                "Chain head updated to {}",
                state.chain_head.as_ref().unwrap()
            );

            let chain_head = state.chain_head.clone().unwrap();
            notify_on_chain_updated(
                &mut state,
                chain_head,
                result.committed_batches,
                result.uncommitted_batches,
            );

            state.chain_head.as_ref().map(|block| {
                block.batches.iter().for_each(|batch| {
                    if batch.trace {
                        debug!(
                            "TRACE: {}: ChainController.on_block_validated",
                            batch.header_signature
                        )
                    }
                })
            });

            let mut new_chain = result.new_chain;
            new_chain.reverse();

            for block in new_chain {
                let receipts: Vec<TransactionReceipt> = make_receipts(&result.execution_results);
                for observer in state.observers.borrow_mut().iter_mut() {
                    observer.chain_update(&block, &receipts.iter().collect::<Vec<_>>());
                }
            }
        } else {
            info!("Rejected new chain head: {}", new_block);
        }
    }

}

fn has_block_no_lock<BC: BlockCache, BV: BlockValidator, CW: ChainWriter>(state: &ChainControllerState<BC, BV, CW>, block_id: &str) -> bool {
    state.block_cache.contains(block_id) || state.block_validator.in_process(block_id)
        || state.block_validator.in_pending(block_id)
}

/// This is used by a non-genesis journal when it has received the
/// genesis block from the genesis validator
fn set_genesis<BC: BlockCache, BV: BlockValidator, CW: ChainWriter>(state: &mut ChainControllerState<BC, BV, CW>, block: Block) -> Result<(), ChainControllerError> {
    if block.previous_block_id == journal::NULL_BLOCK_IDENTIFIER {
        let chain_id = state.chain_id_manager.get_block_chain_id()?;
        if chain_id
            .as_ref()
            .map(|block_id| block_id != &block.header_signature)
            .unwrap_or(false)
        {
            warn!(
                "Block id does not match block chain id {}. Ignoring initial chain head: {}",
                chain_id.unwrap(),
                block.header_signature
            );
        } else {
            state.block_validator.validate_block(block.clone())?;

            if chain_id.is_none() {
                state.chain_id_manager
                    .save_block_chain_id(&block.header_signature)?;
            }

            state.chain_writer.update_chain(&[block.clone()], &[])?;
            state.chain_head = Some(block.clone());
            notify_on_chain_updated(state, block.clone(), vec![], vec![]);
        }
    }

    Ok(())
}

fn submit_blocks_for_verification<BC: BlockCache, BV: BlockValidator, CW: ChainWriter>(
    state: &mut ChainControllerState<BC, BV, CW>,
    blocks: &[Block],
) -> Result<(), ChainControllerError> {
    unimplemented!()
}

fn notify_on_chain_updated<BC: BlockCache, BV: BlockValidator, CW: ChainWriter>(
    state: &mut ChainControllerState<BC, BV, CW>,
    block: Block,
    committed_batches: Vec<Batch>,
    uncommitted_batches: Vec<Batch>,
) {
    unimplemented!()
}


fn make_receipts(result: &[ExecutionResults]) -> Vec<TransactionReceipt> {
    unimplemented!()
}

struct ChainThread {
    block_queue: Receiver<Block>,
    exit: AtomicBool,
}

trait StopHandle {
    fn stop(&self);
}

impl ChainThread {
    fn new(block_queue: Receiver<Block>) -> Self {
        ChainThread {
            block_queue,
            exit: AtomicBool::new(false),
        }
    }

    fn start(&mut self) {
        /*
        thread::spawn(move || {
            if let Err(err) = self.run() {
                error!("Error occurred during ChainController loop: {:?}", err);
            }
        });
        */
    }

    fn run(&mut self) -> Result<(), ChainControllerError> {
        loop {
            let block = self.block_queue.recv()?;
            // self.chain_controller.on_block_received(block)?;

            if self.exit.load(Ordering::Relaxed) {
                break Ok(());
            }
        }
    }
}
impl StopHandle for ChainThread {
    fn stop(&self) {
        self.exit.store(true, Ordering::Relaxed)
    }
}

/// The ChainIdManager is in charge of of keeping track of the block-chain-id
/// stored in the data_dir.
#[derive(Clone, Debug)]
struct ChainIdManager {
    data_dir: String,
}

impl ChainIdManager {
    pub fn new(data_dir: String) -> Self {
        ChainIdManager { data_dir }
    }

    pub fn save_block_chain_id(&self, block_chain_id: &str) -> Result<(), io::Error> {
        debug!("Writing block chain id");

        let mut path = PathBuf::new();
        path.push(&self.data_dir);
        path.push("block-chain-id");

        let mut file = File::create(path)?;
        file.write_all(block_chain_id.as_bytes())
    }

    pub fn get_block_chain_id(&self) -> Result<Option<String>, io::Error> {
        let mut path = PathBuf::new();
        path.push(&self.data_dir);
        path.push("block-chain-id");

        match File::open(path) {
            Ok(mut file) => {
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                Ok(Some(contents))
            }
            Err(ref err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }
}

#[cfg(tests)]
mod tests {}
