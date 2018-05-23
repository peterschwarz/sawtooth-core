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
use std::marker::Send;
use std::marker::Sync;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::Sender;
use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

use batch::Batch;
use block::Block;
use journal;

use proto::transaction_receipt::TransactionReceipt;

#[derive(Debug)]
pub enum ChainControllerError {
    QueueRecvError(RecvError),
    ChainIdError(io::Error),
    ChainUpdateError(String),
    BlockValidationError(ValidationError),
}

impl From<RecvError> for ChainControllerError {
    fn from(err: RecvError) -> Self {
        ChainControllerError::QueueRecvError(err)
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

pub trait ChainObserver: Send + Sync {
    fn chain_update(&mut self, block: &Block, receipts: &[&TransactionReceipt]);
}

pub trait ChainHeadUpdateObserver: Send + Sync {
    /// Called when the chain head has updated.
    ///
    /// Args:
    ///     block: the new chain head
    ///     committed_batches: all of the batches that have been committed
    ///         on the given fork. This may be across multiple blocks.
    ///     uncommitted_batches: all of the batches that have been uncommitted
    ///         from the previous fork, if one was dropped.
    fn on_chain_head_updated(
        &mut self,
        block: Block,
        committed_batches: Vec<Batch>,
        uncommitted_batches: Vec<Batch>,
    );
}

pub trait BlockCache: Send + Sync {
    fn contains(&self, block_id: &str) -> bool;

    fn put(&mut self, block: Block);

    fn get(&self, block_id: &str) -> Option<Block>;
}

#[derive(Debug)]
pub enum ChainReadError {
    GeneralReadError(String),
}

pub trait ChainReader: Send + Sync {
    fn chain_head(&self) -> Result<Option<Block>, ChainReadError>;
}

#[derive(Debug)]
pub enum ValidationError {
    BlockValidationFailure(String),
}

pub trait BlockValidator: Send + Sync {
    fn in_process(&self, block_id: &str) -> bool;
    fn in_pending(&self, block_id: &str) -> bool;
    fn validate_block(&self, block: Block) -> Result<(), ValidationError>;

    fn submit_blocks_for_verification<F>(&self, blocks: &[Block], on_block_validated: F)
    where
        F: FnOnce(bool, BlockValidationResult);
}

// This should be in a validation module
pub struct BlockValidationResult {
    pub chain_head: Block,
    pub block: Block,

    pub execution_results: Vec<TransactionResult>,

    pub new_chain: Vec<Block>,
    pub current_chain: Vec<Block>,

    pub committed_batches: Vec<Batch>,
    pub uncommitted_batches: Vec<Batch>,
}

// This should be in a validation Module
pub struct TransactionResult {
    // pub signature: String,
    // pub is_valid: bool,
    // pub state_hash: String,
    // pub state_changes: Vec<StateChange>,
    // pub events: Vec<Event>,
    // pub data: Vec<(String, Vec<u8>)>,
    // pub error_message: String,
    // pub error_data: Vec<u8>,
}

pub trait ChainWriter: Send + Sync {
    fn update_chain(
        &mut self,
        new_chain: &[Block],
        old_chain: &[Block],
    ) -> Result<(), ChainControllerError>;
}

struct ChainControllerState<BC: BlockCache, BV: BlockValidator, CW: ChainWriter> {
    block_cache: BC,
    block_validator: BV,
    chain_writer: CW,
    chain_head: Option<Block>,
    chain_id_manager: ChainIdManager,
    chain_head_update_observer: Box<ChainHeadUpdateObserver>,
    observers: Vec<Box<ChainObserver>>,
}

#[derive(Clone)]
pub struct ChainController<BC: BlockCache, BV: BlockValidator, CW: ChainWriter> {
    state: Arc<RwLock<ChainControllerState<BC, BV, CW>>>,
    stop_handle: Arc<Mutex<Option<ChainThreadStopHandle>>>,
    block_queue_sender: Option<Sender<Block>>,
}

impl<BC: BlockCache + 'static, BV: BlockValidator + 'static, CW: ChainWriter + 'static>
    ChainController<BC, BV, CW>
{
    pub fn new(
        block_cache: BC,
        block_validator: BV,
        chain_writer: CW,
        chain_reader: Box<ChainReader>,
        data_dir: String,
        chain_head_update_observer: Box<ChainHeadUpdateObserver>,
        observers: Vec<Box<ChainObserver>>,
    ) -> Self {
        let chain_head = chain_reader
            .chain_head()
            .expect("Invalid block store. Head of the block chain cannot be determined");

        if chain_head.is_some() {
            info!(
                "Chain controller initialized with chain head: {}",
                chain_head.as_ref().unwrap()
            );
        }

        ChainController {
            state: Arc::new(RwLock::new(ChainControllerState {
                block_cache,
                block_validator,
                chain_writer,
                chain_id_manager: ChainIdManager::new(data_dir),
                chain_head_update_observer,
                observers,
                chain_head,
            })),
            stop_handle: Arc::new(Mutex::new(None)),
            block_queue_sender: None,
        }
    }

    pub fn chain_head(&self) -> Option<Block> {
        let mut state = self.state
            .write()
            .expect("No lock holder should have poisoned the lock");

        state.chain_head.clone()
    }

    pub fn on_block_received(&mut self, block: Block) -> Result<(), ChainControllerError> {
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
        self.submit_blocks_for_verification(&state.block_validator, &[block])?;
        Ok(())
    }

    pub fn has_block(&self, block_id: &str) -> bool {
        let state = self.state
            .read()
            .expect("No lock holder should have poisoned the lock");
        has_block_no_lock(&state, block_id)
    }

    fn on_block_validated(&mut self, commit_new_block: bool, result: BlockValidationResult) {
        let mut state = self.state
            .write()
            .expect("No lock holder should have poisoned the lock");

        let new_block = result.block;
        if state
            .chain_head
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
            if let Err(err) =
                self.submit_blocks_for_verification(&state.block_validator, &[new_block])
            {
                error!("Unable to submit block for verification: {:?}", err);
            }
        } else if commit_new_block {
            state.chain_head = Some(new_block);

            if let Err(err) = state
                .chain_writer
                .update_chain(&result.new_chain, &result.current_chain)
            {
                error!("Unable to update chain {:?}", err);
                return;
            }

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
                for observer in state.observers.iter_mut() {
                    observer.chain_update(&block, &receipts.iter().collect::<Vec<_>>());
                }
            }
        } else {
            info!("Rejected new chain head: {}", new_block);
        }
    }

    pub fn light_clone(&self) -> Self {
        ChainController {
            state: self.state.clone(),
            // This instance doesn't share the stop handle: it's not a
            // publicly accessible instance
            stop_handle: Arc::new(Mutex::new(None)),
            block_queue_sender: self.block_queue_sender.clone(),
        }
    }

    fn submit_blocks_for_verification(
        &self,
        block_validator: &BV,
        blocks: &[Block],
    ) -> Result<(), ChainControllerError> {
        let mut chain_controller_copy = self.light_clone();
        block_validator.submit_blocks_for_verification(blocks, move |commit_new_block, result| {
            chain_controller_copy.on_block_validated(commit_new_block, result)
        });
        Ok(())
    }

    pub fn queue_block(&self, block: Block) {
        if self.block_queue_sender.is_some() {
            let sender = self.block_queue_sender.clone();
            thread::spawn(move || {
                if let Err(err) = sender.as_ref().unwrap().send(block) {
                    error!("Unable to add block to block queue: {}", err);
                }
            });
        }
    }

    pub fn start(&mut self) {
        let mut stop_handle = self.stop_handle.lock().unwrap();
        if stop_handle.is_none() {
            let (block_queue_sender, block_queue_receiver) = channel();
            self.block_queue_sender = Some(block_queue_sender);
            let thread_chain_controller = self.light_clone();
            let exit_flag = Arc::new(AtomicBool::new(false));
            let mut chain_thread = ChainThread::new(
                thread_chain_controller,
                block_queue_receiver,
                exit_flag.clone(),
            );
            *stop_handle = Some(ChainThreadStopHandle::new(exit_flag));
            thread::spawn(move || {
                if let Err(err) = chain_thread.run() {
                    error!("Error occurred during ChainController loop: {:?}", err);
                }
            });
        }
    }

    pub fn stop(&mut self) {
        let mut stop_handle = self.stop_handle.lock().unwrap();
        if stop_handle.is_some() {
            let handle: ChainThreadStopHandle = stop_handle.take().unwrap();
            handle.stop();
        }
    }
}

fn has_block_no_lock<BC: BlockCache, BV: BlockValidator, CW: ChainWriter>(
    state: &ChainControllerState<BC, BV, CW>,
    block_id: &str,
) -> bool {
    state.block_cache.contains(block_id) || state.block_validator.in_process(block_id)
        || state.block_validator.in_pending(block_id)
}

/// This is used by a non-genesis journal when it has received the
/// genesis block from the genesis validator
fn set_genesis<BC: BlockCache, BV: BlockValidator, CW: ChainWriter>(
    state: &mut ChainControllerState<BC, BV, CW>,
    block: Block,
) -> Result<(), ChainControllerError> {
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
                state
                    .chain_id_manager
                    .save_block_chain_id(&block.header_signature)?;
            }

            state.chain_writer.update_chain(&[block.clone()], &[])?;
            state.chain_head = Some(block.clone());
            notify_on_chain_updated(state, block.clone(), vec![], vec![]);
        }
    }

    Ok(())
}

fn notify_on_chain_updated<BC: BlockCache, BV: BlockValidator, CW: ChainWriter>(
    state: &mut ChainControllerState<BC, BV, CW>,
    block: Block,
    committed_batches: Vec<Batch>,
    uncommitted_batches: Vec<Batch>,
) {
    state.chain_head_update_observer.on_chain_head_updated(
        block,
        committed_batches,
        uncommitted_batches,
    );
}

fn make_receipts(result: &[TransactionResult]) -> Vec<TransactionReceipt> {
    unimplemented!()
}

struct ChainThread<BC: BlockCache, BV: BlockValidator, CW: ChainWriter> {
    chain_controller: ChainController<BC, BV, CW>,
    block_queue: Receiver<Block>,
    exit: Arc<AtomicBool>,
}

trait StopHandle: Clone {
    fn stop(&self);
}

impl<BC: BlockCache + 'static, BV: BlockValidator + 'static, CW: ChainWriter + 'static>
    ChainThread<BC, BV, CW>
{
    fn new(
        chain_controller: ChainController<BC, BV, CW>,
        block_queue: Receiver<Block>,
        exit_flag: Arc<AtomicBool>,
    ) -> Self {
        ChainThread {
            chain_controller,
            block_queue,
            exit: exit_flag,
        }
    }

    fn run(&mut self) -> Result<(), ChainControllerError> {
        loop {
            let block = match self.block_queue.recv_timeout(Duration::from_secs(1)) {
                Err(_) => continue,
                Ok(block) => block,
            };
            println!("Pulled {} from queue", block);
            self.chain_controller.on_block_received(block)?;

            if self.exit.load(Ordering::Relaxed) {
                println!("Shutting down Chain Thread...");
                break Ok(());
            }
        }
    }
}

#[derive(Clone)]
struct ChainThreadStopHandle {
    exit: Arc<AtomicBool>,
}

impl ChainThreadStopHandle {
    fn new(exit_flag: Arc<AtomicBool>) -> Self {
        ChainThreadStopHandle { exit: exit_flag }
    }
}

impl StopHandle for ChainThreadStopHandle {
    fn stop(&self) {
        println!("Stopping chain thread");
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
