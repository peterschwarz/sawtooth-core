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
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvError;
use std::thread;

use batch::Batch;
use block::Block;
use journal;

use cpython::PyObject;

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
    fn chain_update(&mut self, block: &Block, receipts: &[TransactionReceipt]);
}

pub trait BlockCache {
    fn contains(&self, block_id: &str) -> bool;

    fn put(&mut self, block: Block);

    fn get(&self, block_id: &str) -> Option<Block>;
}

struct PyBlockCache {
    py_block_cache: PyObject,
}

impl BlockCache for PyBlockCache {
    fn contains(&self, block_id: &str) -> bool {
        false
    }
    fn put(&mut self, block: Block) {}

    fn get(&self, block_id: &str) -> Option<Block> {
        None
    }
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

pub trait ChainWriter {
    fn update_chain(&mut self, blocks: &[Block]) -> Result<(), ChainControllerError>;
}

pub struct ChainController<BC: BlockCache, BV: BlockValidator, CW: ChainWriter> {
    block_cache: BC,
    block_validator: BV,
    chain_writer: CW,
    chain_head: Option<String>,
    chain_id_manager: ChainIdManager,
    observers: Vec<Box<ChainObserver>>,
    lock: RwLock<usize>,
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
            block_cache,
            block_validator,
            chain_writer,
            chain_id_manager: ChainIdManager::new(data_dir),
            observers,
            lock: RwLock::new(0),
            chain_head: None,
        }
    }

    fn on_block_received(&mut self, block: Block) -> Result<(), ChainControllerError> {
        self.lock.write().unwrap();

        if self.has_block_no_lock(&block.header_signature) {
            return Ok(());
        }

        if self.chain_head.is_none() {
            if let Err(err) = self.set_genesis(block.clone()) {
                warn!(
                    "Unable to set chain head; genesis block {} is not valid: {:?}",
                    block.header_signature, err
                );
            }
            return Ok(());
        }

        self.block_cache.put(block.clone());
        self.submit_blocks_for_verification(&[block])?;
        Ok(())
    }

    fn has_block_no_lock(&self, block_id: &str) -> bool {
        self.block_cache.contains(block_id) || self.block_validator.in_process(block_id)
            || self.block_validator.in_pending(block_id)
    }

    pub fn has_block(&self, block_id: &str) -> bool {
        self.lock.read().unwrap();
        self.has_block_no_lock(block_id)
    }

    /// This is used by a non-genesis journal when it has received the
    /// genesis block from the genesis validator
    fn set_genesis(&mut self, block: Block) -> Result<(), ChainControllerError> {
        if block.previous_block_id == journal::NULL_BLOCK_IDENTIFIER {
            let chain_id = self.chain_id_manager.get_block_chain_id()?;
            if chain_id.as_ref().map(|block_id| block_id != &block.header_signature).unwrap_or(false) {
                warn!(
                    "Block id does not match block chain id {}. Ignoring initial chain head: {}",
                    chain_id.unwrap(),
                    block.header_signature
                );
            } else {
                self.block_validator.validate_block(block.clone())?;

                if chain_id.is_none() {
                    self.chain_id_manager
                        .save_block_chain_id(&block.header_signature)?;
                }

                self.chain_writer.update_chain(&[block.clone()])?;
                self.chain_head = Some(block.header_signature.clone());
                self.notify_on_chain_updated(block.clone());
            }
        }

        Ok(())
    }

    fn notify_on_chain_updated(&self, block: Block) {
        unimplemented!()
    }

    fn submit_blocks_for_verification(
        &mut self,
        blocks: &[Block],
    ) -> Result<(), ChainControllerError> {
        unimplemented!()
    }
}

struct ChainThread {
    block_queue: Receiver<Block>,
    exit: AtomicBool,
}

trait StopHandle {
    fn stop(&self);
}

struct ChainState {}

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
