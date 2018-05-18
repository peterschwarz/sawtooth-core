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

use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time;

use rand;
use rand::Rng;

use sawtooth_sdk::consensus::{engine::*, service::Service};

pub struct DevmodeService {
    pub service: Box<Service>,
}

impl DevmodeService {
    pub fn new(service: Box<Service>) -> Self {
        DevmodeService { service }
    }

    fn get_chain_head(&mut self) -> Block {
        self.service
            .get_chain_head()
            .expect("Failed to get chain head")
    }

    fn get_block(&mut self, block_id: BlockId) -> Block {
        self.service
            .get_blocks(vec![block_id])
            .expect("Failed to get block")[0]
            .clone()
    }

    fn initialize_block(&mut self) {
        self.service
            .initialize_block(None)
            .expect("Failed to initialize");
    }

    fn finalize_block(&mut self) {
        self.service
            .finalize_block(Vec::from(&b"Devmode"[..]))
            .expect("Failed to finalize");
    }

    fn check_block(&mut self, block_id: BlockId) {
        self.service
            .check_blocks(vec![block_id])
            .expect("Failed to check block");
    }

    fn fail_block(&mut self, block_id: BlockId) {
        self.service
            .fail_block(block_id)
            .expect("Failed to fail block");
    }

    fn ignore_block(&mut self, block_id: BlockId) {
        self.service
            .ignore_block(block_id)
            .expect("Failed to ignore block")
    }

    fn commit_block(&mut self, block_id: BlockId) {
        self.service
            .commit_block(block_id)
            .expect("Failed to commit block");
    }

    fn cancel_block(&mut self) {
        self.service.cancel_block().expect("Failed to cancel block");
    }

    // Calculate the time to wait between publishing blocks. When in
    // doubt, pick 0.
    fn calculate_wait_time(&mut self, chain_head_id: BlockId) -> time::Duration {
        match self.service.get_settings(
            chain_head_id,
            vec![
                String::from("sawtooth.consensus.min_wait_time"),
                String::from("sawtooth.consensus.max_wait_time"),
            ],
        ) {
            Ok(setting_strings) => {
                let ints: Vec<u64> = setting_strings
                    .iter()
                    .map(|string| string.parse::<u64>())
                    .map(|result| result.unwrap_or(0))
                    .collect();

                let min_wait_time: u64 = ints[0];
                let max_wait_time: u64 = ints[1];

                if min_wait_time > max_wait_time {
                    return time::Duration::new(0, 0);
                }

                let wait_time = rand::thread_rng().gen_range(min_wait_time, max_wait_time);

                time::Duration::new(wait_time, 0)
            }
            Err(_) => time::Duration::new(0, 0),
        }
    }
}

pub struct DevmodeEngine {
    exit: Exit,
}

impl DevmodeEngine {
    pub fn new() -> Self {
        DevmodeEngine { exit: Exit::new() }
    }
}

impl Engine for DevmodeEngine {
    fn start(&self, updates: Receiver<Update>, mut service: Box<Service>) {
        let mut service = DevmodeService::new(service);

        let mut chain_head = service.get_chain_head();
        let mut wait_time = service.calculate_wait_time(chain_head.block_id);
        let mut start = time::Instant::now();

        service.initialize_block();

        loop {
            if time::Instant::now().duration_since(start) > wait_time {
                service.finalize_block();

                chain_head = service.get_chain_head();
                wait_time = service.calculate_wait_time(chain_head.block_id);
                start = time::Instant::now();

                service.initialize_block();
            }

            // While the new block is getting built, keep validating
            // incoming new blocks.
            match updates.recv_timeout(time::Duration::from_millis(10)) {
                Ok(update) => match update {
                    Update::BlockNew(block) => {
                        if check_consensus(&block) {
                            service.check_block(block.block_id);
                        } else {
                            service.fail_block(block.block_id);
                        }
                    }

                    Update::BlockValid(block_id) => {
                        let block = service.get_block(block_id.clone());

                        // Advance the chain if possible.
                        if block.block_num > chain_head.block_num {
                            service.commit_block(block_id);
                        } else {
                            service.ignore_block(block_id)
                        }
                    }

                    // The chain head was updated, so abandon the
                    // block in progress and start a new one.
                    Update::BlockCommit(_) => {
                        service.cancel_block();

                        chain_head = service.get_chain_head();
                        wait_time = service.calculate_wait_time(chain_head.block_id);
                        start = time::Instant::now();

                        service.initialize_block();
                    }

                    // Devmode doesn't care about peer notifications
                    // or invalid blocks.
                    _ => {}
                },

                Err(RecvTimeoutError::Disconnected) => {
                    println!("disconnected");
                    break;
                }

                Err(RecvTimeoutError::Timeout) => {}
            }

            if self.exit.get() {
                break;
            }
        }
    }

    fn stop(&self) {
        self.exit.set();
    }

    fn version(&self) -> String {
        "0.1".into()
    }

    fn name(&self) -> String {
        "Devmode".into()
    }
}

fn check_consensus(block: &Block) -> bool {
    block.payload == b"Devmode"
}
