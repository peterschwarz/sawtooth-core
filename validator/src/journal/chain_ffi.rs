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
use cpython;
use cpython::{FromPyObject, ObjectProtocol, PyDict, PyList, PyObject, PyResult, Python,
              PythonObject, ToPyObject};
use journal::chain::*;
use py_ffi;
use std::ffi::CStr;
use std::mem::transmute;
use std::os::raw::{c_char, c_void};

use batch::Batch;
use block::Block;
use transaction::Transaction;

use protobuf;
use protobuf::Message;

use proto::batch::Batch as ProtoBatch;
use proto::batch::BatchHeader;
use proto::block::Block as ProtoBlock;
use proto::block::BlockHeader;
use proto::transaction::Transaction as ProtoTxn;
use proto::transaction::TransactionHeader;
use proto::transaction_receipt::TransactionReceipt;

#[repr(u32)]
#[derive(Debug)]
pub enum ErrorCode {
    Success = 0,
    NullPointerProvided = 0x01,
    InvalidDataDir = 0x02,
    InvalidPythonObject = 0x03,
    InvalidBlockId = 0x04,
}

#[no_mangle]
pub extern "C" fn chain_controller_new(
    block_store: *mut py_ffi::PyObject,
    block_cache: *mut py_ffi::PyObject,
    block_validator: *mut py_ffi::PyObject,
    on_chain_updated: *mut py_ffi::PyObject,
    observers: *mut py_ffi::PyObject,
    data_directory: *const c_char,
    chain_controller_ptr: *mut *const c_void,
) -> ErrorCode {
    if block_store.is_null() {
        return ErrorCode::NullPointerProvided;
    }

    if block_cache.is_null() {
        return ErrorCode::NullPointerProvided;
    }

    if block_validator.is_null() {
        return ErrorCode::NullPointerProvided;
    }

    if on_chain_updated.is_null() {
        return ErrorCode::NullPointerProvided;
    }

    if observers.is_null() {
        return ErrorCode::NullPointerProvided;
    }

    if data_directory.is_null() {
        return ErrorCode::NullPointerProvided;
    }

    let data_dir = unsafe {
        match CStr::from_ptr(data_directory).to_str() {
            Ok(s) => s,
            Err(_) => return ErrorCode::InvalidDataDir,
        }
    };

    let py = unsafe { Python::assume_gil_acquired() };

    let py_block_store_reader = unsafe { PyObject::from_borrowed_ptr(py, block_store) };
    let py_block_store_writer = unsafe { PyObject::from_borrowed_ptr(py, block_store) };
    let py_block_cache = unsafe { PyObject::from_borrowed_ptr(py, block_cache) };
    let py_block_validator = unsafe { PyObject::from_borrowed_ptr(py, block_validator) };
    let py_on_chain_updated = unsafe { PyObject::from_borrowed_ptr(py, on_chain_updated) };
    let py_observers = unsafe { PyObject::from_borrowed_ptr(py, observers) };

    let observer_wrappers = if let Ok(py_list) = py_observers.extract::<PyList>(py) {
        let mut res: Vec<Box<ChainObserver>> = Vec::with_capacity(py_list.len(py));
        py_list
            .iter(py)
            .for_each(|pyobj| res.push(Box::new(PyChainObserver::new(pyobj))));
        res
    } else {
        return ErrorCode::InvalidPythonObject;
    };

    let chain_controller = ChainController::new(
        PyBlockCache::new(py_block_cache),
        PyBlockValidator::new(py_block_validator),
        PyBlockStore::new(py_block_store_writer),
        Box::new(PyBlockStore::new(py_block_store_reader)),
        data_dir.into(),
        Box::new(PyChainHeadUpdateObserver::new(py_on_chain_updated)),
        observer_wrappers,
    );

    unsafe {
        *chain_controller_ptr = Box::into_raw(Box::new(chain_controller)) as *const c_void;
    }

    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn chain_controller_drop(chain_controller: *mut c_void) -> ErrorCode {
    if chain_controller.is_null() {
        return ErrorCode::NullPointerProvided;
    }

    unsafe { Box::from_raw(chain_controller) };
    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn chain_controller_start(chain_controller: *mut c_void) -> ErrorCode {
    if chain_controller.is_null() {
        return ErrorCode::NullPointerProvided;
    }

    unsafe {
        (*(chain_controller as *mut ChainController<PyBlockCache, PyBlockValidator, PyBlockStore>))
            .start();
    }
    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn chain_controller_stop(chain_controller: *mut c_void) -> ErrorCode {
    if chain_controller.is_null() {
        return ErrorCode::NullPointerProvided;
    }

    unsafe {
        (*(chain_controller as *mut ChainController<PyBlockCache, PyBlockValidator, PyBlockStore>))
            .stop();
    }
    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn chain_controller_has_block(
    chain_controller: *mut c_void,
    block_id: *const c_char,
    result: *mut bool,
) -> ErrorCode {
    if chain_controller.is_null() {
        return ErrorCode::NullPointerProvided;
    }

    let block_id = unsafe {
        if block_id.is_null() {
            return ErrorCode::NullPointerProvided;
        }
        match CStr::from_ptr(block_id).to_str() {
            Ok(s) => s,
            Err(_) => return ErrorCode::InvalidBlockId,
        }
    };

    unsafe {
        *result = (*(chain_controller
            as *mut ChainController<PyBlockCache, PyBlockValidator, PyBlockStore>))
            .has_block(block_id);
    }

    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn chain_controller_queue_block(
    chain_controller: *mut c_void,
    block: *mut py_ffi::PyObject,
) -> ErrorCode {
    if chain_controller.is_null() {
        return ErrorCode::NullPointerProvided;
    }

    let gil_guard = Python::acquire_gil();
    let py = gil_guard.python();

    let block = unsafe {
        if block.is_null() {
            return ErrorCode::NullPointerProvided;
        }
        match PyObject::from_borrowed_ptr(py, block).extract(py) {
            Ok(val) => val,
            Err(py_err) => {
                py_err.print(py);
                return ErrorCode::InvalidPythonObject;
            }
        }
    };
    unsafe {
        println!("Queuing {}", block);

        let controller = (*(chain_controller
            as *mut ChainController<PyBlockCache, PyBlockValidator, PyBlockStore>))
            .light_clone();

        py.allow_threads(move || {
            controller.queue_block(block);
        });
    }

    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn chain_controller_chain_head(
    chain_controller: *mut c_void,
    block: *mut *const py_ffi::PyObject,
) -> ErrorCode {
    unsafe {
        let chain_head = (*(chain_controller
            as *mut ChainController<PyBlockCache, PyBlockValidator, PyBlockStore>))
            .chain_head();

        let gil_guard = Python::acquire_gil();
        let py = gil_guard.python();

        *block = chain_head.to_py_object(py).steal_ptr();
    }
    ErrorCode::Success
}

struct PyBlockCache {
    py_block_cache: PyObject,
}

impl PyBlockCache {
    fn new(py_block_cache: PyObject) -> Self {
        PyBlockCache { py_block_cache }
    }
}

impl BlockCache for PyBlockCache {
    fn contains(&self, block_id: &str) -> bool {
        let gil_guard = Python::acquire_gil();
        let py = gil_guard.python();

        match self.py_block_cache
            .call_method(py, "__contains__", (block_id,), None)
        {
            Err(py_err) => {
                py_err.print(py);
                false
            }
            Ok(py_bool) => py_bool.extract(py).expect("Unable to extract boolean"),
        }
    }

    fn put(&mut self, block: Block) {
        let gil_guard = Python::acquire_gil();
        let py = gil_guard.python();

        match self.py_block_cache.call_method(
            py,
            "__setitem__",
            (block.header_signature.clone(), block),
            None,
        ) {
            Err(py_err) => {
                py_err.print(py);
                ()
            }
            Ok(_) => (),
        }
    }

    fn get(&self, block_id: &str) -> Option<Block> {
        let gil_guard = Python::acquire_gil();
        let py = gil_guard.python();

        match self.py_block_cache
            .call_method(py, "__getitem__", (block_id,), None)
        {
            Err(py_err) => {
                // This is probably a key error, so we can return none
                None
            }
            Ok(res) => Some(res.extract(py).expect("Unable to extract block")),
        }
    }
}

struct PyBlockValidator {
    py_block_validator: PyObject,
}

impl PyBlockValidator {
    fn new(py_block_validator: PyObject) -> Self {
        PyBlockValidator { py_block_validator }
    }

    fn query_block_status(&self, fn_name: &str, block_id: &str) -> bool {
        let gil_guard = Python::acquire_gil();
        let py = gil_guard.python();

        match self.py_block_validator
            .call_method(py, fn_name, (block_id,), None)
        {
            Err(py_err) => {
                // Presumably a KeyError, so no
                py_err.print(py);
                false
            }
            Ok(py_bool) => py_bool.extract(py).expect("Unable to extract boolean"),
        }
    }
}

impl BlockValidator for PyBlockValidator {
    fn in_process(&self, block_id: &str) -> bool {
        self.query_block_status("in_process", block_id)
    }

    fn in_pending(&self, block_id: &str) -> bool {
        self.query_block_status("in_pending", block_id)
    }

    fn validate_block(&self, block: Block) -> Result<(), ValidationError> {
        let gil_guard = Python::acquire_gil();
        let py = gil_guard.python();

        self.py_block_validator
            .call_method(py, "validate_block", (block,), None)
            .map(|_| ())
            .map_err(|py_err| {
                ValidationError::BlockValidationFailure(py_err.get_type(py).name(py).into_owned())
            })
    }

    fn submit_blocks_for_verification<F>(&self, blocks: &[Block], on_block_validated: F)
    where
        F: FnMut(bool, BlockValidationResult),
    {
        let gil_guard = Python::acquire_gil();
        let py = gil_guard.python();

        let p: *mut FnMut(bool, BlockValidationResult) = Box::into_raw(Box::new(on_block_validated));
        let callback_ptr: u128 = unsafe { transmute(p) };

        // A bit of a hack, but will need to submit a patch to cpython to support To/FromPyObject
        // for u128, which is only in stable as of rustc 1.26
        let ptr_str = format!("{:x}", callback_ptr);

        let dict = PyDict::new(py);
        dict.set_item(py, "ptr", ptr_str).unwrap();
        dict.set_item(
            py,
            "callback",
            py_fn!(
                py,
                execute_callback(callback_ptr: String, can_commit: bool, result: PyObject)
            ),
        ).unwrap();
        let py_callback = py.eval(
            "lambda can_commit, result: f(ptr, can_commit, result)",
            None,
            Some(&dict),
        ).unwrap();

        self.py_block_validator
            .call_method(
                py,
                "submit_blocks_for_verification",
                (blocks, py_callback),
                None,
            )
            .map(|_| ())
            .map_err(|py_err| {
                py_err.print(py);
                ()
            })
            .unwrap_or(())
    }
}

fn execute_callback(
    py: Python,
    fn_ptr: String,
    can_commit: bool,
    result: PyObject,
) -> PyResult<i32> {
    let validation_results: BlockValidationResult = result.extract(py).unwrap();
    let fn_ptr = u128::from_str_radix(&fn_ptr, 16)
        .expect("String corruption occurred while crossing the rust-python frontier");
    py.allow_threads(move || {
        let mut callback = unsafe {
            Box::from_raw(transmute::<u128, *mut FnMut(bool, BlockValidationResult)>(
                fn_ptr,
            ))
        };
        callback(can_commit, validation_results);
    });

    Ok(0)
}

struct PyBlockStore {
    py_block_store: PyObject,
}

impl PyBlockStore {
    fn new(py_block_store: PyObject) -> Self {
        PyBlockStore { py_block_store }
    }
}

impl ChainWriter for PyBlockStore {
    fn update_chain(
        &mut self,
        new_chain: &[Block],
        old_chain: &[Block],
    ) -> Result<(), ChainControllerError> {
        let gil_guard = Python::acquire_gil();
        let py = gil_guard.python();

        self.py_block_store
            .call_method(py, "update_chain", (new_chain, old_chain), None)
            .map(|_| ())
            .map_err(|py_err| {
                py_err.print(py);
                ChainControllerError::ChainUpdateError(
                    "An error occurred while executing update_chain".into(),
                )
            })
    }
}

impl ChainReader for PyBlockStore {
    fn chain_head(&self) -> Result<Option<Block>, ChainReadError> {
        let gil_guard = Python::acquire_gil();
        let py = gil_guard.python();

        self.py_block_store
            .getattr(py, "chain_head")
            .and_then(|result| result.extract(py))
            .map_err(|py_err| {
                py_err.print(py);
                ChainReadError::GeneralReadError("Unable to read from python block store".into())
            })
    }
}

struct PyChainObserver {
    py_observer: PyObject,
}

impl PyChainObserver {
    fn new(py_observer: PyObject) -> Self {
        PyChainObserver { py_observer }
    }
}

impl ChainObserver for PyChainObserver {
    fn chain_update(&mut self, block: &Block, receipts: &[&TransactionReceipt]) {
        let gil_guard = Python::acquire_gil();
        let py = gil_guard.python();

        self.py_observer
            .call_method(py, "chain_update", (block, receipts), None)
            .map(|_| ())
            .map_err(|py_err| {
                py_err.print(py);
                ()
            })
            .unwrap_or(())
    }
}

struct PyChainHeadUpdateObserver {
    py_on_chain_updated: PyObject,
}

impl PyChainHeadUpdateObserver {
    fn new(py_on_chain_updated: PyObject) -> Self {
        PyChainHeadUpdateObserver {
            py_on_chain_updated,
        }
    }
}

impl ChainHeadUpdateObserver for PyChainHeadUpdateObserver {
    fn on_chain_head_updated(
        &mut self,
        block: Block,
        committed_batches: Vec<Batch>,
        uncommitted_batches: Vec<Batch>,
    ) {
        let gil_guard = Python::acquire_gil();
        let py = gil_guard.python();

        self.py_on_chain_updated
            .call(py, (block, committed_batches, uncommitted_batches), None)
            .map(|_| ())
            .map_err(|py_err| {
                py_err.print(py);
                ()
            })
            .unwrap_or(())
    }
}

impl ToPyObject for Block {
    type ObjectType = PyObject;

    fn to_py_object(&self, py: Python) -> PyObject {
        let block_wrapper_mod = py.import("sawtooth_validator.journal.block_wrapper")
            .expect("Unable to import block_wrapper");
        let py_block_wrapper = block_wrapper_mod
            .get(py, "BlockWrapper")
            .expect("Unable to get BlockWrapper");
        let block_protobuf_mod = py.import("sawtooth_validator.protobuf.block_pb2")
            .expect("Unable to import block_pb2");
        let py_block = block_protobuf_mod
            .get(py, "Block")
            .expect("Unable to get Block");

        let mut proto_block = ProtoBlock::new();
        proto_block.set_header(self.header_bytes.clone());
        proto_block.set_header_signature(self.header_signature.clone());

        let proto_batches = self.batches
            .iter()
            .map(|batch| {
                let mut proto_batch = ProtoBatch::new();
                proto_batch.set_header(batch.header_bytes.clone());
                proto_batch.set_header_signature(batch.header_signature.clone());

                let proto_txns = batch
                    .transactions
                    .iter()
                    .map(|txn| {
                        let mut proto_txn = ProtoTxn::new();
                        proto_txn.set_header(txn.header_bytes.clone());
                        proto_txn.set_header_signature(txn.header_signature.clone());
                        proto_txn.set_payload(txn.payload.clone());
                        proto_txn
                    })
                    .collect::<Vec<_>>();

                proto_batch.set_transactions(protobuf::RepeatedField::from_vec(proto_txns));

                proto_batch
            })
            .collect::<Vec<_>>();

        proto_block.set_batches(protobuf::RepeatedField::from_vec(proto_batches));

        let block = py_block
            .call(py, cpython::NoArgs, None)
            .expect("Unable to instantiate Block");
        block
            .call_method(
                py,
                "ParseFromString",
                (cpython::PyBytes::new(py, &proto_block.write_to_bytes().unwrap()).into_object(),),
                None,
            )
            .expect("Unable to ParseFromString");

        py_block_wrapper
            .call_method(py, "wrap", (block,), None)
            .expect("Unable to call BlockWrapper.wrap")
    }
}

impl ToPyObject for TransactionReceipt {
    type ObjectType = PyObject;

    fn to_py_object(&self, py: Python) -> PyObject {
        let txn_receipt_protobuf_mod = py.import(
            "sawtooth_validator.protobuf.transaction_receipt_pb2",
        ).expect("Unable to import transaction_receipt_pb2");
        let py_txn_receipt_class = txn_receipt_protobuf_mod
            .get(py, "TransactionReceipt")
            .expect("Unable to get TransactionReceipt");

        let py_txn_receipt = py_txn_receipt_class
            .call(py, cpython::NoArgs, None)
            .expect("Unable to instantiate TransactionReceipt");
        py_txn_receipt
            .call_method(
                py,
                "ParseFromString",
                (cpython::PyBytes::new(py, &self.write_to_bytes().unwrap()).into_object(),),
                None,
            )
            .expect("Unable to ParseFromString")
    }
}

impl<'source> FromPyObject<'source> for Block {
    fn extract(py: Python, obj: &'source PyObject) -> cpython::PyResult<Self> {
        let py_block = obj.getattr(py, "block")
            .expect("Unable to get block from BlockWrapper");

        let bytes: Vec<u8> = py_block
            .call_method(py, "SerializeToString", cpython::NoArgs, None)?
            .extract(py)?;

        let mut proto_block: ProtoBlock = protobuf::parse_from_bytes(&bytes)
            .expect("Unable to parse protobuf bytes from python protobuf object");

        let mut block_header: BlockHeader = protobuf::parse_from_bytes(proto_block.get_header())
            .expect("Unable to parse protobuf bytes from python protobuf object");
        let block = Block {
            header_signature: proto_block.take_header_signature(),
            header_bytes: proto_block.take_header(),
            state_root_hash: block_header.take_state_root_hash(),
            consensus: block_header.take_consensus(),
            batch_ids: block_header.take_batch_ids().into_vec(),
            signer_public_key: block_header.take_signer_public_key(),
            previous_block_id: block_header.take_previous_block_id(),
            block_num: block_header.get_block_num(),

            batches: proto_block
                .take_batches()
                .iter_mut()
                .map(proto_batch_to_batch)
                .collect(),
        };

        Ok(block)
    }
}

fn proto_batch_to_batch(proto_batch: &mut ProtoBatch) -> Batch {
    let mut batch_header: BatchHeader = protobuf::parse_from_bytes(proto_batch.get_header())
        .expect("Unable to parse protobuf bytes from python protobuf object");
    Batch {
        header_signature: proto_batch.take_header_signature(),
        header_bytes: proto_batch.take_header(),
        signer_public_key: batch_header.take_signer_public_key(),
        transaction_ids: batch_header.take_transaction_ids().into_vec(),
        trace: proto_batch.get_trace(),

        transactions: proto_batch
            .take_transactions()
            .iter_mut()
            .map(proto_txn_to_txn)
            .collect(),
    }
}

fn proto_txn_to_txn(proto_txn: &mut ProtoTxn) -> Transaction {
    let mut txn_header: TransactionHeader = protobuf::parse_from_bytes(proto_txn.get_header())
        .expect("Unable to parse protobuf bytes from python protobuf object");

    Transaction {
        header_signature: proto_txn.take_header_signature(),
        header_bytes: proto_txn.take_header(),
        payload: proto_txn.take_payload(),
        batcher_public_key: txn_header.take_batcher_public_key(),
        dependencies: txn_header.take_dependencies().into_vec(),
        family_name: txn_header.take_family_name(),
        family_version: txn_header.take_family_version(),
        inputs: txn_header.take_inputs().into_vec(),
        outputs: txn_header.take_outputs().into_vec(),
        nonce: txn_header.take_nonce(),
        payload_sha512: txn_header.take_payload_sha512(),
        signer_public_key: txn_header.take_signer_public_key(),
    }
}

impl<'source> FromPyObject<'source> for BlockValidationResult {
    fn extract(py: Python, obj: &'source PyObject) -> cpython::PyResult<Self> {
        Ok(BlockValidationResult {
            chain_head: obj.getattr(py, "chain_head")?.extract(py)?,
            block: obj.getattr(py, "block")?.extract(py)?,
            execution_results: vec![], // obj.getattr(py, "execution_results")?.extract(py)?,
            new_chain: obj.getattr(py, "new_chain")?.extract(py)?,
            current_chain: obj.getattr(py, "current_chain")?.extract(py)?,

            committed_batches: obj.getattr(py, "committed_batches")?.extract(py)?,
            uncommitted_batches: obj.getattr(py, "uncommitted_batches")?.extract(py)?,
        })
    }
}
