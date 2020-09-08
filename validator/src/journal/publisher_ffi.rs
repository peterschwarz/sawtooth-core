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
use py_ffi;
use std::ffi::CStr;
use std::mem;
use std::os::raw::{c_char, c_void};
use std::slice;

use cpython::{ObjectProtocol, PyClone, PyList, PyObject, Python};
use sawtooth::journal::publisher::{
    BatchObserver, BatchSubmitter, BlockPublisher, BlockPublisherError,
};
use sawtooth::journal::{block_manager::BlockManager, commit_store::CommitStore};
use sawtooth::protocol::block::BlockPair;
use sawtooth::state::state_view_factory::StateViewFactory;
use transact::protocol::batch::Batch;

use crate::py_object_wrapper::PyObjectWrapper;
use ffi::py_import_class;

lazy_static! {
    static ref PY_BATCH_PUBLISHER_CLASS: PyObject = py_import_class(
        "sawtooth_validator.journal.consensus.batch_publisher",
        "BatchPublisher"
    );
}

#[repr(u32)]
#[derive(Debug)]
pub enum ErrorCode {
    Success = 0,
    NullPointerProvided = 0x01,
    InvalidInput = 0x02,
    BlockInProgress = 0x03,
    BlockNotInitialized = 0x04,
    BlockEmpty = 0x05,
    MissingPredecessor = 0x07,
    Internal = 0x08,
}

macro_rules! check_null {
    ($($arg:expr) , *) => {
        $(if $arg.is_null() { return ErrorCode::NullPointerProvided; })*
    }
}

#[no_mangle]
pub unsafe extern "C" fn block_publisher_new(
    commit_store_ptr: *mut c_void,
    block_manager_ptr: *const c_void,
    transaction_executor_ptr: *mut py_ffi::PyObject,
    state_view_factory_ptr: *const c_void,
    block_sender_ptr: *mut py_ffi::PyObject,
    batch_sender_ptr: *mut py_ffi::PyObject,
    chain_head_ptr: *mut py_ffi::PyObject,
    identity_signer_ptr: *mut py_ffi::PyObject,
    data_dir_ptr: *mut py_ffi::PyObject,
    config_dir_ptr: *mut py_ffi::PyObject,
    permission_verifier_ptr: *mut py_ffi::PyObject,
    batch_observers_ptr: *mut py_ffi::PyObject,
    batch_injector_factory_ptr: *mut py_ffi::PyObject,
    block_publisher_ptr: *mut *const c_void,
) -> ErrorCode {
    check_null!(
        commit_store_ptr,
        block_manager_ptr,
        transaction_executor_ptr,
        state_view_factory_ptr,
        block_sender_ptr,
        batch_sender_ptr,
        chain_head_ptr,
        identity_signer_ptr,
        data_dir_ptr,
        config_dir_ptr,
        permission_verifier_ptr,
        batch_observers_ptr,
        batch_injector_factory_ptr
    );

    let py = Python::assume_gil_acquired();

    let commit_store = (*(commit_store_ptr as *mut CommitStore)).clone();
    let block_manager = (*(block_manager_ptr as *mut BlockManager)).clone();
    let transaction_executor = PyObject::from_borrowed_ptr(py, transaction_executor_ptr);
    let state_view_factory = (state_view_factory_ptr as *const StateViewFactory)
        .as_ref()
        .unwrap();
    let block_sender = PyObject::from_borrowed_ptr(py, block_sender_ptr);
    let batch_sender = PyObject::from_borrowed_ptr(py, batch_sender_ptr);
    let chain_head = PyObject::from_borrowed_ptr(py, chain_head_ptr);
    let identity_signer = PyObject::from_borrowed_ptr(py, identity_signer_ptr);
    let data_dir = PyObject::from_borrowed_ptr(py, data_dir_ptr);
    let config_dir = PyObject::from_borrowed_ptr(py, config_dir_ptr);
    let permission_verifier = PyObject::from_borrowed_ptr(py, permission_verifier_ptr);
    let batch_observers = PyObject::from_borrowed_ptr(py, batch_observers_ptr);
    let batch_injector_factory = PyObject::from_borrowed_ptr(py, batch_injector_factory_ptr);

    let chain_head = if chain_head == Python::None(py) {
        None
    } else {
        let wrapped_chain_head = PyObjectWrapper::new(chain_head);
        Some(BlockPair::from(wrapped_chain_head))
    };

    let batch_observers = if let Ok(py_list) = batch_observers.extract::<PyList>(py) {
        let mut res: Vec<Box<dyn BatchObserver>> = Vec::with_capacity(py_list.len(py));
        py_list
            .iter(py)
            .for_each(|pyobj| res.push(Box::new(PyBatchObserver::new(pyobj))));
        res
    } else {
        return ErrorCode::InvalidInput;
    };

    let batch_publisher = PY_BATCH_PUBLISHER_CLASS
        .call(py, (identity_signer.clone_ref(py), batch_sender), None)
        .expect("Unable to create BatchPublisher");

    let publisher = match BlockPublisher::builder()
        .with_merkle_state(merkle_state)
        .with_executor(executor)
        .with_scheduler_factory(scheduler_factory)
        .with_block_broadcaster(block_sender)
        .with_commit_store(commit_store)
        .with_block_manager(block_manager)
        .with_state_view_factory(state_view_factory)
        .with_batch_observers(batch_observers)
        .with_signer(identity_signer)
        .start()
    {
        Ok(publisher) => publisher,
        Err(err) => {
            error!("Unable start publisher: {}", err);
            return ErrorCode::Internal;
        }
    };

    *block_publisher_ptr = Box::into_raw(Box::new(publisher)) as *const c_void;

    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn block_publisher_drop(publisher: *mut c_void) -> ErrorCode {
    check_null!(publisher);
    Box::from_raw(publisher as *mut BlockPublisher);
    ErrorCode::Success
}

// block_publisher_on_batch_received is used in tests
#[no_mangle]
pub unsafe extern "C" fn block_publisher_on_batch_received(
    publisher: *mut c_void,
    batch: *mut py_ffi::PyObject,
) -> ErrorCode {
    check_null!(publisher, batch);
    // TODO: How will the existing tests be effected by this change
    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn block_publisher_start(
    publisher: *mut c_void,
    incoming_batch_sender: *mut *const c_void,
) -> ErrorCode {
    check_null!(publisher);
    let batch_tx = match (*(publisher as *mut BlockPublisher)).take_batch_submitter() {
        Some(submitter) => submitter,
        None => {
            error!("Attempting to start an already started publisher");
            return ErrorCode::InvalidInput;
        }
    };
    let batch_tx_ptr: *mut BatchSubmitter = Box::into_raw(Box::new(batch_tx));

    *incoming_batch_sender = batch_tx_ptr as *const c_void;

    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn block_publisher_stop(publisher: *mut c_void) -> ErrorCode {
    check_null!(publisher);
    (*(publisher as *mut BlockPublisher))
        .shutdown_signaler()
        .shutdown();
    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn block_publisher_chain_head_lock(
    publisher_ptr: *mut c_void,
    chain_head_lock_ptr: *mut *const c_void,
) -> ErrorCode {
    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn block_publisher_pending_batch_info(
    publisher: *mut c_void,
    length: *mut i32,
    limit: *mut i32,
) -> ErrorCode {
    check_null!(publisher);

    *length = 0;
    *limit = 0;

    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn block_publisher_initialize_block(
    publisher: *mut c_void,
    previous_block: *mut py_ffi::PyObject,
) -> ErrorCode {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let py_obj = PyObject::from_borrowed_ptr(py, previous_block);
    let wrapper = PyObjectWrapper::new(py_obj);
    let block = BlockPair::from(wrapper);

    let publisher = *(publisher as *mut BlockPublisher);
    use sawtooth::journal::publisher::BlockInitializationError::*;
    py.allow_threads(move || match publisher.initialize_block(block) {
        Err(BlockPublisherError::BlockInitializationFailed(BlockInProgress)) => {
            ErrorCode::BlockInProgress
        }
        Err(BlockPublisherError::BlockInitializationFailed(MissingPredecessor)) => {
            ErrorCode::MissingPredecessor
        }
        Err(err) => {
            error!("Unexpected error returned from initialize_block: {}", err);
            ErrorCode::Internal
        }
        Ok(_) => ErrorCode::Success,
    })
}

#[no_mangle]
pub unsafe extern "C" fn block_publisher_finalize_block(
    publisher: *mut c_void,
    consensus: *const u8,
    consensus_len: usize,
    force: bool,
    result: *mut *const u8,
    result_len: *mut usize,
) -> ErrorCode {
    check_null!(publisher, consensus);
    let consensus = slice::from_raw_parts(consensus, consensus_len);

    use sawtooth::journal::publisher::BlockCompletionError::*;
    match (*(publisher as *mut BlockPublisher)).finalize_block(consensus.to_vec()) {
        Err(BlockPublisherError::BlockCompletionFailed(BlockNotInitialized)) => ErrorCode::BlockNotInitialized,
        Err(BlockPublisherError::BlockCompletionFailed(BlockEmpty)) => ErrorCode::BlockEmpty,
        Err(err) => {
            error!("Unexpected error returned from finalize_block: {}", err);
            ErrorCode::Internal
        }
        Ok(block_id) => {
            *result = block_id.as_ptr();
            *result_len = block_id.as_bytes().len();

            mem::forget(block_id);

            ErrorCode::Success
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn block_publisher_summarize_block(
    publisher: *mut c_void,
    force: bool,
    result: *mut *const u8,
    result_len: *mut usize,
) -> ErrorCode {
    check_null!(publisher);

    match (*(publisher as *mut BlockPublisher)).summarize_block() {
        Err(BlockPublisherError::BlockCompletionFailed(BlockNotInitialized)) => ErrorCode::BlockNotInitialized,
        Err(BlockPublisherError::BlockCompletionFailed(BlockEmpty)) => ErrorCode::BlockEmpty,
        Err(err) => {
            error!("Unexpected error return from finalize_block: {}", err);
            ErrorCode::Internal
        }
        Ok(consensus) => {
            *result = consensus.as_ptr();
            *result_len = consensus.as_slice().len();

            mem::forget(consensus);

            ErrorCode::Success
        }
    }
}

// convert_on_chain_updated_args is used in tests
pub unsafe fn convert_on_chain_updated_args(
    py: Python,
    chain_head_ptr: *mut py_ffi::PyObject,
    committed_batches_ptr: *mut py_ffi::PyObject,
    uncommitted_batches_ptr: *mut py_ffi::PyObject,
) -> (BlockPair, Vec<Batch>, Vec<Batch>) {
    let chain_head = PyObject::from_borrowed_ptr(py, chain_head_ptr);
    let py_committed_batches = PyObject::from_borrowed_ptr(py, committed_batches_ptr);
    let py_wrappers_committed: Vec<PyObjectWrapper> = py_committed_batches
        .extract::<PyList>(py)
        .expect("Failed to extract PyList from uncommitted_batches")
        .iter(py)
        .map(PyObjectWrapper::new)
        .collect::<Vec<PyObjectWrapper>>();

    let committed_batches: Vec<Batch> = if py_committed_batches == Python::None(py) {
        Vec::new()
    } else {
        py_wrappers_committed
            .into_iter()
            .map(Batch::from)
            .collect::<Vec<Batch>>()
    };
    let py_uncommitted_batches = PyObject::from_borrowed_ptr(py, uncommitted_batches_ptr);
    let py_wrappers_uncommitted = py_uncommitted_batches
        .extract::<PyList>(py)
        .expect("Failed to extract PyList from uncommitted_batches")
        .iter(py)
        .map(PyObjectWrapper::new)
        .collect::<Vec<PyObjectWrapper>>();

    let uncommitted_batches: Vec<Batch> = if py_uncommitted_batches == Python::None(py) {
        Vec::new()
    } else {
        py_wrappers_uncommitted
            .into_iter()
            .map(Batch::from)
            .collect::<Vec<Batch>>()
    };
    let wrapped_chain_head = PyObjectWrapper::new(chain_head);
    let chain_head = BlockPair::from(wrapped_chain_head);

    (chain_head, committed_batches, uncommitted_batches)
}

// block_publisher_on_chain_updated is used in tests
#[no_mangle]
pub unsafe extern "C" fn block_publisher_on_chain_updated(
    publisher: *mut c_void,
    chain_head_ptr: *mut py_ffi::PyObject,
    committed_batches_ptr: *mut py_ffi::PyObject,
    uncommitted_batches_ptr: *mut py_ffi::PyObject,
) -> ErrorCode {
    check_null!(
        publisher,
        chain_head_ptr,
        committed_batches_ptr,
        uncommitted_batches_ptr
    );

    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn block_publisher_has_batch(
    publisher: *mut c_void,
    batch_id: *const c_char,
    has: *mut bool,
) -> ErrorCode {
    check_null!(publisher, batch_id);
    let batch_id = match CStr::from_ptr(batch_id).to_str() {
        Ok(s) => s,
        Err(_) => return ErrorCode::InvalidInput,
    };

    *has = match (*(publisher as *mut BlockPublisher)).has_batch(batch_id) {
        Ok(has) => has,
        Err(err) => {
            error!("Unexpected error returned from has_batch: {}", err);
            return ErrorCode::Internal;
        }
    };

    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn block_publisher_cancel_block(publisher: *mut c_void) -> ErrorCode {
    check_null!(publisher);

    match (*(publisher as *mut BlockPublisher)).cancel_block() {
        Ok(_) => ErrorCode::Success,
        Err(_) => ErrorCode::BlockNotInitialized,
    }
}

struct PyBatchObserver {
    py_batch_observer: PyObject,
}

impl PyBatchObserver {
    fn new(py_batch_observer: PyObject) -> Self {
        PyBatchObserver { py_batch_observer }
    }
}

impl BatchObserver for PyBatchObserver {
    fn notify_batch_pending(&self, batch: &Batch) {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let batch_wrapper = PyObjectWrapper::from(batch.clone());
        self.py_batch_observer
            .call_method(py, "notify_batch_pending", (batch_wrapper,), None)
            .expect("BatchObserver has no method notify_batch_pending");
    }
}
