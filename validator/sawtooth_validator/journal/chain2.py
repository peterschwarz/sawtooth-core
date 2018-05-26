# Copyright 2018 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------------
import ctypes
from enum import IntEnum

from sawtooth_validator.ffi import PY_LIBRARY
from sawtooth_validator.ffi import LIBRARY
from sawtooth_validator.ffi import CommonErrorCode
from sawtooth_validator.ffi import OwnedPointer


class NativeChainController(OwnedPointer):
    def __init__(self,
            block_store,
            block_cache,
            block_validator,
            on_chain_updated,
            data_dir,
            observers=None
    ):
        super(NativeChainController, self).__init__('chain_controller_drop')

        print("Creating Rust ChainContoller")
        if observers is None:
            observers = []

        _pylibexec(
            'chain_controller_new',
            ctypes.py_object(block_store),
            ctypes.py_object(block_cache),
            ctypes.py_object(block_validator),
            ctypes.py_object(on_chain_updated),
            ctypes.py_object(observers),
            ctypes.c_char_p(data_dir.encode()),
            ctypes.byref(self.pointer))

    def start(self):
        _libexec('chain_controller_start', self.pointer)
        print('started')

    def stop(self):
        _libexec('chain_controller_stop', self.pointer)
        print('stopped')

    def has_block(self, block_id):
        result = ctypes.c_bool()

        _libexec('chain_controller_has_block',
                 self.pointer, ctypes.c_char_p(block_id.encode()),
                 ctypes.byref(result))
        print("has_block: {}".format(result.value))
        return result.value

    def queue_block(self, block):
        _pylibexec('chain_controller_queue_block', self.pointer,
                   ctypes.py_object(block))
        print('queued')

    @property
    def chain_head(self):
        print("getting chain_head")
        result = ctypes.py_object()

        _pylibexec('chain_controller_chain_head',
                 self.pointer,
                 ctypes.byref(result))

        print("get chain_head {}".format(result.value))
        return result.value


def _libexec(name, *args):
    return _exec(LIBRARY, name, *args)


def _pylibexec(name, *args):
    return _exec(PY_LIBRARY, name, *args)


def _exec(library, name, *args):
    res = library.call(name, *args)
    if res == ErrorCode.Success:
        return
    elif res == ErrorCode.NullPointerProvided:
        raise ValueError("Provided null pointer(s)")
    elif res == ErrorCode.InvalidDataDir:
        raise ValueError("Invalid data dir {}".format(data_dir))
    elif res == ErrorCode.InvalidPythonObject:
        raise ValueError("Invalid python object submitted")
    elif res == ErrorCode.InvalidBlockId:
        raise ValueError("Invalid block id provided.")
    else:
        raise TypeError("Unknown error occurred: {}".format(res.error))


class ErrorCode(IntEnum):
    Success = CommonErrorCode.Success
    NullPointerProvided = CommonErrorCode.NullPointerProvided
    InvalidDataDir = 0x02
    InvalidPythonObject = 0x03
    InvalidBlockId = 0x04
