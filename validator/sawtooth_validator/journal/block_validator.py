# Copyright 2017 Intel Corporation
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

from sawtooth_validator.ffi import CommonErrorCode
from sawtooth_validator.ffi import OwnedPointer
from sawtooth_validator.ffi import LIBRARY


class BlockValidationResultStore(OwnedPointer):

    def __init__(self):
        super().__init__(
            "block_status_store_drop")

        _to_exception(LIBRARY.call("block_status_store_new",
                                   ctypes.byref(self.pointer)))


def _to_exception(res):
    if res == CommonErrorCode.Success:
        return

    if res == CommonErrorCode.NullPointerProvided:
        raise ValueError("Provided null pointer(s)")

    raise Exception("An error occurred calling a BlockValidator function")
