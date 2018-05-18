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

import json
import time
import unittest
import urllib

from sawtooth_sdk.protobuf import validator_pb2
from sawtooth_sdk.protobuf import consensus_pb2
from sawtooth_sdk.messaging.stream import Stream

from sawtooth_intkey.intkey_message_factory import IntkeyMessageFactory

from sawtooth_integration.tests.integration_tools import wait_for_rest_apis


REST_API_URL = "http://rest-api:8008"
INTERBLOCK_PERIOD = 1
WAIT = 300
BATCH_KEYS = 'abcd'


class TestConsensusEngineAPI(unittest.TestCase):
    def setUp(self):
        wait_for_rest_apis([REST_API_URL])
        self.stream = Stream("tcp://validator:5005")

    def tearDown(self):
        self.stream.close()

    def test_consensus_engine_api(self):
        batches = make_batches(BATCH_KEYS)
        committed = 0

        for batch in batches:
            batch_response = self.publish_block(batch)

            self.commit_block()

            # Wait for the validator to respond that the batch was committed
            wait_for_batch(batch_response)
            committed += 1

        self.assertEqual(committed, len(batches))
        blocks = query_rest_api('/blocks')
        self.assertEqual(
            len(blocks['data']),
            len(BATCH_KEYS) + 1)

    def publish_block(self, batch):
        # Initialize a new block
        status = self._initialize()

        # Submit a batch and wait
        response = post_batch(batch)
        time.sleep(INTERBLOCK_PERIOD)

        # Finalize the block
        while True:
            status = self._finalize()
            if status == consensus_pb2.\
                    ConsensusFinalizeBlockResponse.BLOCK_NOT_READY:
                time.sleep(1)
            else:
                self.assertEqual(
                    status,
                    consensus_pb2.ConsensusFinalizeBlockResponse.OK)
                break

        return response

    def commit_block(self):
        block_id = self._receive_new()
        self._check_and_commit(block_id)

    def _receive_new(self):
        new_update = self._receive_update(
            validator_pb2.Message.CONSENSUS_NOTIFY_BLOCK_NEW,
            consensus_pb2.ConsensusNotifyBlockNew)
        return new_update.block.block_id

    def _check_and_commit(self, block_id):
        self._check(block_id)
        valid_update = self._receive_update(
            validator_pb2.Message.CONSENSUS_NOTIFY_BLOCK_VALID,
            consensus_pb2.ConsensusNotifyBlockValid)
        self.assertEqual(
            block_id,
            valid_update.block_id)
        self._commit(block_id)
        commit_update = self._receive_update(
            validator_pb2.Message.CONSENSUS_NOTIFY_BLOCK_COMMIT,
            consensus_pb2.ConsensusNotifyBlockCommit)
        self.assertEqual(
            block_id,
            commit_update.block_id)

    def _check(self, block_id):
        future = self.stream.send(
            validator_pb2.Message.CONSENSUS_CHECK_BLOCKS_REQUEST,
            consensus_pb2.ConsensusCheckBlocksRequest(block_ids=[block_id])
                         .SerializeToString())
        result = future.result()
        self.assertEqual(
            result.message_type,
            validator_pb2.Message.CONSENSUS_CHECK_BLOCKS_RESPONSE)
        response = consensus_pb2.ConsensusCheckBlocksResponse()
        response.ParseFromString(result.content)
        return response.status

    def _commit(self, block_id):
        future = self.stream.send(
            validator_pb2.Message.CONSENSUS_COMMIT_BLOCK_REQUEST,
            consensus_pb2.ConsensusCommitBlockRequest(block_id=block_id)
                         .SerializeToString())
        result = future.result()
        self.assertEqual(
            result.message_type,
            validator_pb2.Message.CONSENSUS_COMMIT_BLOCK_RESPONSE)
        response = consensus_pb2.ConsensusCommitBlockResponse()
        response.ParseFromString(result.content)
        return response.status

    def _initialize(self):
        future = self.stream.send(
            validator_pb2.Message.CONSENSUS_INITIALIZE_BLOCK_REQUEST,
            consensus_pb2.ConsensusInitializeBlockRequest()
                         .SerializeToString())
        result = future.result()
        self.assertEqual(
            result.message_type,
            validator_pb2.Message.CONSENSUS_INITIALIZE_BLOCK_RESPONSE)
        response = consensus_pb2.ConsensusInitializeBlockResponse()
        response.ParseFromString(result.content)
        return response.status

    def _finalize(self):
        future = self.stream.send(
            validator_pb2.Message.CONSENSUS_FINALIZE_BLOCK_REQUEST,
            consensus_pb2.ConsensusFinalizeBlockRequest(data=b"Devmode")
                         .SerializeToString())
        result = future.result()
        self.assertEqual(
            result.message_type,
            validator_pb2.Message.CONSENSUS_FINALIZE_BLOCK_RESPONSE)
        response = consensus_pb2.ConsensusFinalizeBlockResponse()
        response.ParseFromString(result.content)
        return response.status

    def _receive_update(self, update_type, update_class):
        message = self.stream.receive().result()
        self.stream.send_back(
            validator_pb2.Message.CONSENSUS_NOTIFY_ACK,
            message.correlation_id,
            consensus_pb2.ConsensusNotifyAck().SerializeToString())
        self.assertEqual(message.message_type, update_type)
        update = update_class()
        update.ParseFromString(message.content)
        return update


def post_batch(batch):
    headers = {'Content-Type': 'application/octet-stream'}
    response = query_rest_api(
        '/batches', data=batch, headers=headers)
    return response


def wait_for_batch(post_response):
    response = submit_request('{}&wait={}'.format(post_response['link'], WAIT))
    return response


def query_rest_api(suffix='', data=None, headers=None):
    if headers is None:
        headers = {}
    url = REST_API_URL + suffix
    return submit_request(urllib.request.Request(url, data, headers))


def submit_request(request):
    response = urllib.request.urlopen(request).read().decode('utf-8')
    return json.loads(response)


def make_batches(keys):
    imf = IntkeyMessageFactory()
    return [imf.create_batch([('set', k, 0)]) for k in keys]
