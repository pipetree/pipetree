# MIT License

# Copyright (c) 2016 Morgan McDermott & John Carlyle

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE

import os
import os.path
import distutils.dir_util
import unittest
from tests import isolated_filesystem
from concurrent.futures import CancelledError
from collections import OrderedDict
from pipetree.arbiter import LocalArbiter
import boto3
import botocore
import asyncio
import random
import time
import json

from pipetree.exceptions import ArtifactMissingPayloadError
from pipetree.backend import S3ArtifactBackend, LocalArtifactBackend, STAGE_COMPLETE, STAGE_DOES_NOT_EXIST, STAGE_IN_PROGRESS
from pipetree.config import PipelineStageConfig
from pipetree.artifact import Artifact, Item


from pipetree.stage import PipelineStageFactory
from pipetree.executor.remoteSQS import RemoteSQSExecutor, RemoteSQSServer
from aws_base import AWSTestBase
import asyncio


class TestRemoteSQS(AWSTestBase):
    def setUp(self):
        # File system configuration
        self.filename = ['foo.bar', 'foo.baz']
        self.filedatas = ['foo bar baz', 'hello, world']
        self.fs = isolated_filesystem()
        self.fs.__enter__()

        for name, data in zip(self.filename, self.filedatas):
            with open(os.path.join(os.getcwd(),
                                   name), 'w') as f:
                f.write(data)

        # Setup stage config
        self.stage_config = PipelineStageConfig("test_stage_name", {
            "type": "ParameterPipelineStage",
            "param_a": "string parameter value"
        })

    def tearDown(self):
        self.fs.__exit__(None, None, None)

    def test_create_tasks(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        executor = RemoteSQSExecutor(
            aws_profile="testing",
            task_queue_name=self.test_queue_task_name,
            result_queue_name=self.test_queue_result_name,
            loop=loop
        )

        pf = PipelineStageFactory()
        stage = pf.create_pipeline_stage(self.stage_config)
        input_artifacts = [Artifact(self.stage_config)]
        executor.create_task(stage, input_artifacts)

        async def process_loop(executor, stage, input_artifacts):
            exit_loop = False
            while not exit_loop:
                await asyncio.sleep(2.0)
                for message in executor._task_queue.receive_messages(
                        MessageAttributeNames=['stage_config_hash',
                                               'dependency_hash']):
                    print("Retrieved message")
                    print(message.body)
                    print(message.message_attributes)
                    if message.message_attributes is None:
                        self.assertEqual(0, "Message attributes absent")

                    m_config_hash = message.message_attributes.\
                        get('stage_config_hash').get('StringValue')
                    m_dependency_hash = message.message_attributes.\
                        get('dependency_hash').get('StringValue')
                    config_hash = stage._config.hash()
                    dependency_hash = Artifact.dependency_hash(
                        input_artifacts)

                    self.assertEqual(config_hash, m_config_hash)
                    self.assertEqual(dependency_hash, m_dependency_hash)
                    message.delete()
                    exit_loop = True
            for task in asyncio.Task.all_tasks():
                task.cancel()
            raise CancelledError
        try:
            loop.run_until_complete(asyncio.wait([
                executor._process_queue(),
                process_loop(executor, stage, input_artifacts)
            ]))
        except CancelledError:
            print('CancelledError raised: closing event loop.')

    def test_executor_server_integration(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        executor = RemoteSQSExecutor(
            aws_profile=self.test_profile,
            task_queue_name=self.test_queue_task_name,
            result_queue_name=self.test_queue_result_name,
            s3_bucket_name=self.test_bucket_name,
            dynamodb_artifact_table_name=self.test_dynamodb_artifact_table_name,
            dynamodb_stage_run_table_name=self.test_dynamodb_stage_run_name,
            loop=loop
        )

        server = RemoteSQSServer(
            aws_profile=self.test_profile,
            aws_region=self.test_region,
            s3_bucket_name=self.test_bucket_name,
            task_queue_name=self.test_queue_task_name,
            result_queue_name=self.test_queue_result_name,
            dynamodb_artifact_table_name=self.test_dynamodb_artifact_table_name,
            dynamodb_stage_run_table_name=self.test_dynamodb_stage_run_name,
            loop=loop
        )

        # Create task. Its input will be itself because that's just great.
        pf = PipelineStageFactory()
        stage = pf.create_pipeline_stage(self.stage_config)
        input_artifacts = []
        for art in stage.yield_artifacts():
            input_artifacts.append(art)
        executor.create_task(stage, input_artifacts)

        # Save input artifacts so they're available for the remote server
        executor._backend.save_artifact(input_artifacts[0])

        # Run our local RemoteExecutor and the remote RemoteSQSServer
        # for 10 seconds.
        async def timeout():
            await asyncio.sleep(10.0)
            for task in asyncio.Task.all_tasks():
                task.cancel()
            raise CancelledError
        try:
            loop.run_until_complete(asyncio.wait([
                executor._process_queue(),
                server._process_tasks(),
                server._executor_server._listen_to_queue(),
                timeout()
            ]))
        except CancelledError:
            print('CancelledError raised: closing event loop.')

        # Load our remotely generated artifact(s) and ensure they
        # have the correct payload.
        arts = executor._backend.find_pipeline_stage_run_artifacts(
            self.stage_config,
            Artifact.dependency_hash(input_artifacts))

        loaded = []
        for art in arts:
            loaded.append(executor._backend.load_artifact(art))

        self.assertEqual(1, len(loaded))
        self.assertEqual(loaded[0].item.payload['param_a'],
                         "string parameter value")

    def test_remote_sqs_server(self):
        #Ensuring we cover the direct .run() codepaths
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        server = RemoteSQSServer(
            aws_profile=self.test_profile,
            aws_region=self.test_region,
            s3_bucket_name=self.test_bucket_name,
            task_queue_name=self.test_queue_task_name,
            result_queue_name=self.test_queue_result_name,
            dynamodb_artifact_table_name=self.test_dynamodb_artifact_table_name,
            dynamodb_stage_run_table_name=self.test_dynamodb_stage_run_name,
            loop=loop
        )
        server.run()
