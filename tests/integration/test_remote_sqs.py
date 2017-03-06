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

from pipetree import openStream, readStream

from pipetree.exceptions import ArtifactMissingPayloadError
from pipetree.backend import S3ArtifactBackend, LocalArtifactBackend, STAGE_COMPLETE, STAGE_DOES_NOT_EXIST, STAGE_IN_PROGRESS
from pipetree.config import PipelineStageConfig
from pipetree.artifact import Artifact, Item
from pipetree.monitor import Monitor

from pipetree.stage import PipelineStageFactory
from pipetree.executor.remoteSQS import RemoteSQSExecutor, RemoteSQSServer
from aws_base import AWSTestBase
import asyncio


class TestRemoteSQS(AWSTestBase):
    def setUp(self):
        # File system configuration
        self.filenames = ['foo.bar', 'foo.baz']
        self.filedatas = ['foo bar baz', 'hello, world']
        self.fs = isolated_filesystem()
        self.fs.__enter__()

        for name, data in zip(self.filenames, self.filedatas):
            with open(os.path.join(os.getcwd(),
                                   name), 'w') as f:
                f.write(data)

        # Setup stage config identical to our default project template

        args = ["/"] + (__file__.split("/")[1:-1])
        self.stage_config_objs = OrderedDict([
            ('CatPictures', {
                'type': 'LocalDirectoryPipelineStage',
                'filepath': os.path.join(*args, "test_images"),
                'binary_mode': True
            }),
            ('SearchParams', {
                'type': 'GridSearchPipelineStage',
                'whiten_images': [True, False],
                'number_neurons': [100]
            }),
            ('ProcessImages', {
                'inputs': ['CatPictures', 'SearchParams'],
                'type': 'ExecutorPipelineStage',
                'directory': os.path.join(*args),
                'execute': 'test_module.executor_function.process_images'
            })
        ])

        self.configs = {}
        for stage_name in self.stage_config_objs:
            self.configs[stage_name] = PipelineStageConfig(
                stage_name,
                self.stage_config_objs[stage_name])

        self.monitor = Monitor()
        self.cleanup_tables(self._default_backend)

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
        stage = pf.create_pipeline_stage(self.configs['CatPictures'])
        input_artifacts = []
        executor.create_task(stage, input_artifacts, self.monitor)

        async def timeout():
            await asyncio.sleep(10.0)
            for task in asyncio.Task.all_tasks():
                task.cancel()
            raise CancelledError

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
                timeout(),
                process_loop(executor, stage, input_artifacts)
            ]))
        except CancelledError:
            print('CancelledError raised: closing event loop.')

    def _create_executor(self, loop):
        return RemoteSQSExecutor(
            aws_profile=self.test_profile,
            task_queue_name=self.test_queue_task_name,
            result_queue_name=self.test_queue_result_name,
            s3_bucket_name=self.test_bucket_name,
            dynamodb_artifact_table_name=self.test_dynamodb_artifact_table_name,
            dynamodb_stage_run_table_name=self.test_dynamodb_stage_run_name,
            loop=loop
        )

    def _create_server(self, loop):
        return RemoteSQSServer(
            aws_profile=self.test_profile,
            aws_region=self.test_region,
            s3_bucket_name=self.test_bucket_name,
            task_queue_name=self.test_queue_task_name,
            result_queue_name=self.test_queue_result_name,
            dynamodb_artifact_table_name=self.test_dynamodb_artifact_table_name,
            dynamodb_stage_run_table_name=self.test_dynamodb_stage_run_name,
            loop=loop
        )

    def test_executor_server_integration(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        executor = self._create_executor(loop)
        server = self._create_server(loop)

        pf = PipelineStageFactory()
        file_stage = pf.create_pipeline_stage(self.configs['CatPictures'])
        executor.create_task(file_stage, [], self.monitor)

        grid_stage = pf.create_pipeline_stage(self.configs['SearchParams'])
        executor.create_task(grid_stage, [], self.monitor)

        # Run our local RemoteExecutor and the remote RemoteSQSServer
        async def timeout(n):
            await asyncio.sleep(n)
            for task in asyncio.Task.all_tasks():
                task.cancel()
            raise CancelledError
        try:
            loop.run_until_complete(asyncio.wait([
                executor._process_queue(),
                executor._process_sqs_messages(),
                server._process_tasks(),
                server._executor_server._listen_to_queue(),
                timeout(10.0)
            ]))
        except CancelledError:
            print('CancelledError raised: closing event loop.')

        # Load our locally generated artifact(s) and ensure they
        # have the correct payload.
        file_stage_out = executor._backend.find_pipeline_stage_run_artifacts(
            self.configs['CatPictures'],
            Artifact.dependency_hash([]))
        self.assertNotEqual(file_stage_out, None)
        for art in file_stage_out:
            loaded = executor._backend.load_artifact(art)
            self.assertNotEqual(None, loaded)
            self.assertNotEqual(None, loaded.item.payload)

        grid_stage_out = executor._backend.find_pipeline_stage_run_artifacts(
            self.configs['SearchParams'],
            Artifact.dependency_hash([]))
        self.assertNotEqual(grid_stage_out, None)
        for art in grid_stage_out:
            loaded = executor._backend.load_artifact(art)
            print("GRID PARAM", loaded.item.payload)
            self.assertNotEqual(None, loaded)
            self.assertNotEqual(None, loaded.item.payload)

        # Now remotely run the executor stage
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        executor = self._create_executor(loop)
        server = self._create_server(loop)

        process_stage = pf.create_pipeline_stage(self.configs['ProcessImages'])
        for grid_params in grid_stage_out:
            print("Test: Creating task for grid params")
            executor.create_task(process_stage, file_stage_out + [grid_params], self.monitor)
        try:
            loop.run_until_complete(asyncio.wait([
                executor._process_queue(),
                executor._process_sqs_messages(),
                server._process_tasks(),
                server._executor_server._listen_to_queue(),
                timeout(30.0)
            ]))
        except CancelledError:
            print('CancelledError raised: closing event loop.')

        for grid_params in grid_stage_out:
            process_stage_out = executor._backend.find_pipeline_stage_run_artifacts(
                self.configs['ProcessImages'],
                Artifact.dependency_hash(file_stage_out + [grid_params]))
            self.assertNotEqual(process_stage_out, None)
            for art in process_stage_out:
                loaded = executor._backend.load_artifact(art)
                self.assertNotEqual(None, loaded)
                self.assertNotEqual(None, loaded.item.payload)
                self.assertEqual(loaded.item.payload, 14)


    def test_remote_sqs_server(self):
        #Ensure that we cover the direct .run() codepaths

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
