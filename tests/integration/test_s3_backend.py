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
# SOFTWARE.
import os
import os.path
import distutils.dir_util
import unittest
from tests import isolated_filesystem
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

class TestS3ArtifactBackend(unittest.TestCase):
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

        # Setup settings for local arbiter
        self.stage_config = PipelineStageConfig("test_stage_name", {
            "type": "ParameterPipelineStage"
        })
        self.config_filename = 'pipetree.json'
        with open(os.path.join(".", self.config_filename), 'w') as f:
            json.dump(self.generate_pipeline_config(), f)

        # Cleanup before each test is run
        self.cleanup_test_tables(self._default_backend)            

    @classmethod
    def setUpClass(cls):
        x = random.randrange(10000000000000)
        cls.test_bucket_name = "pipetree-test-bucket-" + str(x)
        cls.test_region = "us-west-1"
        cls.test_profile = "testing"
        cls.test_dynamodb_artifact_table_name = "pipetree_test_artifact_meta"
        cls.test_dynamodb_stage_run_name = "pipetree_test_stage_run"

        cls._session = boto3.Session(profile_name=cls.test_profile,
                                      region_name=cls.test_region)
        cls._default_backend = cls.newBackend(cls)

        # Delete buckets and rows in table
        cls.cleanup_test_tables(cls._default_backend)

    @classmethod
    def tearDownClass(cls):
        cls.cleanup_buckets(cls._default_backend._s3_client)
        cls.cleanup_test_tables(cls._default_backend)

    def generate_pipeline_config(self):
        return OrderedDict([(
            'StageA', {
                'type': 'LocalFilePipelineStage',
                'filepath': self.filename[0]
            }),
            ('StageB', {
                'inputs': ['StageA'],
                'type': 'IdentityPipelineStage'
            })]
        )

    @staticmethod
    def cleanup_buckets(client):
        for bucket in client.list_buckets()['Buckets']:
            if 'pipetree-test' in bucket['Name']:
                print("Deleting test bucket: %s" % bucket['Name'])
                listed = client.list_objects_v2(Bucket=bucket['Name'])
                if 'Contents' in listed:
                    objs = listed['Contents']
                    response = client.delete_objects(
                        Bucket=bucket['Name'],
                        Delete={'Objects': [{"Key": x["Key"]} for x in objs]})
                client.delete_bucket(Bucket=bucket['Name'])

    @staticmethod
    def newBackend(conf):
        return S3ArtifactBackend(s3_bucket_name=conf.test_bucket_name,
                                 aws_region=conf.test_region,
                                 aws_profile=conf.test_profile,
                                 dynamodb_artifact_table_name=
                                 conf.test_dynamodb_artifact_table_name,
                                 dynamodb_stage_run_table_name=
                                 conf.test_dynamodb_stage_run_name)

    def tearDown(self):

        self.fs.__exit__(None, None, None)

    def tearDownClass():
        pass

    @staticmethod
    def delete_all_rows(table_name, table, key_names):
        response = table.scan()
        keys = []
        for item in response['Items']:
            obj = {}
            for k in key_names:
                obj[k] = item[k]
            keys.append(obj)
        with table.batch_writer() as batch:
            for k in keys:
                batch.delete_item(Key=k)

    @staticmethod
    def cleanup_test_tables(backend):
        TestS3ArtifactBackend.delete_all_rows(backend.dynamodb_stage_run_table_name,
                                              backend._stage_run_table,
                                              ['stage_config_hash', 'dependency_hash']
        )

        TestS3ArtifactBackend.delete_all_rows(backend.dynamodb_artifact_table_name,
                                              backend._artifact_meta_table,
                                              ['artifact_uid']
        )

    def test_save_artifact(self):
        s3_backend = self._default_backend
        artifact = Artifact(self.stage_config)
        artifact.item.payload = "foobs"
        s3_backend.save_artifact(artifact)
        self.cleanup_test_tables(self._default_backend)

    def test_no_profile(self):
        x = random.randrange(10000000000000)
        try:
            s3_backend = S3ArtifactBackend(s3_bucket_name=self.test_bucket_name,
                                           aws_region=self.test_region)
        except botocore.exceptions.NoCredentialsError:
            pass

    def test_non_existant_profile(self):
        x = botocore.exceptions.ProfileNotFound
        try:
            s3_backend = S3ArtifactBackend(s3_bucket_name=self.test_bucket_name,
                                           aws_region=self.test_region,
                                           aws_profile="fdbfgfsd"
            )
        except botocore.exceptions.ProfileNotFound:
            pass

    def test_save_missing_payload(self):
        artifact = Artifact(self.stage_config)
        try:
            self._default_backend.save_artifact(artifact)
            self.assertEqual(0, "The above line should fail " +
                             "due to the artifact having no payload")
            self.fail()
        except ArtifactMissingPayloadError:
            pass

    def test_load_artifact_local_cache(self):
        backend = LocalArtifactBackend(config={"path": "./test_storage/"})

        artifact = Artifact(self.stage_config)
        payload = "SHRIM"
        artifact.item = Item(payload=payload)
        backend.save_artifact(artifact)

        loaded_artifact = backend.load_artifact(artifact)
        self.assertEqual(loaded_artifact.item.payload, artifact.item.payload)
        self.assertEqual(True, loaded_artifact._loaded_from_local_cache)

    def test_load_artifact_from_s3(self):
        backend = self._default_backend
        artifact = Artifact(self.stage_config)
        payload = "SHRIM"
        artifact.item = Item(payload=payload)
        backend.save_artifact(artifact)

        # Now we'll delete the local artifact cache, forcing retrieval from S3
        path = backend._localArtifactBackend.path
        for root, dirs, files in os.walk(path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        distutils.dir_util.mkpath(path)

        loaded_artifact = backend.load_artifact(artifact)
        self.assertEqual(loaded_artifact.item.payload.decode('utf-8'), payload)
        self.assertEqual(True, loaded_artifact._loaded_from_s3_cache)

        self.cleanup_test_tables(self._default_backend)

    def test_pipeline_caching(self):
        self._default_backend.enable_local_caching = False
        arbiter = LocalArbiter(os.path.join(".", self.config_filename),
                               loop=None, backend=self._default_backend)
        try:
            arbiter.run_event_loop(close_after=15.0)
        except RuntimeError:
            # Event loop is always closed
            pass

        final_artifacts = arbiter.await_run_complete()
        for artifact in final_artifacts:
            print(artifact.item.payload)
        self.assertEqual(len(final_artifacts), 1)
        self.assertEqual(final_artifacts[0]._loaded_from_cache, False)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        arbiter = LocalArbiter(os.path.join(".", self.config_filename),
                               loop=None, backend=self._default_backend)
        arbiter.reset()
        print("========= Second run of pipeline to test caching ======")
        try:
            arbiter.run_event_loop(close_after=15.0)
        except RuntimeError:
            # Event loop is always closed
            pass

        final_artifacts = arbiter.await_run_complete()

        print("Final Artifacts")
        for artifact in final_artifacts:
            print(artifact.item.payload)
        
        self.assertEqual(len(final_artifacts), 1)
        self.assertEqual(final_artifacts[0]._loaded_from_s3_cache, True)
        self.cleanup_test_tables(self._default_backend)        
