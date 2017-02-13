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
from pipetree.backend import S3ArtifactBackend
import unittest
from collections import OrderedDict
import boto3
import random


def get_or_create_queue(sqs_resource, queue_name):
    try:
        return sqs_resource.create_queue(QueueName=queue_name)
    except:
        return sqs_resource.get_queue_by_name(QueueName=queue_name)


class AWSTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        x = random.randrange(10000000000000)
        cls.test_bucket_name = "pipetree-test-bucket-" + str(x)
        cls.test_region = "us-west-1"
        cls.test_profile = "testing"
        cls.test_dynamodb_artifact_table_name = "pipetree_test_artifact_meta"
        cls.test_dynamodb_stage_run_name = "pipetree_test_stage_run"
        cls.test_queue_task_name = "pipetree_test_task_queue"
        cls.test_queue_result_name = "pipetree_test_result_queue"

        cls._session = boto3.Session(profile_name=cls.test_profile,
                                      region_name=cls.test_region)
        cls._sqs = cls._session.resource('sqs')
        cls._default_backend = cls.newBackend(cls)

        # Delete buckets and rows in table
        cls.cleanup_test_tables(cls._default_backend)
        cls.cleanup_test_queues()

    @classmethod
    def tearDownClass(cls):
        cls.cleanup_buckets(cls._default_backend._s3_client)
        cls.cleanup_test_tables(cls._default_backend)
        cls.cleanup_test_queues()

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

    @classmethod
    def cleanup_test_queues(cls):
        print("Cleaning up test queues")
        q1 = get_or_create_queue(cls._sqs, cls.test_queue_task_name)
        q2 = get_or_create_queue(cls._sqs, cls.test_queue_result_name)
        for message in q1.receive_messages():
            message.delete()
        for message in q2.receive_messages():
            message.delete()

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
        AWSTestBase.delete_all_rows(
            backend.dynamodb_stage_run_table_name,
            backend._stage_run_table,
            ['stage_config_hash', 'dependency_hash']
        )

        AWSTestBase.delete_all_rows(
            backend.dynamodb_artifact_table_name,
            backend._artifact_meta_table,
            ['artifact_uid']
        )
