import os
import json
import os.path
import distutils.dir_util
import unittest
import test
import shutil
import pkg_resources
from tests import isolated_filesystem

from botocore import exception
from pipetree.cluster import PipetreeCluster

class TestCluster(unittest.TestCase):
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

        test_docker_app = pkg_resources.resource_filename(__name__, "test_docker_app")
        shutil.copytree(test_docker_app, os.path.join(os.getcwd(), "test_docker_app"))
        uid = 600

        self._config = {
            "aws_region": "us-west-1",
            "s3_artifact_bucket_name": "pipetree-test-bucket",
            "sqs_task_queue_name": "pipetree-test-task-queue",
            "sqs_result_queue_name": "pipetree-test-result-queue",
            "dynamodb_artifact_table_name": "piepetree-artifact-meta",
            "dynamodb_stage_run_table_name": "pipetree-stage-run-meta"
        }
        with open(os.path.join("test_docker_app","server_config.json"), 'w') as f:
            json.dump(self._config, f)

    def tearDown(self):
        self.fs.__exit__(None, None, None)

    def test_cluster_deploy(self):
        cluster = PipetreeCluster(cluster_name="testPipetreeCluster",
                                  aws_profile="testing",
                                  **self._config)
        print(json.dumps(cluster.generate_redleader_cluster().cloud_formation_template(), indent=4))
        try:
            cluster.delete_cluster()
            cluster.create_cluster()
            time.sleep(10)
        except Exception as e:
            print("Cluster may already be deployed: %s" % e)
        print("CWD %s", os.getcwd())

        cluster.deploy_application(os.path.join(os.getcwd(), "test_docker_app"))
