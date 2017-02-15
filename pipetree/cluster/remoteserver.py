import os.path
import json
from pipetree.executor.remoteSQS import RemoteSQSServer

class RemoteServer(object):
    def __init__(self, path):
        self._path = path

    def load_config(self):
        with open(os.path.join(self._path, "server_config.json")) as f:
            self._config = json.load(f)

    def run(self):
        self.load_config()
        print(json.dumps(self._config, indent=4))
        self._server = RemoteSQSServer(
            aws_region=self._config['aws_region'],
            s3_bucket_name=self._config['s3_artifact_bucket_name'],
            task_queue_name=self._config['sqs_task_queue_name'],
            result_queue_name=self._config['sqs_result_queue_name'],
            dynamodb_artifact_table_name=self._config['dynamodb_artifact_table_name'],
            dynamodb_stage_run_table_name=self._config['dynamodb_stage_run_table_name']
        )
        self._server.run()
