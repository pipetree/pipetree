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
import boto3
import botocore.exceptions
import json
import asyncio
import threading

from pipetree import settings
from pipetree.executor import Executor, LocalCPUExecutor
from pipetree.artifact import Artifact
from pipetree.backend import S3ArtifactBackend
from pipetree.executor.executor import ALL_ARTIFACTS_GENERATED
from pipetree.executor.server import ExecutorServer


class RemoteSQSExecutor(Executor):
    """
    RemoteSQSExecutor serializes the tasks provided,
    pushing them to an SQS queue so that tasks can be
    completed on remote servers.
    """
    def __init__(self,
                 aws_region=settings.AWS_REGION,
                 aws_profile=settings.AWS_PROFILE,
                 task_queue_name=settings.SQS_TASK_QUEUE_NAME,
                 result_queue_name=settings.SQS_RESULT_QUEUE_NAME,
                 s3_bucket_name=settings.S3_ARTIFACT_BUCKET_NAME,
                 dynamodb_artifact_table_name=
                   settings.DYNAMODB_ARTIFACT_TABLE_NAME,
                 dynamodb_stage_run_table_name=
                   settings.DYNAMODB_STAGE_RUN_TABLE_NAME,
                 loop=None):
        super().__init__(loop)

        try:
            self._session = boto3.Session(profile_name=aws_profile,
                                          region_name=aws_region)
        except botocore.exceptions.NoCredentialsError:
            self._session = boto3.Session(region_name=aws_region)
        except botocore.exceptions.ProfileNotFound:
            self._session = boto3.Session(region_name=aws_region)

        self._backend = S3ArtifactBackend(
            aws_region=aws_region,
            aws_profile=aws_profile,
            s3_bucket_name=s3_bucket_name,
            dynamodb_artifact_table_name=dynamodb_artifact_table_name,
            dynamodb_stage_run_table_name=dynamodb_stage_run_table_name)

        self._sqs = self._session.resource('sqs')
        self._task_queue = self._sqs.create_queue(QueueName=task_queue_name)
        self._result_queue = self._sqs.create_queue(QueueName=result_queue_name)

        # One queue is created for each awaited result. When the thread reading from
        # SQS
        self._lock = threading.Lock()
        self._await_result_queues = {}


        self._log("RemoteSQSExecutor intitialized with task queue %s and result queue %s" %
              (task_queue_name, result_queue_name))
        self._local_executor = LocalCPUExecutor(self._loop)

    def _log(self, message):
        print("RemoteSQSExecutor: %s" % message)

    def _queue_push(self, task, config_hash, dependency_hash):
        self._log("Sending SQS task message for stage %s: %s / %s" %
                  (task._stage._config.name, config_hash, dependency_hash))
        self._task_queue.send_message(
            MessageBody=task.serialize(),
            MessageAttributes={
                'stage_config_hash': {
                    'StringValue': config_hash,
                    'DataType': 'String'
                },
                'dependency_hash': {
                    'StringValue': dependency_hash,
                    'DataType': 'String'
                }
            }
        )

    def _complete_task(self, task, config_hash, dependency_hash):
        """
        Load artifacts produced by a task from the backend, then mark the task complete
        """
        cached_arts = self._backend.find_pipeline_stage_run_artifacts(
            task._stage._config, dependency_hash)
        for art in cached_arts:
            loaded = self._backend.load_artifact(art)
            loaded._loaded_from_cache = True
            loaded._remotely_produced = True
            task.enqueue_artifact(art)
        task.all_artifacts_generated()

    def _await_queue_id(self, config_hash, dependency_hash):
        return config_hash + "__" + dependency_hash

    async def _await_result(self, config_hash, dependency_hash):
        key = self._await_queue_id(config_hash, dependency_hash)
        queue = asyncio.Queue(loop=self._loop)
        with self._lock:
            self._await_result_queues[key] = queue
        message = await queue.get()
        return message

    async def _process_sqs_messages(self):
        while True:
            await asyncio.sleep(2.0)
            for message in self._result_queue.receive_messages(
                    MessageAttributeNames=['stage_config_hash',
                                           'dependency_hash']):
                if message.message_attributes is None:
                    self._log("WARNING: SQS message received without attributes.")
                    continue
                m_config_hash = message.message_attributes.\
                    get('stage_config_hash').get('StringValue')
                m_dependency_hash = message.message_attributes.\
                    get('dependency_hash').get('StringValue')

                # Mark the task as complete if this is the message
                #  we've been waiting for.
                with self._lock:
                    key = self._await_queue_id(m_config_hash, m_dependency_hash)
                    if key in self._await_result_queues:
                        self._log("Task complete. %s / %s" %
                                  (m_config_hash, m_dependency_hash))
                        result = self._await_result_queues[key].put_nowait(message)
                        del self._await_result_queues[key]
                        return result
                    else:
                        self._log("Task %s / %s not yet awaited" %
                                  (m_config_hash, m_dependency_hash))

    async def _execute_locally(self, task):
        """
        Execute a task locally when necessary
        """
        local_task = self._local_executor.create_task(task._stage,
                                                      task._input_artifacts,
                                                      task._monitor,
                                                      task._pipeline_run_id)
        arts = []
        res = (None, None)
        while res[1] != ALL_ARTIFACTS_GENERATED:
            res = await local_task._queue.get()
            arts.append(res)
            if res[0] != None:
                self._backend.save_artifact(res[0])

        # Re-enqueue artifacts in task for future processing
        for art in arts:
            task.enqueue_artifact(art)

    async def _process_queue(self):
        while True:
            task = await self._queue.get()
            self._log('Acquired Task: %s with %d inputs' %
                      (task._stage._config.name,
                       len(task._input_artifacts)))

            config_hash = task._stage._config.hash()
            dependency_hash = Artifact.dependency_hash(
                task._input_artifacts)

            # If task should be executed locally, do so.
            if(True == hasattr(task._stage, "_local_stage")):
                await self._execute_locally(task)
                return

            # Push task to SQS queue
            self._queue_push(task, config_hash, dependency_hash)

            # Wait until task is complete
            message = await self._await_result(config_hash, dependency_hash)
            result = message.body
            message.delete()
            self._complete_task(task, config_hash, dependency_hash)


class RemoteSQSServer(object):
    """
    Listen to an SQS queue, consuming serialized tasks and
    pushing messages indicating their completion.
    """
    def __init__(self,
                 s3_bucket_name=settings.S3_ARTIFACT_BUCKET_NAME,
                 aws_region=settings.AWS_REGION,
                 aws_profile=settings.AWS_PROFILE,
                 dynamodb_artifact_table_name=
                   settings.DYNAMODB_ARTIFACT_TABLE_NAME,
                 dynamodb_stage_run_table_name=
                   settings.DYNAMODB_STAGE_RUN_TABLE_NAME,
                 loop=None,
                 task_queue_name=settings.SQS_TASK_QUEUE_NAME,
                 result_queue_name=settings.SQS_RESULT_QUEUE_NAME):

        # Configure S3 backend
        self._backend = S3ArtifactBackend(
            aws_region=aws_region,
            aws_profile=aws_profile,
            s3_bucket_name=s3_bucket_name,
            dynamodb_artifact_table_name=dynamodb_artifact_table_name,
            dynamodb_stage_run_table_name=dynamodb_stage_run_table_name)

        # Setup AWS Session
        try:
            self._session = boto3.Session(profile_name=aws_profile,
                                          region_name=aws_region)
        except botocore.exceptions.NoCredentialsError:
            self._session = boto3.Session(region_name=aws_region)
        except botocore.exceptions.ProfileNotFound:
            self._session = boto3.Session(region_name=aws_region)

        # Setup SQS Queues
        self._sqs = self._session.resource('sqs')
        self._task_queue = self._sqs.create_queue(QueueName=task_queue_name)
        self._result_queue = self._sqs.create_queue(QueueName=result_queue_name)

        self._log("Initialized with task queue %s and result queue %s" %
                  (task_queue_name, result_queue_name))
        # Setup executor server
        if loop is None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        else:
            self._loop = loop
        self._executor = LocalCPUExecutor(loop=self._loop)
        self._executor_server = ExecutorServer(self._backend,
                                               self._executor,
                                               self._loop)

    def _log(self, message):
        print("RemoteSQSServer: %s" % message)

    def _send_complete_message(self, config_hash, dependency_hash):
        self._log("Sending SQS Result Message for %s / %s" % (config_hash,
                                                              dependency_hash))
        self._result_queue.send_message(
            MessageBody="Task complete",
            MessageAttributes={
                'stage_config_hash': {
                    'StringValue': config_hash,
                    'DataType': 'String'
                },
                'dependency_hash': {
                    'StringValue': dependency_hash,
                    'DataType': 'String'
                }
            }
        )

    async def _process_tasks(self):
        i = 0
        while True:
            await asyncio.sleep(2.0)
            if i % 30 == 0:
                self._log("Awaiting SQS Result Message")
            i += 1
            for message in self._task_queue.receive_messages(
                    MessageAttributeNames=['stage_config_hash',
                                           'dependency_hash']):
                if message.message_attributes is not None:
                    m_config_hash = message.message_attributes.\
                        get('stage_config_hash').get('StringValue')
                    m_dependency_hash = message.message_attributes.\
                        get('dependency_hash').get('StringValue')
                    self._log("Retrieved SQS task message %s / %s" %
                              (m_config_hash, m_dependency_hash))
                    task = json.loads(message.body)
                    job_id = self._executor_server.enqueue_job(task)
                    self._log("Enqueued Job ID: %d for stage %s" %
                              (job_id, task['stage_name']))
                    message.delete()
                    job = None
                    while job is None or job['status'] is not 'complete':
                        job = self._executor_server.retrieve_job(job_id)
                        await asyncio.sleep(1.0)
                    self._log("Completed Job ID: %d for stage %s" %
                              (job_id, task['stage_name']))
                    self._send_complete_message(m_config_hash, m_dependency_hash)

    def run(self):
        print("Running SQS Executor Server")
        asyncio.ensure_future(self._process_tasks())
        self._executor_server.run_event_loop()
