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
import signal
import asyncio
import threading
import time
import copy
from concurrent.futures import CancelledError

from pipetree.executor.local import LocalCPUExecutor
from pipetree.executor.remoteSQS import RemoteSQSExecutor
from pipetree.pipeline import PipelineFactory
from pipetree.backend import LocalArtifactBackend, S3ArtifactBackend
from pipetree import settings
from pipetree.utils import attach_config_to_object
from pipetree.monitor import Monitor
import pipetree.exceptions as exceptions

class ArbiterBase(object):
    def __init__(self, filepath, loop=None, monitor=None, pipeline_run_id=None):
        if loop is not None:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()
        self._pipeline = PipelineFactory().generate_pipeline_from_file(
            filepath)
        self._queue = asyncio.Queue(loop=self._loop)
        self._pipeline.set_arbiter_queue(self._queue)

        self._final_artifacts = []
        self._run_complete = False
        self._lock = threading.Lock()
        self._artifact_backend = None

        self._monitor = monitor
        self._pipeline_run_id = pipeline_run_id
        if monitor is None:
            self._monitor = Monitor()

    def await_run_complete(self):
        """
        Wait for final artifacts to be produced.
        Useful for testing purposes.
        """
        while True:
            with self._lock:
                if self._run_complete is True:
                    break
            time.sleep(0.25)
        return self._final_artifacts

    def _complete_run(self):
        self.shutdown()
        raise exceptions.PipelineRunComplete

    def reset(self):
        """
        Reset accumulated internal state
        """
        with self._lock:
            self._final_artifacts = []
            self._run_complete = False

    async def _evaluate_pipeline(self):
        """
        Beginning at pipeline endpoints, create input futures
        from the bottom up.
        """
        for name in self._pipeline.endpoints:
            self._log("Evaluating pipeline endpoints: %s" %
                      (str(self._pipeline.endpoints)))
            groups = await self._pipeline.generate_stage(
                name,
                self.enqueue,
                self._default_executor,
                self._artifact_backend,
                self._monitor,
                self._pipeline_run_id
            )

            for group in groups:
                x = await group
                print("ACQUIRED GROUP: ", x)
                with self._lock:
                    self._log("Stage %s complete. %s Appending final artifacts"
                              % (name, x))
                    self._final_artifacts += [x]

        with self._lock:
            self._run_complete = True
            self._complete_run()

    def enqueue(self, obj):
        self._queue.put_nowait(obj)

    def run_event_loop(self):
        raise NotImplementedError


class LocalArbiter(ArbiterBase):
    def __init__(self, filepath, loop=None, backend=None, monitor=None, pipeline_run_id=None):
        super().__init__(filepath, loop)
        self._local_cpu_executor = LocalCPUExecutor(self._loop)
        self._default_executor = self._local_cpu_executor
        self._pipeline_run_id = pipeline_run_id
        if backend is None:
            backend = LocalArtifactBackend()
        if monitor is None:
            monitor = Monitor()
        self._artifact_backend = backend
        self._monitor = monitor

    def _log(self, text):
        print("LocalArbiter: %s" % text)

    async def _resolve_future_inputs(self, future):
        """
        Ensure that all input futures to this future will be resolved.
        Once all tasks have been created, we call
        set_all_associated_futures_created so that we can block on
        this future as necessary.
        """
        for input_stage in future._input_sources:
            input_futures = await self._pipeline.generate_stage(
                input_stage,
                self.enqueue,
                self._default_executor,
                self._artifact_backend,
                self._monitor,
                self._pipeline_run_id
            )
            for input_future in input_futures:
                future.add_associated_future(asyncio.ensure_future(
                    input_future
                ))
        future.set_all_associated_futures_created()

    async def _listen_to_queue(self):
        try:
            while True:
                self._log("Listening on queue")

                # Extract future from queue
                future = await self._queue.get()
                self._log('Read: %s' % future._input_sources)

                # Create an async job to generate all input futures
                # associated with this future, and to ensure they resolve.
                asyncio.ensure_future(self._resolve_future_inputs(future))
        except RuntimeError:
            pass

    async def _main(self):
        try:
            await self._evaluate_pipeline()
        except exceptions.PipelineRunComplete:
            self._log('Pipeline run complete')

    async def _close_after(self, num_seconds):
        if num_seconds is None:
            return
        await asyncio.sleep(num_seconds)
        self.shutdown()
        raise CancelledError

    def shutdown(self):
        for task in asyncio.Task.all_tasks():
            task.cancel()

    def run_event_loop(self, close_after=None):
        self._loop.add_signal_handler(signal.SIGHUP, self.shutdown)
        self._loop.add_signal_handler(signal.SIGINT, self.shutdown)
        self._loop.add_signal_handler(signal.SIGTERM, self.shutdown)

        try:
            self._loop.run_until_complete(asyncio.wait([
                self._close_after(close_after),
                self._main(),
                self._monitor.run(),
                self._listen_to_queue()
            ]))
        except CancelledError:
            self._log('Closing event loop.')
            with self._lock:
                self._run_complete = True
        finally:
            self._loop.close()


class RemoteSQSArbiter(ArbiterBase):
    DEFAULTS = {
        "s3_bucket_name": settings.S3_ARTIFACT_BUCKET_NAME,
        "aws_region": settings.AWS_REGION,
        "aws_profile": settings.AWS_PROFILE,
        "artifact_table_name": settings.DYNAMODB_ARTIFACT_TABLE_NAME,
        "stage_run_table_name": settings.DYNAMODB_STAGE_RUN_TABLE_NAME,
        "task_queue_name": settings.SQS_TASK_QUEUE_NAME,
        "result_queue_name": settings.SQS_RESULT_QUEUE_NAME,
    }

    def __init__(self, filepath, loop=None, monitor=None, pipeline_run_id=None, **kwargs):
        super().__init__(filepath, loop, monitor, pipeline_run_id)
        config = copy.copy(self.DEFAULTS)
        config.update(kwargs)
        self._config = kwargs
        attach_config_to_object(self, config)

        self._artifact_backend = S3ArtifactBackend(
            s3_bucket_name=self.s3_bucket_name,
            aws_region=self.aws_region,
            aws_profile=self.aws_profile,
            dynamodb_artifact_table_name=
            self.artifact_table_name,
            dynamodb_stage_run_table_name=
            self.stage_run_table_name)

        self._default_executor = RemoteSQSExecutor(
            s3_bucket_name=self.s3_bucket_name,
            aws_region=self.aws_region,
            aws_profile=self.aws_profile,
            dynamodb_artifact_table_name=self.artifact_table_name,
            dynamodb_stage_run_table_name= self.stage_run_table_name,
            task_queue_name=self.task_queue_name,
            result_queue_name=self.result_queue_name
        )

    def _log(self, text):
        print("RemoteSQSArbiter: %s" % text)

    async def _resolve_future_inputs(self, future):
        """
        Ensure that all input futures to this future will be resolved.
        Once all tasks have been created, we call
        set_all_associated_futures_created so that we can block on
        this future as necessary.
        """
        for input_stage in future._input_sources:
            input_futures = await self._pipeline.generate_stage(
                input_stage,
                self.enqueue,
                self._default_executor,
                self._artifact_backend,
                self._monitor,
                self._pipeline_run_id
            )
            for input_future in input_futures:
                future.add_associated_future(asyncio.ensure_future(
                    input_future
                ))
        future.set_all_associated_futures_created()

    async def _listen_to_queue(self):
        try:
            while True:
                self._log("Listening on queue")

                # Extract future from queue
                future = await self._queue.get()
                self._log('Read: %s' % future._input_sources)

                # Create an async job to generate all input futures
                # associated with this future, and to ensure they resolve.
                asyncio.ensure_future(self._resolve_future_inputs(future))
        except RuntimeError:
            pass

    async def _main(self):
        try:
            await self._evaluate_pipeline()
        except exceptions.PipelineRunComplete:
            self._log('Pipeline run complete')

    async def _close_after(self, num_seconds):
        if num_seconds is None:
            return
        await asyncio.sleep(num_seconds)
        self.shutdown()
        raise CancelledError

    def shutdown(self):
        for task in asyncio.Task.all_tasks():
            task.cancel()

    def run_event_loop(self, close_after=None):
        self._loop.add_signal_handler(signal.SIGHUP, self.shutdown)
        self._loop.add_signal_handler(signal.SIGINT, self.shutdown)
        self._loop.add_signal_handler(signal.SIGTERM, self.shutdown)
        print("MONITOR", self._monitor)
        try:
            self._loop.run_until_complete(asyncio.wait([
                self._close_after(close_after),
                self._main(),
                self._default_executor._process_queue(),
                self._monitor.run(),
                self._listen_to_queue(),
            ]))
        except CancelledError:
            self._log('CancelledError raised: closing event loop.')
            with self._lock:
                self._run_complete = True
        except exceptions.PipelineRunComplete:
            self._log('Pipeline run complete')
        finally:
            self._loop.close()
