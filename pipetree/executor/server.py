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

import signal
import asyncio
import threading
import time
import json
from concurrent.futures import CancelledError
from pipetree.artifact import Artifact
from pipetree.stage import PipelineStageFactory
from pipetree.config import PipelineStageConfig


class ExecutorServer(object):
    """
    A multi purpose executor server, accepting serialized
    tasks as jobs, allowing outside services to
    monitor task completion.

    Tasks should be the output of ExecutorTask.serialize(),
    which contains a JSON list of Artifact.meta_to_dict() as well
    as a stage definition.
    """
    def __init__(self, backend, executor, loop=None):
        self._backend = backend
        self._executor = executor
        self._job_count = 0
        self._jobs = {}
        self._lock = threading.Lock()
        self._loop = loop or asyncio.get_event_loop()
        self._queue = asyncio.Queue(loop=self._loop)

    def _log(self, message):
        print("ExecutorServer: %s" % message)

    def enqueue_job(self, job):
        """
        Accept the JSON representation of a task and its artifacts.
        returns a unique job ID

        Tasks should be the output of ExecutorTask.serialize(),
        which contains a JSON list of Artifact.meta_to_dict() as well
        as stage information
        """

        job_id = -1
        with self._lock:
            self._job_count += 1
            job_id = self._job_count
            self._jobs[job_id] = {"status": "queued"}
        self._queue.put_nowait((job_id, job))
        return job_id

    def retrieve_job(self, job_id):
        """
        Retrieve the current status and/or result for the given job.
        """
        job = {}
        with self._lock:
            if job_id not in self._jobs:
                return None
            job = self._jobs[job_id]
        return job

    async def _run_job(self, job):
        # Get stage from pipeline
        pf = PipelineStageFactory()
        config = PipelineStageConfig(job['stage_name'], job['stage_config'])
        stage = pf.create_pipeline_stage(config)

        # Load input artifact payloads from cache
        loaded_artifacts = []
        for artifact in job['artifacts']:
            in_config = PipelineStageConfig(
                artifact['meta']['pipeline_stage'],
                artifact['stage_config']
            )
            in_stage = pf.create_pipeline_stage(in_config)
            art_obj = Artifact(in_config)
            art_obj.meta_from_dict(artifact['meta'])
            loaded = self._backend.load_artifact(art_obj)
            if loaded is None:
                self._log(json.dumps(art_obj.meta_to_dict(), indent=4))
                self._log(art_obj.get_uid())
                self._log("Could not find payload for artifact")
                self._log(job['stage_name'])
                self._log(job['stage_config'])
                raise Exception("Could not find payload for artifact")
            loaded_artifacts.append(loaded)

        # Execute the task
        exec_task = self._executor.create_task(stage, loaded_artifacts, None)
        result = await exec_task.generate_artifacts()

        for art in result:
            art._creation_time = float(time.time())
            art._dependency_hash = Artifact.dependency_hash(loaded_artifacts)
            self._backend.save_artifact(art)
        self._backend.log_pipeline_stage_run_complete(
            config,
            Artifact.dependency_hash(loaded_artifacts))

        return result

    async def _listen_to_queue(self):
        while True:
            self._log("Listening on queue")
            job_id, job = await self._queue.get()
            self._log('Acquired job for stage: %s' % job['stage_name'])
            result_artifacts = await self._run_job(job)
            with self._lock:
                self._jobs[job_id] = {"status": "complete",
                                      "artifacts": result_artifacts,
                                      "job": job}
                self._log("Job complete: %d for stage %s" %
                          (job_id,  job['stage_name']))

    def shutdown(self):
        for task in asyncio.Task.all_tasks():
            task.cancel()

    async def _close_after(self, num_seconds):
        if num_seconds is None:
            return
        await asyncio.sleep(num_seconds)
        self.shutdown()
        raise CancelledError

    def run_event_loop(self, close_after=None):
        self._loop.add_signal_handler(signal.SIGHUP, self.shutdown)
        self._loop.add_signal_handler(signal.SIGINT, self.shutdown)
        self._loop.add_signal_handler(signal.SIGTERM, self.shutdown)

        try:
            self._loop.run_until_complete(asyncio.wait([
                self._close_after(close_after),
                self._executor._process_queue(),
                self._listen_to_queue()
            ]))
        except CancelledError:
            self._log('CancelledError raised: closing event loop.')
        finally:
            self._loop.close()
