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
import click
import asyncio
import signal
import json
from concurrent.futures import CancelledError

ALL_ARTIFACTS_GENERATED = "ALL_ARTIFACTS_GENERATED"

class ExecutorTask(object):
    def __init__(self, loop, stage, input_artifacts):
        """
        Task corresponding to the execution of a stage
        utilizing input_artifacts.
        """
        self._input_artifacts = input_artifacts
        self._stage = stage
        self._loop = None #loop
        self._queue = asyncio.Queue(loop=self._loop)        

    def serialize(self):
        return json.dumps({
            "stage_name": stage._config.name,
            "artifacts": list(map(lambda x: x.meta_to_dict(), input_artifacts))
        })

    def load_from_json(self):
        raise NotImplementedError

    def enqueue_artifact(self, artifact):
        self._queue.put_nowait((artifact, None))

    def all_artifacts_generated(self):
        """
        Once all artifacts are generated, send a message on the queue
        so that consumers can finish waiting.
        """
        self._queue.put_nowait((None, ALL_ARTIFACTS_GENERATED))
    
    async def generate_artifacts(self):
        """
        Returns all artifacts produced by this stage. 
        To stream artifact results, the queue can be accessed
        directly. 
        """
        artifacts = []
        while True:
            (artifact, status) = await self._queue.get()
            if status == ALL_ARTIFACTS_GENERATED:
                break
            artifacts.append(artifact)
        return artifacts

class Executor(object):
    def __init__(self, loop):
        self._loop = loop
        self._queue = asyncio.Queue(loop=self._loop)
        asyncio.ensure_future(self._process_queue())

    def create_task(self, stage, input_artifacts):
        task = ExecutorTask(loop=self._loop,
                            stage=stage,
                            input_artifacts=input_artifacts)
        self._queue.put_nowait(task)
        return task

    def _process_queue(self):
        raise NotImplementedError

