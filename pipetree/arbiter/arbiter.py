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
from pipetree.executor.local import LocalCPUExecutor
from pipetree.pipeline import PipelineFactory
from pipetree.backend import LocalArtifactBackend
from concurrent.futures import CancelledError


class ArbiterBase(object):
    def __init__(self, filepath, loop=None):
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
            x = await self._pipeline.generate_stage(
                name,
                self.enqueue,
                self._default_executor,
                self._artifact_backend
            )
            with self._lock:
                self._log("Stage %s complete. Appending final artifacts"
                          % name)
                self._final_artifacts += x
        with self._lock:
            self._run_complete = True

    def enqueue(self, obj):
        self._queue.put_nowait(obj)

    def run_event_loop(self):
        raise NotImplementedError


class LocalArbiter(ArbiterBase):
    def __init__(self, filepath, loop=None, backend=None):
        super().__init__(filepath, loop)
        self._local_cpu_executor = LocalCPUExecutor(self._loop)
        self._default_executor = self._local_cpu_executor
        if backend is None:
            backend = LocalArtifactBackend()
        self._artifact_backend = backend

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
            future.add_associated_future(asyncio.ensure_future(
                self._pipeline.generate_stage(
                    input_stage,
                    self.enqueue,
                    self._local_cpu_executor,
                    self._artifact_backend
                )))
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
        await self._evaluate_pipeline()

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
                self._listen_to_queue()
            ]))
        except CancelledError:
            self._log('CancelledError raised: closing event loop.')
            with self._lock:
                self._run_complete = True
        finally:
            self._loop.close()


class RemoteArbiter(ArbiterBase):
    pass
