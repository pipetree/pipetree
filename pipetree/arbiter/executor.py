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
from concurrent.futures import CancelledError


class ExecutorTask(object):
    def __init__(self, stage_config, input_artifacts):
        """
        Task corresponding to the execution of stage given by stage_config,
        utilizing input_artifacts
        """
        self._input_artifacts = []
        self._stage_config = stage_config

    def serialize(self):
        raise NotImplementedError

    def load_from_json(self):
        raise NotImplementedError


class Executor(object):
    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self._queue = asyncio.Queue(loop=self._loop)

    def enqueue_task(self, task):
        self._queue.put_nowait(task)

    @asyncio.coroutine
    def _process_queue(self):
        try:
            while True:
                task = yield from self._queue.get()
                print('Acquired Task: %s' % task)
        except RuntimeError:
            pass

    def run_event_loop(self):
        self._loop.add_signal_handler(signal.SIGHUP, self.shutdown)
        self._loop.add_signal_handler(signal.SIGINT, self.shutdown)
        self._loop.add_signal_handler(signal.SIGTERM, self.shutdown)

        try:
            self._loop.run_until_complete(asyncio.wait([
                self._process_queue(),
                self._listen_to_queue()
            ]))
        except CancelledError:
            click.echo('\nKeyboard Interrupt: closing event loop.')
        finally:
            self._loop.close()


class LocalCPUExecutor(Executor):
    def __init__(self):
        super().__init__()
