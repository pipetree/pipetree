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
from pipetree.executor import Executor


class LocalCPUExecutor(Executor):
    def __init__(self, loop):
        super().__init__(loop)

    def _log(self, message):
        print("LocalExecutor: %s" % message)

    async def _process_queue(self):
        try:
            while True:
                task = await self._queue.get()
                self._log('Acquired Task: %s with %d inputs' %
                    (task._stage._config.name,
                     len(task._input_artifacts)))
                for artifact in task._stage.yield_artifacts(
                        input_artifacts=task._input_artifacts):
                    task.enqueue_artifact(artifact)
                task.all_artifacts_generated()
        except RuntimeError:
            pass
