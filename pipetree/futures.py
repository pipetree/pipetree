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
import copy
import asyncio
import threading


class ArtifactFuture(object):
    """This is the future associated with an artifact"""
    def __init__(self):
        self._dependency_hash = dependency_hash
        # Check cache layers
        # set result if we have it in cache
        # Otherwise we need to call out to previous pipeline layers


class InputFuture(object):
    """
    Helper class that manages ArtifactFutures
    and resolves them into Artifacts
    """
    def __init__(self, stage_name):
        self._stage_name = stage_name
        self._input_sources = []

        # This future represents the overall batch status
        self._future = None
        self._exception = None
        self._result = None
        self._lock = threading.Lock()

        self._futures_created = False
        """
        Associated futures are the individual artifact
        futures that are resolving. Once all resolve the
        main _future will have its result set if any
        associated_futures fail then the _future will have an exception set
        """
        self._associated_futures = set()
        self._associated_futures_lock = threading.Lock()

    def add_input_source(self, source):
        self._input_sources.append(source)

    @property
    def associated_futures(self):
        with self._associated_futures_lock:
            return copy.copy(self._associated_futures)

    async def await_artifacts(self):
        """
        Await the production of artifacts from all associated futures
        """
        while True:
            await asyncio.sleep(1)
            with self._lock:
                if self._futures_created is True:
                    break

        results = []
        for future in self._associated_futures:
            x = await future
            results.append(x)
        return results

    def add_associated_future(self, future):
        with self._associated_futures_lock:
            self._associated_futures.add(future)

    def set_all_associated_futures_created(self):
        with self._lock:
            self._futures_created = True

    @property
    def exception(self):
        return self._exception

    def set_result(self, result):
        with self._lock:
            self._exception = None
            self._result = result

    def set_exception(self, exception):
        with self._lock:
            self._exception = exception
            self._result = None
