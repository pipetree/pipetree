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
import os
import json
from collections import OrderedDict

from pipetree.config import PipelineStageConfig
from pipetree.exceptions import InvalidPipelineConfigError


def _append_json_ext(path):
    if path.endswith('.json'):
        return path
    return '%s.json' % path


class JSONFileLoader(object):
    """Parent class of any loader that reads from a json file"""
    def load_file(self, path):
        full_path = _append_json_ext(path)
        with open(path, 'rb') as f:
            content = f.read().decode('utf-8')
            return json.loads(content, object_pairs_hook=OrderedDict)


class PipelineConfigLoader():
    LOADER_CLASS = JSONFileLoader

    def __init__(self, file_loader=None):
        if file_loader is None:
            file_loader = self.LOADER_CLASS()
        self._file_loader = file_loader

    def load_file(self, path):
        data = self._file_loader.load_file(path)
        for name, pipeline_stage in data.items():
            config = PipelineStageConfig(name, pipeline_stage)
            yield config
