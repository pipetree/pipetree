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
import unittest
import json

from pipetree.loaders import JSONFileLoader, PipelineConfigLoader
from pipetree.exceptions import InvalidPipelineConfigError
from pipetree.stage import PipelineStageFactory
from tests import isolated_filesystem


VALID_PIPELINE_CONFIG = """
{
    "name": {
        "type": "LocalDirectoryPipelineStage"
    }
}
"""

INVALID_PIPELINE_CONFIG = """
{
    "name": "bar"
}
"""


class TestJSONLoader(unittest.TestCase):
    def setUp(self):
        self.filename = 'file.json'
        self.dictionary = {
            'foo': "bar",
            'baz': 3,
            'list': [1, 2, 3],
            'nested': {
                'foo': 'bar'
            }
        }

    def test_read_json_from_file(self):
        with isolated_filesystem():
            with open(self.filename, 'w') as f:
                f.write(json.dumps(self.dictionary))

            loader = JSONFileLoader()
            loaded_dict = loader.load_file(self.filename)
            self.assertEqual(self.dictionary, loaded_dict)


class TestPipelineConfigLoader(unittest.TestCase):
    def setUp(self):
        self.filename = 'file.json'
        self.factory = PipelineStageFactory()
        self.fs = isolated_filesystem()
        self.fs.__enter__()

    def tearDown(self):
        self.fs.__exit__(None, None, None)

    # These might not be needed, the same paths are tested in test_pipeline
    # def test_load_valid_pipeline_config(self):
    #     with open(self.filename, 'w') as f:
    #         f.write(VALID_PIPELINE_CONFIG)
    #         loader = PipelineConfigLoader()
    #     try:
    #         config = loader.load_file(self.filename)
    #     except InvalidPipelineConfigError:
    #         print('This was supposed to be a valid config')
    #         raise
    #     stage = self.factory.create_pipeline_stage(config)

    # def test_load_invalid_pipeline_config(self):
    #     with open(self.filename, 'w') as f:
    #         f.write(INVALID_PIPELINE_CONFIG)
    #     loader = PipelineConfigLoader()
    #     try:
    #         loader.load_file(self.filename)
    #         raise Exception('This should have thrown an error, '
    #                         'it is an invalid config.')
    #     except InvalidPipelineConfigError:
    #         pass
