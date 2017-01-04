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
import os.path
import unittest
from tests import isolated_filesystem
from collections import OrderedDict
from pipetree.pipeline import PipelineFactory
from pipetree.exceptions import InvalidConfigurationFileError

PIPELINE_CONFIG = """
{
    "CatPicFolder": {
        "type": "LocalDirectoryPipelineStage",
         "filepath": "."
    }
}
"""


class TestPipelineLoading(unittest.TestCase):
    def setUp(self):
        self.dirname = 'foo'
        self.filename = 'bar'
        self.data = 'The charming anatomy of vestigial organs'
        self.factory = PipelineFactory()
        self.fs = isolated_filesystem()
        self.fs.__enter__()
        os.makedirs(self.dirname)
        with open(os.path.join(os.getcwd(),
                               self.dirname,
                               self.filename), 'w') as f:
            f.write(self.data)

    def tearDown(self):
        self.fs.__exit__(None, None, None)

    def test_load_full_pipeine(self):
        config = OrderedDict({
            'StageA': {
                'type': 'LocalDirectoryPipelineStage',
                'filepath': self.dirname
            },
            'StageB': {
                'type': 'LocalDirectory',  # PipelineStage suffix is optional
                'filepath': self.dirname,
                'read_content': True
            },
            'StageC': {
                'type': 'LocalFilePipelineStage',
                'filepath': os.path.join(self.dirname, self.filename)
            },
            'StageD': {
                'type': 'ParameterPipelineStage',
                'parameters': {"int_param": 200, "str_param": "str"}
            },

        })
        self.factory.generate_pipeline_from_dict(config)

    def test_load_non_ordered_dict(self):
        try:
            self.factory.generate_pipeline_from_dict({})
            print('This should have failed, supplied dict not OrderedDict')
            self.fail()
        except TypeError:
            pass

    def test_load_duplicate_stage_name(self):
        config = OrderedDict({
            'StageA': {
                'type': 'LocalDirectoryPipelineStage',
                'filepath': self.dirname
            },
            'StageB': {
                'type': 'LocalDirectory',
                'filepath': self.dirname,
            },
        })
        self.factory.generate_pipeline_from_dict(config)

    def test_generate_from_file(self):
        filename = 'config.json'
        with open(filename, 'w') as f:
            f.write(PIPELINE_CONFIG)
        self.factory.generate_pipeline_from_file(filename)

    def test_generate_executor_bad_inputs(self):
        config = OrderedDict({
            'StageA': {
                'type': 'LocalDirectoryPipelineStage',
                'filepath': self.dirname
            },
            'StageB': {
                'inputs': 'StageA',
                'type': 'ExecutorPipelineStage',
                'execute': 'package.file.function'
            },
        })
        try:
            self.factory.generate_pipeline_from_dict(config)
            print('This should have failed, inputs is not an array')
            self.fail()
        except InvalidConfigurationFileError:
            pass

    def test_generate_local_file_no_path(self):
        config = OrderedDict({
            'StageA': {
                'type': 'LocalFilePipelineStage'
            },
        })
        try:
            self.factory.generate_pipeline_from_dict(config)
            print('This should have failed, inputs is not an array')
            self.fail()
        except InvalidConfigurationFileError:
            pass

    def test_generage_executor_no_input(self):
        config = OrderedDict({
            'StageA': {
                'type': 'LocalDirectoryPipelineStage',
                'filepath': self.dirname
            },
            'StageB': {
                'type': 'ExecutorPipelineStage',
                'execute': 'package.file.function'
            },
        })
        try:
            self.factory.generate_pipeline_from_dict(config)
            print('Should have failed, no input key')
            self.fail()
        except InvalidConfigurationFileError:
            pass

    def test_generage_executor_no_execute(self):
        config = OrderedDict({
            'StageA': {
                'type': 'LocalDirectoryPipelineStage',
                'filepath': self.dirname
            },
            'StageB': {
                'inputs': ['StageA'],
                'type': 'ExecutorPipelineStage'
            },
        })
        try:
            self.factory.generate_pipeline_from_dict(config)
            print('Should have failed, no execute key')
            self.fail()
        except InvalidConfigurationFileError:
            pass

    def test_generage_executor_bad_reference(self):
        config = OrderedDict({
            'StageA': {
                'type': 'LocalDirectoryPipelineStage',
                'filepath': self.dirname
            },
            'StageB': {
                'inputs': ['BadReference'],
                'type': 'ExecutorPipelineStage',
                'execute': 'package.file.function'
            },
        })
        try:
            self.factory.generate_pipeline_from_dict(config)
            print('Should have failed, the input reference is bad')
            self.fail()
        except InvalidConfigurationFileError:
            pass

    def test_generate_executor_correct(self):
        config = OrderedDict([(
            'StageA', {
                'type': 'LocalDirectoryPipelineStage',
                'filepath': self.dirname
            }),
            ('StageB', {
                'inputs': ['StageA'],
                'type': 'ExecutorPipelineStage',
                'execute': 'package.file.function'
            }),
        ])
        self.factory.generate_pipeline_from_dict(config)
