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

import pipetree
import pipetree.stage
from pipetree.loaders import PipelineConfigLoader
from pipetree.exceptions import InvalidConfigurationFileError,\
    StageDoesNotExistError
from pipetree.stage import PipelineStageFactory
from tests import isolated_filesystem


# TODO this test scheme isn't maintainable, re-write goodlike later
PIPELINE_CONFIG = """
{
    "CatPicFolder": {
        "type": "LocalDirectoryPipelineStage",
         "filepath": "."
    }
}
"""
PIPELINE_NOFILEPATH_CONFIG = """
{
    "CatPicFolder": {
        "type": "LocalDirectoryPipelineStage"
    }
}

"""


class TestPipelineStageLoader(unittest.TestCase):
    def setUp(self):
        self.filename = 'file.json'
        self.factory = PipelineStageFactory()
        self.fs = isolated_filesystem()
        self.fs.__enter__()

    def tearDown(self):
        self.fs.__exit__(None, None, None)

    def test_stage_export(self):
        for stage in pipetree.STAGES:
            if not hasattr(pipetree.stage, stage):
                raise StageDoesNotExistError(stage=stage)
        pass

    def test_load_valid_pipeline_config(self):
        with open(self.filename, 'w') as f:
            f.write(PIPELINE_CONFIG)
        loader = PipelineConfigLoader()
        for config in loader.load_file(self.filename):
            self.factory.create_pipeline_stage(config)

    def test_load_stage_without_filepath(self):
        with open(self.filename, 'w') as f:
            f.write(PIPELINE_NOFILEPATH_CONFIG)
        loader = PipelineConfigLoader()
        try:
            for config in loader.load_file(self.filename):
                self.factory.create_pipeline_stage(config)
            print('This should have raised an invalid config file error')
            self.fail()
        except InvalidConfigurationFileError:
            pass
