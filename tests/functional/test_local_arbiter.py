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
import asyncio
import os.path
from tests import isolated_filesystem
from collections import OrderedDict
from pipetree.artifact import Artifact
from pipetree.config import PipelineStageConfig
from pipetree.exceptions import *
from pipetree.arbiter import LocalArbiter


class TestLocalArbiter(unittest.TestCase):
    def setUp(self):
        self.config_filename = 'pipetree.json'
        self.testfile_name = 'testfile'
        self.testfile_contents = "Testfile Contents"
        self.fs = isolated_filesystem()
        self.fs.__enter__()

        with open(os.path.join(".", self.testfile_name), 'w') as f:
            json.dump(self.testfile_contents, f)

        with open(os.path.join(".", self.config_filename), 'w') as f:
            json.dump(self.generate_pipeline_config(), f)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
            
        pass

    def tearDown(self):
        self.fs.__exit__(None, None, None)

    def generate_pipeline_config(self):
        return OrderedDict([(
            'StageA', {
                'type': 'LocalFilePipelineStage',
                'filepath': self.testfile_name
            }),
            ('StageB', {
                'inputs': ['StageA'],
                'type': 'IdentityPipelineStage'
            })]
        )

    def test_run_pipeline(self):
        arbiter = LocalArbiter(os.path.join(".", self.config_filename))
        try:
            arbiter.run_event_loop(close_after=3.0)
        except RuntimeError:
            # Event loop is always closed
            print("RTE")
            pass
        final_artifacts = arbiter.await_run_complete()
        print(final_artifacts[0].item.payload)
        self.assertEqual(len(final_artifacts), 1)
        self.assertEqual(final_artifacts[0]._loaded_from_cache, False)

    def test_pipeline_caching(self):
        arbiter = LocalArbiter(os.path.join(".", self.config_filename))
        try:        
            arbiter.run_event_loop(close_after=3.0)
        except RuntimeError:
            # Event loop is always closed
            pass
        
        final_artifacts = arbiter.await_run_complete()
        print(final_artifacts[0].item.payload)
        print("FINAL ARTIFACTS")
        for artifact in final_artifacts:
            print(artifact.item.payload)
        self.assertEqual(len(final_artifacts), 1)
        self.assertEqual(final_artifacts[0]._loaded_from_cache, False)

        print("")
        print("")
        print("")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        arbiter = LocalArbiter(os.path.join(".", self.config_filename),
                               loop)
        arbiter.reset()
        try:                
            arbiter.run_event_loop(close_after=3.0)
        except RuntimeError:
            # Event loop is always closed
            pass
            
        final_artifacts = arbiter.await_run_complete()
        print("FINAL ARTIFACTS")
        for artifact in final_artifacts:
            print(artifact.item.payload)
        print(final_artifacts)
        self.assertEqual(len(final_artifacts), 1)
        self.assertEqual(final_artifacts[0]._loaded_from_cache, True)
