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
import unittest
import asyncio
import json
import time
import os
from collections import OrderedDict
from pipetree.executor.server import ExecutorServer
from pipetree.executor import LocalCPUExecutor, ExecutorTask
from pipetree.backend import LocalArtifactBackend
from tests import isolated_filesystem
from pipetree.arbiter import LocalArbiter
from pipetree.stage import PipelineStageFactory
from pipetree.config import PipelineStageConfig


class TestServer(unittest.TestCase):
    def setUp(self):
        self.config_filename = 'pipetree.json'
        self.testfile_name = 'testfile'
        self.testfile_contents = "Testfile Contents"
        self.fs = isolated_filesystem()
        self.fs.__enter__()

        with open(os.path.join(".", self.testfile_name), 'w') as f:
            f.write(self.testfile_contents)

        with open(os.path.join(".", self.config_filename), 'w') as f:
            json.dump(self.generate_pipeline_config(), f)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    def tearDown(self):
        pass

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

    def pregenerate_artifacts(self, backend):
        pf = PipelineStageFactory()
        config = PipelineStageConfig("StageA",
                                     self.generate_pipeline_config()["StageA"])
        stage = pf.create_pipeline_stage(config)
        arts = []
        for art in stage.yield_artifacts():
            backend.save_artifact(art)
            arts.append(art)
        return arts

    def test_run(self):
        backend = LocalArtifactBackend()
        arts = self.pregenerate_artifacts(backend)
        loop = asyncio.set_event_loop(asyncio.new_event_loop())
        executor = LocalCPUExecutor(loop=loop)
        server = ExecutorServer(backend, executor)
        job_id = server.enqueue_job({
            "stage_name": "StageB",
            "stage_config": self.generate_pipeline_config()["StageB"],
            "artifacts":  list(map(ExecutorTask.wrap_input_artifact, arts))
        })
        server.run_event_loop(3)
        job_result = server.retrieve_job(job_id)
        self.assertEqual(len(job_result['artifacts']), 1)

        pl = job_result['artifacts'][0].item.payload
        pl.open()
        self.assertEqual(self.testfile_contents, pl.read())
        pl.close()
