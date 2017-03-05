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
import os.path
from pipetree.config import PipelineStageConfig
from pipetree.stage import PipelineStageFactory


eshclass TestExecutorPipelineStage(unittest.TestCase):
    def setUp(self):
        self.test_array = ['foo', 'bar', 'baz']
        self.stage_a = {
            'inputs': [],
            'type': 'ParameterPipelineStage',
            'parameter_a': self.test_array
        }
        args = ["/"] + (__file__.split("/")[1:-1])
        self.stage_b = {
            'inputs': ['StageA'],
            'execute': 'module.executor_function.stageB',
            'type': 'ExecutorPipelineStage',
            'directory': os.path.join(*args)
        }

        self.stage_c = {
            'inputs': ['StageB'],
            'execute': 'module.executor_function.stageC',
            'type': 'ExecutorPipelineStage',
            'directory': os.path.join(*args),
            'full_artifacts': True
        }

        self.stage_a_config = PipelineStageConfig('StageA', self.stage_a)
        self.stage_b_config = PipelineStageConfig('StageB', self.stage_b)
        self.stage_c_config = PipelineStageConfig('StageC', self.stage_c)
        self.factory = PipelineStageFactory()

    def test_init_executor(self):
        stage_a = self.factory.create_pipeline_stage(self.stage_a_config)
        stage_b = self.factory.create_pipeline_stage(self.stage_b_config)
        stage_c = self.factory.create_pipeline_stage(self.stage_c_config)
        res = []

        stage_a_out = []
        for x in stage_a.yield_artifacts():
            stage_a_out.append(x)

        stage_b_out = []
        for x in stage_b.yield_artifacts(stage_a_out):
            stage_b_out.append(x)

        stage_c_out = []
        for x in stage_c.yield_artifacts(stage_b_out):
            stage_c_out.append(x)

        res = None
        for x in stage_c_out:
            res = x

        self.assertEqual(res.item.payload, "foo, bar, baz information_value5.0")
