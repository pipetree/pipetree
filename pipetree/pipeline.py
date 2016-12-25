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
from collections import OrderedDict
from pipetree.config import PipelineStageConfig
from pipetree.stage import PipelineStageFactory
from pipetree.loaders import PipelineConfigLoader
from pipetree.exceptions import DuplicateStageNameError


class Pipeline(object):
    def __init__(self, stages=None):
        self._stages = stages or OrderedDict()


class PipelineFactory(object):
    def __init__(self):
        self._stage_factory = PipelineStageFactory()
        self._loader = PipelineConfigLoader()

    def generate_pipeline_from_file(self, config_file):
        configs = []
        for config in self._loader.load_file(config_file):
            configs.append(config)
        stages = self._generate_stages(configs)
        return Pipeline(stages)

    def generate_pipeline_from_dict(self, config_data):
        if not isinstance(config_data, OrderedDict):
            raise TypeError('generate_pipeline_from_dict requires an '
                            'OrderedDict to preserve the loading order '
                            'of the pipeline stages. Found %s instead.' %
                            type(config_data))
        configs = []
        for name, data in config_data.items():
            config = PipelineStageConfig(name, data)
            configs.append(config)
        stages = self._generate_stages(configs)
        return Pipeline(stages)

    def _generate_stages(self, configs):
        stages = OrderedDict()
        for config in configs:
            self._add_stage(stages, config)
        return stages

    def _add_stage(self, stages, config):
        stage = self._stage_factory.create_pipeline_stage(config)
        if stage.validate_prereqs(stages):
            stages[stage.name] = stage
