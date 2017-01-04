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
import asyncio
from collections import OrderedDict
from pipetree.config import PipelineStageConfig
from pipetree.stage import PipelineStageFactory
from pipetree.loaders import PipelineConfigLoader
from pipetree.exceptions import DuplicateStageNameError
from pipetree.futures import InputFuture


class DependencyChain(object):
    def __init__(self, start_stage):
        self.levels = [set([start_stage])]

    def add_stage(self, level, name):
        if level < len(self.levels):
            self.levels[level].add(name)
        else:
            self.levels.append(set([name]))

    def get_level(self, level):
        if level < len(self.levels):
            return self.levels[level]
        return None

    def __repr__(self):
        result = ''
        i = 0
        for level in self.levels:
            result += '%d: (' % i
            for name in level:
                result += '%s ' % name
            result += ')\n'
            i += 1
        return result


class Pipeline(object):
    def __init__(self, stages=None):
        self._stages = stages or OrderedDict()
        self._endpoints = set()
        self._find_endpoint_stages()
        self._queue = None

    def _find_endpoint_stages(self):
        for stage in self._stages:
            self._endpoints.add(stage)
        for _, stage in self._stages.items():
            if hasattr(stage, 'inputs'):
                for input_stage in stage.inputs:
                    self._endpoints.remove(input_stage)

    def set_arbiter_queue(self, queue):
        self._queue = queue

    def _build_chain(self, stage_name, level=0, chain=None):
        if chain is None:
            chain = DependencyChain(stage_name)
            self._build_chain(stage_name, level + 1, chain)
            return chain
        stage = self._stages[stage_name]
        if not hasattr(stage, 'inputs'):
            return
        for input in self._stages[stage_name].inputs:
            chain.add_stage(level, input)
            self._build_chain(input, level+1, chain)

    @asyncio.coroutine
    def generate_stage(self, stage_name, schedule, executor):
        chain = self._build_chain(stage_name)

        # Create an input future for each input to this function
        pre_reqs = chain.get_level(1)

        print("Generating stage %s" % stage_name)
        if pre_reqs is None or len(pre_reqs) == 0:
            # This stage does not need to make futures based on inputs
            # just write some result somewhere who knows wtf is going on
            print("No inputs needed for stage %s" % stage_name)
            artifacts = []
            res = self._stages[stage_name]._yield_artifacts()
            for art in res:
                print("Yielding an artifact from stage %s" % stage_name)
                artifacts.append(art)
            print("Stage Complete: %s" % stage_name)
            return artifacts
        else:
            # This is when we do need to schedule a future with the arbiter
            # Schedule somethign to be written when input_future resolves
            input_future = InputFuture(stage_name)
            for pre_req in pre_reqs:
                input_future.add_input_source(pre_req)
            schedule(input_future)

            # Wait until the associated futures have been setup
            artifactChunks = yield from input_future.await_artifacts()
            artifacts = []
            for artifactChunk in artifactChunks:
                artifacts += artifactChunk

            print("All (%d) inputs acquired for stage %s" %
                  (len(artifacts), stage_name))

            # Now feed their result to the executor,
            # returning generated artifacts
            # For now we'll ignore this, and just execute the stage.
            found_stage = self._stages[stage_name]
            result = []
            for art in found_stage.yield_artifacts(input_artifacts=artifacts):
                print("Yielded an artifact after getting input artifacts")
                result.append(art)
            print("Done generating stage %s" % stage_name)
            return result

    @property
    def stages(self):
        return self._stages

    @property
    def endpoints(self):
        return self._endpoints


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
