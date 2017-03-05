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
import time
import asyncio
import hashlib
import itertools
import json
from collections import OrderedDict
from pipetree.config import PipelineStageConfig
from pipetree.stage import PipelineStageFactory
from pipetree.loaders import PipelineConfigLoader
from pipetree.exceptions import DuplicateStageNameError
from pipetree.futures import InputFuture
from pipetree.backend import STAGE_COMPLETE, STAGE_IN_PROGRESS
from pipetree.artifact import Artifact


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

    def _log(self, text):
        print("Pipeline: %s" % text)

    def _ensure_artifact_meta(self, artifact, dependency_hash):
        """
        Ensure that an artifact has the required default metadata.
        This includes:
        - creation_time
        - dependency_hash
        """
        if artifact._creation_time is None:
            artifact._creation_time = float(time.time())
        if artifact._dependency_hash is "0":
            artifact._dependency_hash = dependency_hash
        return artifact

    def _get_cached_artifacts(self, stage_name, input_artifacts, backend):
        """
        Attempts to retrieve cached artifacts for the stage run,
        identified uniquely by its definition and the hash of its
        input artifacts.
        """
        stage = self._stages[stage_name]
        dependency_hash = Artifact.dependency_hash(input_artifacts)
        status = backend.pipeline_stage_run_status(
            stage, dependency_hash)
        if status == STAGE_COMPLETE or status == STAGE_IN_PROGRESS:
            cached_arts = backend.find_pipeline_stage_run_artifacts(
                stage._config, dependency_hash)
            self._log("Loaded %d cached artifacts for stage %s, dependency hash %s" %\
                      (len(cached_arts), stage_name, dependency_hash))
            loaded_arts = []
            for art in cached_arts:
                loaded = backend.load_artifact(art)
                loaded._loaded_from_cache = True
                loaded_arts.append(loaded)
            return loaded_arts
        else:
            return None

    async def _run_stage(self, stage_name, input_artifacts,
                         executor, backend, monitor,
                         pipeline_run_id=None,
    ):
        """
        Run a stage once we've acquired the input artifacts
        """
        # Check if the stage has already been run with the given
        # input artifacts and pipeline definition. If so,
        # return the cached run.
        cached_arts = self._get_cached_artifacts(stage_name,
                                                 input_artifacts,
                                                 backend)
        if cached_arts is not None:
            self._log("Found %d cached artifacts for stage %s" %
                      (len(cached_arts), stage_name))
            return cached_arts

        # We need to generate fresh artifacts.
        # We'll feed the input artifacts to the executor,
        # returning generated artifacts
        stage = self._stages[stage_name]
        result = []
        dependency_hash = Artifact.dependency_hash(input_artifacts)

        monitor.log_stage_run_init(stage, pipeline_run_id)
        task = executor.create_task(self._stages[stage_name],
                                    input_artifacts, monitor,
                                    pipeline_run_id)
        artifacts = await task.generate_artifacts()
        monitor.log_stage_run_complete(stage, pipeline_run_id)

        for art in artifacts:
            if hasattr(art, "_remotely_produced"):
                self._log("Remotely produced artifact for %s" % stage_name)
                result.append(art)
            else:
                self._log("Yielding fresh artifact for stage %s with dependency hash %s" %
                          (stage_name, dependency_hash))
                #self._log("\tPayload: %s " % str(art.item.payload)[0:50])
                art = self._ensure_artifact_meta(art, dependency_hash)
                backend.save_artifact(art)
                result.append(art)

        self._log("Done generating stage %s" % stage_name)
        backend.log_pipeline_stage_run_complete(stage, dependency_hash)
        return result

    async def generate_stage(self, stage_name, schedule,
                             executor, backend, monitor,
                             pipeline_run_id=None):
        """
        Acquire input artifacts for a stage and run it.
        """
        chain = self._build_chain(stage_name)

        # Create an input future for each input to this stage
        pre_reqs = chain.get_level(1)

        self._log("Generating stage %s" % stage_name)
        if pre_reqs is None or len(pre_reqs) == 0:
            # This stage does not require any input artifacts
            self._log("No inputs needed for stage %s" % stage_name)
            res = self._run_stage(stage_name, [], executor, backend, monitor, pipeline_run_id)
            return [res]
        else:
            # Schedule an input future with the arbiter
            input_future = InputFuture(stage_name)
            for pre_req in pre_reqs:
                input_future.add_input_source(pre_req)
            schedule(input_future)

            # Wait until the associated futures resolve
            artifactChunks = await input_future.await_artifacts()
            input_artifacts = []
            for artifactChunk in artifactChunks:
                input_artifacts += artifactChunk

            self._log("All (%d) inputs acquired for stage %s" %
                  (len(input_artifacts), stage_name))

            # Create futures for each unique combination of fanout parameters
            grouped = self.group_fanout_parameters(input_artifacts)
            runs = []
            for group in grouped:
                runs.append(self._run_stage(stage_name,
                                            group,
                                            executor,
                                            backend,
                                            monitor,
                                            pipeline_run_id
                ))
            return runs

    def group_fanout_parameters(self, input_artifacts):
        fanout_parameters = {}
        artifacts_without_fanout = []
        for artifact in input_artifacts:
            afp = artifact._fanout_parameters
            if len(list(afp.keys())) == 0:
                artifacts_without_fanout.append(artifact)
            for param in afp:
                if param not in fanout_parameters:
                    fanout_parameters[param] = {}
                if afp[param] not in fanout_parameters[param]:
                    fanout_parameters[param][afp[param]] = []
                fanout_parameters[param][afp[param]].append(artifact)

        # Create a cartesian product of unqiue values for each fanout parameter
        ordered = OrderedDict(fanout_parameters)
        ordered_keys = list(ordered.keys())
        products = list(itertools.product( *list(ordered.values()) ))
        self._log("Found fanout parameters: ")
        self._log(products)

        result = []
        for product in products:
            matching_artifacts = []
            for input_artifact in input_artifacts:
                # Find artifacts whose fanout parameters match ours
                conflicting_values = False
                for idx in range(len(product)):
                    param = ordered_keys[idx]
                    param_value = product[idx]
                    if param in input_artifact._fanout_parameters:
                        if input_artifact._fanout_parameters[param] != param_value:
                            conflicting_values = True
                            break
                if not conflicting_values:
                    matching_artifacts.append(input_artifact)
            result.append(matching_artifacts)
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
