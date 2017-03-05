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
import time
import inspect
import importlib.util
from pipetree.providers import LocalDirectoryArtifactProvider,\
    LocalFileArtifactProvider,\
    ParameterArtifactProvider, \
    GridSearchArtifactProvider
from pipetree.exceptions import InvalidConfigurationFileError
from pipetree.artifact import Artifact, Item


class BasePipelineStage(object):
    """Base class for a pipeline stage"""
    def __init__(self, config):
        if self._validate_config(config):
            self._config = config

    def validate_prereqs(self, previous_stages):
        raise NotImplementedError

    def _source_artifact(self, artifact_name):
        raise NotImplementedError

    def yield_artifacts(self, input_artifacts=None):
        raise NotImplementedError

    def _validate_config(self):
        raise NotImplementedError

    def _wrap_item_in_artifact(self, item):
        raise NotImplementedError


class ParameterPipelineStage(BasePipelineStage):
    """A pipeline stage for fixed parameters"""

    def __init__(self, config):
        super().__init__(config)

        params = self.params_from_config(config)
        self._artifact_source = ParameterArtifactProvider(
            stage_config=config,
            parameters=params)
        self._local_stage = True

    def params_from_config(self, config):
        """ Extract parameters from a config"""
        exclude = ['type', 'raw_config', 'parent_class', 'name']
        params = {}
        for prop in dir(config):
            value = getattr(config, prop)
            if not prop.startswith('__') and not inspect.ismethod(value)\
               and prop not in exclude:
                params[prop] = value
        return params

    def validate_prereqs(self, previous_stages):
        return True

    def _source_artifact(self, artifact_name):
        pass

    def yield_artifacts(self, input_artifacts=None):
        for art in self._artifact_source.yield_artifacts():
            yield art

    def _validate_config(self, config):
        """
        Raise an exception if the config is invald
        """
        return True

class GridSearchPipelineStage(BasePipelineStage):
    """A pipeline stage for gridsearch"""

    def __init__(self, config):
        super().__init__(config)

        params = self.params_from_config(config)
        self._artifact_source = GridSearchArtifactProvider(
            stage_config=config,
            parameters=params)
        self._local_stage = False

    def params_from_config(self, config):
        """ Extract parameters from a config"""
        exclude = ['type', 'raw_config', 'parent_class', 'name']
        params = {}
        for prop in dir(config):
            value = getattr(config, prop)
            if not prop.startswith('__') and not inspect.ismethod(value)\
               and prop not in exclude:
                params[prop] = value
        return params

    def validate_prereqs(self, previous_stages):
        return True

    def _source_artifact(self, artifact_name):
        pass

    def yield_artifacts(self, input_artifacts=None):
        for art in self._artifact_source.yield_artifacts():
            yield art

    def _validate_config(self, config):
        """
        Raise an exception if the config is invald
        """
        return True



class LocalFilePipelineStage(BasePipelineStage):
    """A pipeline stage for sourcing a single file locally"""

    def __init__(self, config):
        super().__init__(config)
        self._artifact_source = LocalFileArtifactProvider(
            path=config.filepath,
            stage_config=config)
        self._local_stage = True

    def validate_prereqs(self, previous_stages):
        return True

    def _source_artifact(self, artifact_name):
        pass

    def yield_artifacts(self, input_artifacts=None):
        for art in self._artifact_source.yield_artifacts():
            yield art

    def _validate_config(self, config):
        """
        Raise an exception if the config is invald
        """
        if not hasattr(config, 'filepath'):
            raise InvalidConfigurationFileError(
                configurable=self.__class__.__name__,
                reason='expected \'filepath\' entry of type string.')
        return True


class IdentityPipelineStage(BasePipelineStage):
    """A pipeline stage that returns its inputs exactly"""

    def __init__(self, config):
        super().__init__(config)

    def validate_prereqs(self, previous_stages):
        for input in self.inputs:
            if input not in previous_stages.keys():
                raise InvalidConfigurationFileError(
                    configurable=self.__class__.__name__,
                    reason='input stage \'%s\' not found in pipeline' % input)
        return True

    def _validate_config(self, config):
        if not hasattr(config, 'inputs'):
            raise InvalidConfigurationFileError(
                configurable=self.__class__.__name__,
                reason='expected \'input\' entry of type array')
        if not isinstance(config.inputs, list):
            raise InvalidConfigurationFileError(
                configurable=self.__class__.__name__,
                reason='expected \'input\' to be an array, found a %s'
                % type(config.inputs))
        return True

    def validate_prereqs(self, previous_stages):
        return True

    def _source_artifact(self, artifact_name):
        pass

    def yield_artifacts(self, input_artifacts):
        for artifact in input_artifacts:
            new_artifact = Artifact(self._config,
                                    artifact.item,
                                    serialization_type=artifact._serialization_type
            )
            new_artifact._specific_hash = artifact._specific_hash
            yield new_artifact

    def _validate_config(self, config):
        return True


class LocalDirectoryPipelineStage(BasePipelineStage):
    """A pipeline stage for sourcing files from a directory"""

    def __init__(self, config):
        super().__init__(config)
        self._artifact_source = LocalDirectoryArtifactProvider(
            path=config.filepath,
            stage_config=config)
        self._local_stage = True

    def validate_prereqs(self, previous_stages):
        return True

    def _source_artifact(self, artifact_name):
        pass

    def yield_artifacts(self, *args, **kwargs):
        return self._artifact_source.yield_artifacts()

    def _validate_config(self, config):
        """
        Raise an exception if the config is invald
        """
        if not hasattr(config, 'filepath'):
            raise InvalidConfigurationFileError(
                configurable=self.__class__.__name__,
                reason='expected \'filepath\' entry of type string.')
        return True


class ExecutorPipelineStage(BasePipelineStage):
    """
    Pipeline stage for executing user-supplied functions.

    Arguments are passed to the function in the following way:
    fn(stage_A_name={"itemTypeA": [Item0, Item1, ...], "itemTypeB": [...]},
       stage_B_name={...})

    If a stage only produces items without a type, then the list of output artifacts
    will be passed as a simple array. I.e) simple_stage=[Item0, Item1, Item2]

    If config['full_artifacts'] == True, then Artifact objects will be passed
    instead of Items

    """
    def __init__(self, config, path=None):
        super().__init__(config)
        self._module = None
        self._callable = None
        self._fn = None
        self._load_module(config)

    def _load_module(self, config):
        path = os.path.join(*config.execute.split('.')[:-1]) + '.py'

        if not hasattr(self._config, 'directory'):
            path = os.path.join(os.getcwd(), path)
        else:
            path = os.path.join(self._config.directory, path)
        spec = importlib.util.spec_from_file_location(
            config.name,
            path)
        if not hasattr(self._config, 'full_artifacts'):
            self.full_artifacts = False
        self._module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self._module)
        self._callable = config.execute.split('.')[-1]
        self._fn = getattr(self._module, self._callable)

    def validate_prereqs(self, previous_stages):
        for input in self.inputs:
            if input not in previous_stages.keys():
                raise InvalidConfigurationFileError(
                    configurable=self.__class__.__name__,
                    reason='input stage \'%s\' not found in pipeline' % input)
        return True

    def _source_artifact(self, artifact_name):
        pass

    def yield_artifacts(self, input_artifacts=[]):
        kwargs = {}
        for artifact in input_artifacts:
            if artifact._pipeline_stage not in kwargs:
                kwargs[artifact._pipeline_stage] = {}
            if artifact.item.type not in kwargs[artifact._pipeline_stage]:
                kwargs[artifact._pipeline_stage][artifact.item.type] = []
            if self.full_artifacts:
                kwargs[artifact._pipeline_stage][artifact.item.type].append(artifact)
            else:
                kwargs[artifact._pipeline_stage][artifact.item.type].append(artifact.item)
        keys = list(kwargs.keys())

        # If there are no item types produced by previous stages, present
        # the items as a list instead of {None: [...]}
        for key in keys:
            if list(kwargs[key].keys())[0] is None:
                kwargs[key] = kwargs[key][None]

        for item in self._fn(**kwargs):
            art = Artifact(self._config,
                     item=item,
                     serialization_type=item.payload_type)
            art._specific_hash = art.specific_hash_from_payload()
            yield art

    def _validate_config(self, config):
        if not hasattr(config, 'inputs'):
            raise InvalidConfigurationFileError(
                configurable=self.__class__.__name__,
                reason='expected \'input\' entry of type array')
        if not isinstance(config.inputs, list):
            raise InvalidConfigurationFileError(
                configurable=self.__class__.__name__,
                reason='expected \'input\' to be an array, found a %s'
                % type(config.inputs))
        if not hasattr(config, 'execute'):
            raise InvalidConfigurationFileError(
                configurable=self.__class__.__name__,
                reason='expected \'execute\' entry of type string')
        return True


class PipelineStageFactory(object):
    def create_pipeline_stage(self, pipeline_config):
        stage = self._create_pipeline_stage_class(pipeline_config)
        return stage

    def _create_pipeline_stage_class(self, pipeline_config):
        class_attributes = self._generate_attributes(pipeline_config)
        parent_class = tuple([pipeline_config.parent_class])
        class_name = pipeline_config.name
        cls = type(class_name, parent_class, class_attributes)
        return cls(pipeline_config)

    def _generate_attributes(self, config):
        return {k: getattr(config, k) for k in
                [a for a in dir(config) if not a.startswith('__')]}
