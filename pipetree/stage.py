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
from pipetree.storage import LocalFileArtifactProvider
from pipetree.exceptions import InvalidConfigurationFileError


class BasePipelineStage(object):
    """Base class for a pipeline stage"""
    def __init__(self, config):
        if self._validate_config(config):
            self._config = config

    @property
    def name(self):
        return self._config.name

    def validate_prereqs(self, previous_stages):
        raise NotImplementedError

    def _source_artifact(self, artifact_name):
        raise NotImplementedError

    def _yield_artifacts(self):
        raise NotImplementedError

    def _validate_config(self):
        raise NotImplementedError


class LocalDirectoryPipelineStage(BasePipelineStage):
    """A pipeline stage for sourcing files from a directory"""

    def __init__(self, config):
        super().__init__(config)
        self._artifact_source = LocalFileArtifactProvider(config.filepath)

    def validate_prereqs(self, previous_stages):
        return True

    def _source_artifact(self, artifact_name):
        pass

    def _yield_artifacts(self):
        pass

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
    def __init__(self, config):
        super().__init__(config)

    def validate_prereqs(self, previous_stages):
        for input in self.inputs:
            if input not in previous_stages.keys():
                raise InvalidConfigurationFileError(
                    configurable=self.__class__.__name__,
                    reason='input stage \'%s\' not found in pipeline' % input)
        return True

    def _source_artifact(self, artifact_name):
        pass

    def _yield_artifacts(self):
        pass

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
