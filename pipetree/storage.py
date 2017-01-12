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
import copy
from pipetree.utils import attach_config_to_object
from pipetree.exceptions import ArtifactSourceDoesNotExistError,\
    ArtifactProviderMissingParameterError
from pipetree.artifact import Artifact
#from pipetree.config import PipelineStageConfig


class ArtifactProvider(object):
    def __init__(self, **kwargs):
        config = copy.copy(self.DEFAULTS)
        config.update(kwargs)
        self._validate_config()
        self._config = kwargs
        attach_config_to_object(self, config)

    def _validate_config(self):
        raise NotImplementedError

    def _ensure_base_meta(self, art):
        return art

    def yield_artifacts(self):
        for art in self._yield_artifacts():
            yield self._ensure_base_meta(art)


class ParameterArtifactProvider(ArtifactProvider):
    DEFAULTS = {
    }

    def __init__(self, parameters={}, stage_config=None, **kwargs):
        super().__init__(
            parameters=parameters,
            stage_config=stage_config,
            **kwargs)
        if stage_config is None:
            raise ArtifactProviderMissingParameterError(
                provider=self.__class__.__name__,
                parameter="stage_config")
        if len(list(parameters.keys())) is 0:
            raise ArtifactProviderMissingParameterError(
                provider=self.__class__.__name__,
                parameter="parameters")

        self._parameters = parameters
        self._stage_config = stage_config

    def _validate_config(self):
        pass

    def _yield_artifacts(self):
        yield self._yield_artifact()

    def _yield_artifact(self):
        art = Artifact(self._stage_config)
        art.payload = self._parameters
        return art


class LocalFileArtifactProvider(ArtifactProvider):
    DEFAULTS = {
    }

    def __init__(self, path='', stage_config=None, **kwargs):
        super().__init__(path=path, stage_config=stage_config, **kwargs)
        if stage_config is None:
            raise ArtifactProviderMissingParameterError(
                provider=self.__class__.__name__,
                parameter="stage_config")
        self._stage_config = stage_config
        self._path = path
        self._validate_file()

    def _validate_config(self):
        pass

    def _validate_file(self):
        if not os.path.isfile(self.path):
            if not os.access(self.path, os.R_OK):
                raise ArtifactSourceDoesNotExistError(
                    provider=self.__class__.__name__,
                    source='file: %s' % os.path.join(os.getcwd(), self.path))

    def _yield_artifacts(self):
        yield self._yield_artifact()

    def _yield_artifact(self):
        artifact_path = os.path.join(os.getcwd(), self._path)
        content = ""
        with open(artifact_path, 'rb') as f:
            content = f.read()

        art = Artifact(self._stage_config)
        art.item.payload = content
        return art


class LocalDirectoryArtifactProvider(ArtifactProvider):
    DEFAULTS = {
        'read_content': False
    }

    def __init__(self, path='', stage_config=None, **kwargs):
        super().__init__(path=path, stage_config=None, **kwargs)
        if stage_config is None:
            raise ArtifactProviderMissingParameterError(
                provider=self.__class__.__name__,
                parameter="stage_config")
        self._stage_config = stage_config
        self._root = path
        self._validate_dir()

    def _validate_config(self):
        pass

    def _validate_dir(self):
        if not os.path.isdir(self._root):
            raise ArtifactSourceDoesNotExistError(
                provider=self.__class__.__name__,
                source='directory: %s' % os.path.join(os.getcwd(), self._root))

    def _yield_artifacts(self):
        for entry in os.listdir(self._root):
            yield self._yield_artifact(entry)

    def _yield_artifact(self, artifact_name):
        artifact_path = os.path.join(os.getcwd(),
                                     self._root,
                                     artifact_name)
        if self.read_content:
            with open(artifact_path, 'rb') as f:
                art = Artifact(self._stage_config)
                art.item.payload = f.read()
                return art
        return artifact_path
