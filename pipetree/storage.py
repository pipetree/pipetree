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
import copy
from pipetree.utils import attach_config_to_object
from pipetree.exceptions import ArtifactSourceDoesNotExistError


class ArtifactProvider(object):
    def __init__(self, **kwargs):
        config = copy.copy(self.DEFAULTS)
        config.update(kwargs)
        self._validate_config()
        self._config = kwargs
        attach_config_to_object(self, config)

    def _validate_config(self):
        raise NotImplementedError


class LocalFileArtifactProvider(ArtifactProvider):
    DEFAULTS = {
        'read_content': False
    }

    def __init__(self, path='', **kwargs):
        super().__init__(path=path, **kwargs)
        self._root = path
        self._validate_dir()

    def _validate_config(self):
        pass

    def _validate_dir(self):
        if not os.path.isdir(self._root):
            raise ArtifactSourceDoesNotExistError(
                provider=self.__class__.__name__,
                source='directory: %s' % os.path.join(os.getcwd(), self._root))

    def yield_artifacts(self):
        for entry in os.listdir(self._root):
            yield self.yield_artifact(entry)

    def yield_artifact(self, artifact_name):
        artifact_path = os.path.join(os.getcwd(),
                                     self._root,
                                     artifact_name)
        if self.read_content:
            with open(artifact_path, 'rb') as f:
                return f.read()
        return artifact_path
