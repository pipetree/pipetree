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

import hashlib
import inspect
import json
from pipetree import STAGES
from pipetree.utils import attach_config_to_object
from pipetree.exceptions import IncorrectPipelineStageNameError,\
    NonPythonicNameError, MissingPipelineAttributeError
from pipetree.utils import name_is_pythonic


class PipelineStageConfig(object):
    def __init__(self, key, data):
        if not name_is_pythonic(key):
            raise NonPythonicNameError(symbol=key)
        if not isinstance(data, dict):
            raise TypeError('Expected type \'dict\', found %s instead.'
                            % type(data))
        attach_config_to_object(self, data)
        self.name = key

        if not hasattr(self, "type"):
            raise MissingPipelineAttributeError(
                attribute="type",
                stage_name=key)
        if not self.type.endswith('PipelineStage'):
            self.type += 'PipelineStage'
        if self.type not in STAGES:
            raise IncorrectPipelineStageNameError(
                types=list(STAGES.keys()))
        self.parent_class = STAGES[self.type]

    def hash(self):
        """
        Hash a pipeline stage in an idempotent way
        """

        ignore = ['parent_class']
        props = {k: getattr(self, k)
                 for k in dir(self)
                 if not k.startswith('__')
                 and not inspect.ismethod(getattr(self, k))
                 and k not in ignore}
        h = hashlib.md5()
        stage_json = json.dumps(props, sort_keys=True)
        h.update(str(stage_json).encode('utf-8'))
        return str(h.hexdigest())
