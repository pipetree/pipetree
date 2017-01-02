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
from pipetree.config import PipelineStageConfig
from pipetree.exceptions import NonPythonicNameError, IncorrectPipelineStageNameError, MissingPipelineAttributeError

class TestPipelineStageConfig(unittest.TestCase):
    def setUp(self):
        pass

    def test_initialize_with_no_type(self):
        try:
            PipelineStageConfig('valid_name', {})
            print('This should have raised an error, the config has no type')
            self.fail()
        except MissingPipelineAttributeError:
            pass

    def test_initialize_with_bad_key(self):
        try:
            PipelineStageConfig('invalid name', {})
            print('This should have raised an error, the name is invalid')
            self.fail()
        except NonPythonicNameError:
            pass

    def test_initialize_with_bad_dictionary(self):
        try:
            PipelineStageConfig('valid_name', 'not a dict')
            print('This should have raised an error, the value isn\'t a dict')
            self.fail()
        except TypeError:
            pass

    def test_initialize_with_bad_type(self):
        try:
            PipelineStageConfig('valid_name', {
                'type': 'InvalidPipelineType'
            })
            print('This should have raised an error, the type is invalid.')
            self.fail()
        except IncorrectPipelineStageNameError:
            pass
