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
from pipetree.utils import name_is_pythonic, attach_config_to_object


class TestUtils(unittest.TestCase):
    def test_is_pythonic_name(self):
        self.assertFalse(name_is_pythonic('hello there'))
        self.assertFalse(name_is_pythonic('1this_is_a_test'))
        self.assertTrue(name_is_pythonic('hello_there'))
        self.assertTrue(name_is_pythonic('FelloThereMaiFriend'))
        self.assertTrue(name_is_pythonic('_function_name_'))

    def test_attach_config_to_object(self):
        config = {
            'integer': 1,
            'numlist': [1, 2, 3],
            'string': 'foobarbaz'
        }
        obj = type("TestObj", (object,), {})
        attach_config_to_object(obj, config)
        for k, v in config.items():
            self.assertEqual(v, getattr(obj, k))
