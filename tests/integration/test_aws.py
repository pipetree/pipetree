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
import string
import base64
import random
import unittest
from pipetree.aws import Aws

ALPHABET = string.ascii_lowercase

def generate_random_bucket_name():
    return 'pipetreetest-s3integ-' + ''.join(
        [random.choice(ALPHABET) for i in range(20)])


class TestS3(unittest.TestCase):
    def setUp(self):
        self.client = Aws()

    def test_nonexistant_bucket(self):
        response = self.client.can_access_bucket(
            generate_random_bucket_name())
        self.assertFalse(response)

    def test_create__destroy_bucket(self):
        name = generate_random_bucket_name()
        create_response = self.client.create_bucket(name)
        self.assertTrue(create_response)
        destroy_response = self.client.destroy_bucket(name)
        self.assertTrue(destroy_response)
