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
import sys
import os

from  pipetree.storage import local, storage

class TestBasicLocal(unittest.TestCase):
    def test_filehandle(self):
        """
        Test local file pipeline creation
        """
        testcontents = 'gorb' * 1000
        pipeline_item = {"name": "test_file",
                         "type": storage.PLI_FILE_TYPE,
                         "local_filepath": "./testfile"}
        
        with open('testfile', 'w') as f:
            f.write(testcontents)        
        with local.file_handle(pipeline_item, {}) as fh:
            contents = fh.read()
            self.assertEqual(testcontents, contents)
        
    def test_versions(self):
        """
        Test that when we create a new version of a pipeline item,
        that item becomes the default and has a new hash
        """
        self.assertTrue(True)

    def test_pruning(self):
        """
        Test that files get pruned after a certain number of versions,
        according to the given policy
        """
        self.assertTrue(True)
