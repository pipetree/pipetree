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
import unittest
from tests import isolated_filesystem
from pipetree.storage import LocalFileArtifactProvider
from pipetree.exceptions import ArtifactSourceDoesNotExistError,\
    InvalidConfigurationFileError


class TestLocalFileProvider(unittest.TestCase):
    def setUp(self):
        self.dirname = 'foo'
        self.filename = ['foo.bar', 'foo.baz']
        self.filedatas = ['foo bar baz', 'helloworld']
        self.fs = isolated_filesystem()
        self.fs.__enter__()

        # Build directory structure
        os.makedirs(self.dirname)
        for name, data in zip(self.filename, self.filedatas):
            with open(os.path.join(os.getcwd(),
                                   self.dirname,
                                   name), 'w') as f:
                f.write(data)

    def tearDown(self):
        self.fs.__exit__(None, None, None)

    def test_load_nonexistant_dir(self):
        try:
            LocalFileArtifactProvider(path='folder/')
            self.assertTrue(False, 'This was supposed to raise an exception')
        except ArtifactSourceDoesNotExistError:
            pass

    def test_load_file_data(self):
        provider = LocalFileArtifactProvider(path=self.dirname,
                                             read_content=True)
        data = provider.yield_artifact(self.filename[0])
        self.assertEqual(data.decode('utf-8'),
                         self.filedatas[0])

    def test_load_file_names(self):
        provider = LocalFileArtifactProvider(path=self.dirname)
        for loaded_name, name in zip(provider.yield_artifacts(),
                                     self.filename):
            self.assertEqual(loaded_name, os.path.join(os.getcwd(),
                                                       self.dirname,
                                                       name))

    def test_load_multiple_file_contents(self):
        provider = LocalFileArtifactProvider(path=self.dirname,
                                             read_content=True)
        for block, data in zip(provider.yield_artifacts(),
                               self.filedatas):
            self.assertEqual(block.decode('utf-8'), data)
