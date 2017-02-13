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
import unittest
from tests import isolated_filesystem
from pipetree.config import PipelineStageConfig
from pipetree.providers import LocalDirectoryArtifactProvider,\
    LocalFileArtifactProvider,\
    ParameterArtifactProvider
from pipetree.exceptions import ArtifactSourceDoesNotExistError,\
    InvalidConfigurationFileError,\
    ArtifactProviderMissingParameterError,\
    ArtifactProviderFailedError


class TestParameterArtifactProvider(unittest.TestCase):
    def setUp(self):
        self.stage_config = PipelineStageConfig("test_stage_name", {
            "type": "ParameterPipelineStage"
        })
        self.test_parameters = {"int_param": 200, "str_param": "str"}
        pass

    def tearDown(self):
        pass

    def test_missing_config(self):
        try:
            provider = ParameterArtifactProvider(
                stage_config=None,
                parameters={})
            self.assertEqual(provider, "Provider creation should have failed")
        except ArtifactProviderMissingParameterError:
            pass

    def test_missing_parameters(self):
        try:
            provider = ParameterArtifactProvider(
                stage_config=self.stage_config,
                parameters={})
            self.assertEqual(provider, "Provider creation should have failed")
        except ArtifactProviderMissingParameterError:
            pass

    def test_yield_artifacts(self):
        provider = ParameterArtifactProvider(
            stage_config=self.stage_config,
            parameters=self.test_parameters)

        arts = provider.yield_artifacts()
        la = list(arts)
        self.assertEqual(1, len(la))
        yielded_params = la[0].payload
        for k in self.test_parameters:
            if k not in yielded_params:
                raise ArtifactProviderFailedError(
                    provider = self.__class__.__name__,
                    error="Missing parameter "+k
                )
    pass

class TestLocalFileArtifactProvider(unittest.TestCase):
    def setUp(self):
        self.dirname = 'foo'
        self.filename = ['foo.bar', 'foo.baz']
        self.filedatas = ['foo bar baz', 'helloworld']
        self.fs = isolated_filesystem()
        self.fs.__enter__()
        self.stage_config = PipelineStageConfig("test_stage_name", {
            "type": "LocalFilePipelineStage"
        })

        # Build directory structure
        os.makedirs(self.dirname)
        for name, data in zip(self.filename, self.filedatas):
            with open(os.path.join(os.getcwd(),
                                   self.dirname,
                                   name), 'w') as f:
                f.write(data)

    def tearDown(self):
        self.fs.__exit__(None, None, None)

    def test_missing_config(self):
        try:
            LocalFileArtifactProvider(path='folder/shim.sham',
                                           stage_config=None)
            self.assertEqual(0, "Provider creation should have failed")
        except ArtifactProviderMissingParameterError:
            pass

    def test_load_nonexistant_file(self):
        try:
            LocalFileArtifactProvider(path='folder/shim.sham',
                                           stage_config=self.stage_config)
            self.assertTrue(False, 'This was supposed to raise an exception')
        except ArtifactSourceDoesNotExistError:
            pass

    def test_yield_artifacts(self):
        provider = LocalFileArtifactProvider(
            path=os.path.join(self.dirname, self.filename[0]),
            stage_config=self.stage_config,
            read_content=True)
        arts = provider.yield_artifacts()
        la = list(arts)
        self.assertEqual(len(la), 1)

    def test_load_file_data(self):
        provider = LocalFileArtifactProvider(
            path=os.path.join(self.dirname, self.filename[0]),
            stage_config=self.stage_config,
            read_content=True)
        art = provider._yield_artifact()
        self.assertEqual(art.item.payload,
                         self.filedatas[0])


class TestLocalDirectoryArtifactProvider(unittest.TestCase):
    def setUp(self):
        self.dirname = 'foo'
        self.filename = ['foo.bar', 'foo.baz']
        self.filedatas = ['foo bar baz', 'helloworld']
        self.fs = isolated_filesystem()
        self.fs.__enter__()
        self.stage_config = PipelineStageConfig("test_stage_name", {
            "type": "LocalDirectoryPipelineStage"
        })

        # Build directory structure
        os.makedirs(self.dirname)
        for name, data in zip(self.filename, self.filedatas):
            with open(os.path.join(os.getcwd(),
                                   self.dirname,
                                   name), 'w') as f:
                f.write(data)

    def tearDown(self):
        self.fs.__exit__(None, None, None)

    def test_missing_config(self):
        try:
            LocalDirectoryArtifactProvider(path='folder/',
                                           stage_config=None)
            self.assertEqual(0, "Provider creation should have failed")
        except ArtifactProviderMissingParameterError:
            pass

    def test_load_nonexistant_dir(self):
        try:
            LocalDirectoryArtifactProvider(path='folder/',
                                           stage_config=self.stage_config)
            self.assertTrue(False, 'This was supposed to raise an exception')
        except ArtifactSourceDoesNotExistError:
            pass

    def test_load_file_data(self):
        provider = LocalDirectoryArtifactProvider(path=self.dirname,
                                                  stage_config=self.stage_config,
                                                  read_content=True)
        art = provider._yield_artifact(self.filename[0])
        self.assertEqual(art.item.payload.decode('utf-8'),
                         self.filedatas[0])

    def test_load_file_names(self):
        provider = LocalDirectoryArtifactProvider(path=self.dirname,
                                                  stage_config=self.stage_config)
        for loaded_name, name in zip(provider.yield_artifacts(),
                                     self.filename):
            self.assertEqual(loaded_name, os.path.join(os.getcwd(),
                                                       self.dirname,
                                                       name))

    def test_load_multiple_file_contents(self):
        provider = LocalDirectoryArtifactProvider(path=self.dirname,
                                                  stage_config=self.stage_config,
                                                  read_content=True)
        for art, data in zip(provider.yield_artifacts(),
                             self.filedatas):
            art_data = art.item.payload
            self.assertEqual(art_data.decode('utf-8'), data)
