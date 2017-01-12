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

from pipetree.exceptions import ArtifactMissingPayloadError
from pipetree.backend import LocalArtifactBackend, STAGE_COMPLETE, STAGE_DOES_NOT_EXIST, STAGE_IN_PROGRESS
from pipetree.config import PipelineStageConfig
from pipetree.artifact import Artifact, Item


class TestLocalArtifactBackend(unittest.TestCase):
    def setUp(self):
        self.dirname = 'foo'
        self.filename = ['foo.bar', 'foo.baz']
        self.filedatas = ['foo bar baz', 'helloworld']

        self.fs = isolated_filesystem()
        self.fs.__enter__()

        self.stage_config = PipelineStageConfig("test_stage_name", {
            "type": "ParameterPipelineStage"
        })

        self.stage_config = PipelineStageConfig("test_stage_name", {
            "type": "ParameterPipelineStage"
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

    def test_save_missing_payload(self):
        backend = LocalArtifactBackend(config={"path": "./test_storage/"})
        artifact = Artifact(self.stage_config)
        try:
            backend.save_artifact(artifact)
            self.assertEqual(0, "The above line should fail " +
                             "due to the artifact having no payload")
            self.fail()
        except ArtifactMissingPayloadError:
            pass

    def test_save_artifact(self):
        backend = LocalArtifactBackend(config={"path": "./test_storage/"})
        artifact = Artifact(self.stage_config)
        artifact.item = Item(payload="SHRIM")
        backend.save_artifact(artifact)
        pass

    def test_load_artifact(self):
        backend = LocalArtifactBackend(config={"path": "./test_storage/"})
        artifact = Artifact(self.stage_config)
        payload = "SHRIM"
        artifact.item = Item(payload="SHRIM")
        backend.save_artifact(artifact)

        loaded_artifact = backend.load_artifact(artifact)
        self.assertEqual(loaded_artifact.item.payload, artifact.item.payload)

    def test_find_artifact(self):
        backend = LocalArtifactBackend(config={"path": "./test_storage/"})
        artifact = Artifact(self.stage_config)
        payload = "SHRIM"
        artifact.item = Item(payload="SHRIM")
        backend.save_artifact(artifact)

        loaded_artifact = backend._find_cached_artifact(artifact)

        # Ensure that we found the artifact
        self.assertNotEqual(None, loaded_artifact)

        # Ensure that the artifact doesn't have a payload
        self.assertNotEqual(None, loaded_artifact.item)
        self.assertEqual(None, loaded_artifact.item.payload)

        # Ensure that meta properties are correctly set on artifact
        self.assertEqual(loaded_artifact._specific_hash,
                         artifact._specific_hash)
        self.assertEqual(loaded_artifact._dependency_hash,
                         artifact._dependency_hash)
        self.assertEqual(loaded_artifact._definition_hash,
                         artifact._definition_hash)
        self.assertEqual(loaded_artifact._pipeline_stage,
                         artifact._pipeline_stage)
        self.assertEqual(loaded_artifact.item.type,
                         artifact.item.type)

    def test_user_meta(self):
        backend = LocalArtifactBackend(config={"path": "./test_storage/"})
        artifact = Artifact(self.stage_config)
        payload = "SHRIM"
        artifact.item = Item(payload=payload)
        backend.save_artifact(artifact)

        loaded_artifact = backend.load_artifact(artifact)
        self.assertEqual(loaded_artifact.item.payload, payload)

    def test_pipeline_stage_run_meta(self):
        backend = LocalArtifactBackend(config={"path": "./test_storage/"})
        artifact = Artifact(self.stage_config)
        payload = "SHRIM"
        artifact.item = Item(payload=payload)
        backend.save_artifact(artifact)

        backend.log_pipeline_stage_run_complete(
            self.stage_config,
            artifact._dependency_hash)
        
        arts = backend.find_pipeline_stage_run_artifacts(
            self.stage_config,
            artifact._dependency_hash)
        self.assertEqual(len(arts), 1)
        self.assertEqual(arts[0].get_uid(), artifact.get_uid())

    def test_pipeline_stage_status(self):        
        backend = LocalArtifactBackend(config={"path": "./test_storage/"})
        artifact = Artifact(self.stage_config)
        payload = "SHRIM"
        artifact.item = Item(payload=payload)

        status = backend.pipeline_stage_run_status(
            self.stage_config,
            artifact._dependency_hash)
        self.assertEqual(status, STAGE_DOES_NOT_EXIST)

        backend.save_artifact(artifact)

        status = backend.pipeline_stage_run_status(
            self.stage_config,
            artifact._dependency_hash)
        self.assertEqual(status, STAGE_IN_PROGRESS)


        backend.log_pipeline_stage_run_complete(
            self.stage_config,
            artifact._dependency_hash)
        status = backend.pipeline_stage_run_status(
            self.stage_config,
            artifact._dependency_hash)
        self.assertEqual(status, STAGE_COMPLETE)

        meta = backend._get_pipeline_stage_run_meta(
            self.stage_config,
            artifact._dependency_hash)

        self.assertEqual(len(meta['artifacts']), 1)
