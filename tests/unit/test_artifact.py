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
from pipetree.artifact import Artifact
from pipetree.config import PipelineStageConfig
from pipetree.exceptions import InvalidArtifactMetadataError


class TestArtifact(unittest.TestCase):
    def setUp(self):
        pass

    def test_stage_definition_hash_uniqueness(self):
        stage_a = PipelineStageConfig(
            'some_name',
            {"foo": "bar", "type": "ExecutorPipelineStage"})
        stage_b = PipelineStageConfig(
            'some_name',
            {"foo": "quux", "type": "ExecutorPipelineStage"})
        art_a = Artifact(stage_a)
        art_b = Artifact(stage_b)
        self.assertNotEqual(art_a._definition_hash, art_b._definition_hash)

    def test_stage_definition_hash_idempotence(self):
        stage_a = PipelineStageConfig(
            'some_name',
            {"A": 1, "B": 2, "type": "ExecutorPipelineStage"})
        stage_b = PipelineStageConfig(
            'some_name',
            {"B": 2, "A": 1, "type": "ExecutorPipelineStage"})
        art_a = Artifact(stage_a)
        art_b = Artifact(stage_b)
        self.assertEqual(art_a._definition_hash, art_b._definition_hash)

    def test_metadata_from_dict(self):
        stage_a = PipelineStageConfig(
            'some_name',
            {"A": 1, "B": 2, "type": "ExecutorPipelineStage"})
        art_a = Artifact(stage_a)
        d = {
            "antecedents": {},
            "creation_time": 124566722.3,
            "definition_hash": "dac9630aec642a428cd73f4be0a03569",
            "specific_hash": "bc1687bbb3b97214d46b7c30ab307cc1",
            "dependency_hash": "ecad5fc98abf66565e009155f5e57dda",
            "serialization_type": "json",
            "pipeline_stage": "some_stage",
            "item": {
                "meta": {"loss": 0.2},
                "tags": ["my_pipeline_run"],
                "type": "my_item_type"
            }
        }
        art_a.meta_from_dict(d)

        for prop in d:
            if prop == "item":
                for iprop in d['item']:
                    value = getattr(art_a.item, iprop)
                    self.assertEqual(d['item'][iprop], value)
            else:
                value = getattr(art_a, "_" + prop)
                self.assertEqual(d[prop], value)

    def test_metadata_from_bad_dict(self):
        stage_a = PipelineStageConfig(
            'some_name',
            {"A": 1, "B": 2, "type": "ExecutorPipelineStage"})
        art_a = Artifact(stage_a)
        try:
            art_a.meta_from_dict({})
            self.fail()
        except InvalidArtifactMetadataError:
            pass

    def test_generate_metadata(self):
        stage_a = PipelineStageConfig(
            'some_name',
            {"A": 1, "B": 2, "type": "ExecutorPipelineStage"})
        art_a = Artifact(stage_a)
        d = art_a.meta_to_dict()
        for m in art_a._meta_properties:
            if m not in d:
                self.fail()
