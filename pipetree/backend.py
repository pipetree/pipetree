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
import copy
import distutils.dir_util
import json
import threading

from pipetree.utils import attach_config_to_object
from pipetree.exceptions import ArtifactMissingPayloadError
from pipetree.artifact import Artifact

STAGE_COMPLETE = 'complete'
STAGE_IN_PROGRESS = 'in_progress'
STAGE_DOES_NOT_EXIST = 'does_not_exist'

class ArtifactBackend(object):
    def __init__(self, **kwargs):
        config = copy.copy(self.DEFAULTS)
        config.update(kwargs)
        self._validate_config()
        self._config = kwargs
        attach_config_to_object(self, config)

    def _validate_config(self):
        raise NotImplementedError

    def load_artifact(self, artifact):
        """
        Returns a fully instantiated artifact with an Item containing
        payload and metadata. This occurs
        iff one is found matching the provided hashes/properties
        of the given artifact object. Otherwise returns None.
        """
        cached_artifact = self._find_cached_artifact(artifact)
        if cached_artifact is None:
            return None
        else:
            cached_artifact.item.payload = self._get_cached_artifact_payload(
                cached_artifact)
            return cached_artifact

    def save_artifact(self, artifact):
        """
        Saves an artifact at every layer of the cache.

        Artifact must have all necessary metadata, including:
         - specific_hash
         - dependency_hash
         - definition_hash
        """
        raise NotImplementedError

    def log_pipeline_stage_run_complete(self, dependency_hash, definition_hash):
        """
        Record that the pipeline stage run for the given dependency hash and
        definition hash completed successfully.
        """
        raise NotImplementedError

    def pipeline_stage_run_status(self, dependency_hash, definition_hash):
        """
        Returns the status of a pipeline stage run
        Returns one of: "does_not_exist", "in_progress", "completed"
        """
        raise NotImplementedError

    def find_pipeline_stage_run_artifacts(self, dependency_hash, definition_hash):
        """
        Finds all artifacts for a given pipeline run.
        """
        raise NotImplementedError

    def _find_cached_artifact(self, artifact):
        """
        Tries to find a cached artifact matching the provided hashes/properties
        of the given artifact object.

        If only stage & item name are supplied, will return the newest artifact
        given the pruning ordering.

        Loads the metadata, but not the payload of an artifact.
        """
        raise NotImplementedError

    def _get_cached_artifact_payload(self, artifact):
        """
        Returns the payload for a given artifact, assuming that it
        has already been produced and is cached.
        """
        raise NotImplementedError

    def _get_cached_artifact_metadata(self, artifact):
        """
        Returns the artifact metadata for a given artifact, assuming that it
        has already been produced and is cached.
        """
        raise NotImplementedError

    def _sorted_artifacts(self, artifact):
        """
        Returns a sorted list of artifacts, based upon pruning ordering
        """
        raise NotImplementedError


class LocalArtifactBackend(ArtifactBackend):
    """
    Provide a local cache layer for artifacts.
    Intended to be composed with S3ArtifactBackend to provide local storage.

    Utilizes a global internal lock to ensure serial access to files.
    Since the majority of execution time is spent generating individual
    artifacts, this shouldn't impact performance tremendously at the moment.
    """
    DEFAULTS = {
        "path": "~/.pipetree/local_cache/",
        "metadata_file": "pipeline.meta"
    }

    def __init__(self, path=DEFAULTS['path'], **kwargs):
        super().__init__(path=path, **kwargs)
        if not os.path.exists(self.path):
            distutils.dir_util.mkpath(self.path)
        self.cached_meta = {}
        self._write_lock = threading.Lock()

    def _validate_config(self):
        return True

    def _relative_artifact_dir(self, artifact):
        """
        Returns the relative directory to house artifacts from a given
        stage and of a given item type.
        """
        it = "default"
        if artifact.item is not None and artifact.item.type is not None:
            it = artifact.item.type
        return os.path.join(artifact._pipeline_stage, it)

    def _relative_artifact_path(self, artifact):
        """
        Returns the relative path for an artifact that has a specified
        specific_hash, dependency_hash, definition_hash, stage and item type
        """
        return os.path.join(self._relative_artifact_dir(artifact),
                            artifact.get_uid())

    def save_artifact(self, artifact):
        """
        Saves an artifact locally on disk.

        Artifact must have all necessary metadata, including:
         - specific_hash
         - dependency_hash
         - definition_hash
        """
        # TODO: Check if the file exists. If it does, skip writing it out.
        if artifact.item is None or artifact.item.payload is None:
            raise ArtifactMissingPayloadError(stage=artifact._pipeline_stage)

        distutils.dir_util.mkpath(os.path.join(
            self.path,
            self._relative_artifact_dir(artifact)))

        with self._write_lock:
            with open(os.path.join(self.path,
                                   self._relative_artifact_path(artifact)),
                      'w') as f:
                f.write(artifact.item.payload)

        self._write_artifact_meta(artifact)
        self._record_pipeline_stage_run_artifact(artifact)

    def _load_item_meta(self, pipeline_stage, item_type):
        """
        Load the shared metadata for all artifacts of the
        given stage & item type
        """
        if item_type is None:
            item_type = "default"
        try:
            with open(os.path.join(
                    self.path,
                    pipeline_stage,
                    item_type,
                    self.metadata_file),
                      'r') as f:
                contents = json.load(f)
                return contents
        except FileNotFoundError:
            return {}

    def _write_artifact_meta(self, artifact):
        with self._write_lock:
            self._u_write_artifact_meta(artifact)

    def _u_write_artifact_meta(self, artifact):
        """
        Writes this artifact's metadata to a shared metadata file
        """
        distutils.dir_util.mkpath(os.path.join(
            self.path,
            self._relative_artifact_dir(artifact)))

        item_meta = self._load_item_meta(artifact._pipeline_stage,
                                         artifact.item.type)

        item_meta[artifact.get_uid()] = artifact.meta_to_dict()
        meta_key = artifact._pipeline_stage + str(artifact.item.type)
        self.cached_meta[meta_key] = item_meta

        with open(os.path.join(
                self.path,
                self._relative_artifact_dir(artifact),
                self.metadata_file),
                  'w') as f:
            json.dump(item_meta, f)

    def _find_cached_artifact(self, artifact):
        """
        Loads the metadata, but not the payload of an artifact.

        Tries to find a cached artifact matching the provided hashes/properties
        of the given artifact object.

        If only stage & item name are supplied, will return the newest artifact
        given the pruning ordering.
        """
        if artifact._specific_hash is not None or \
           artifact._dependency_hash is not None:
            item_meta = self._load_item_meta(artifact._pipeline_stage,
                                             artifact.item.type)
            if artifact.get_uid() in item_meta:
                artifact.meta_from_dict(item_meta[artifact.get_uid()])
                return artifact

            return None
        else:
            # Sort artifacts and return most recent
            sorted_artifacts = self._sorted_artifacts(artifact)
            if len(sorted_artifacts) == 0:
                return None
            return sorted_artifacts[0]
        raise NotImplementedError

    def _get_cached_artifact_payload(self, artifact):
        """
        Returns the payload for a given artifact, assuming that it
        has already been produced and is cached.
        """
        with open(os.path.join(self.path,
                               self._relative_artifact_path(artifact)),
                  'r') as f:
            return f.read()

    def _get_cached_artifact_metadata(self, artifact):
        """
        Returns the metadata for a given artifact, assuming that it
        has already been produced and is cached.
        """
        raise NotImplementedError

    def log_pipeline_stage_run_complete(self, stage_config, dependency_hash):
        with self._write_lock:
            self._u_log_pipeline_stage_run_complete(stage_config,
                                                      dependency_hash)

    def _u_log_pipeline_stage_run_complete(self, stage_config,
                                           dependency_hash):
        """
        Record that the pipeline stage run for the given dependency hash and
        definition hash completed successfully.
        """
        meta = self._get_pipeline_stage_run_meta(stage_config,
                                                 dependency_hash)

        meta['complete'] = True
        distutils.dir_util.mkpath(os.path.join(
            self.path,
            stage_config.name))
        with open(os.path.join(
                self.path,
                stage_config.name,
                self._pipeline_stage_run_filename(
                        dependency_hash,
                        stage_config.hash())),
                  'w') as f:
            json.dump(meta, f)

    def _pipeline_stage_run_filename(self, dependency_hash,
                                     definition_hash):
        return ("pipeline_stage_run_%s_%s" %
                (dependency_hash, definition_hash))

    def _record_pipeline_stage_run_artifact(self, artifact):
        with self._write_lock:
            self._u_record_pipeline_stage_run_artifact(artifact)

    def _u_record_pipeline_stage_run_artifact(self, artifact):
        """
        Record that the given artifact was produced during its corresponding
        pipeline stage run.
        """
        meta = self._get_pipeline_stage_run_meta(
            artifact._config,
            artifact._dependency_hash)

        if 'artifacts' not in meta:
            meta['artifacts'] = {}

        if 'dependency_hash' not in meta:
            meta['dependency_hash'] = artifact._dependency_hash

        uid = artifact.get_uid()
        if uid not in meta['artifacts']:
            meta['artifacts'][uid] = \
                {"item_type": artifact.item.type,
                 "specific_hash": artifact._specific_hash,
                 "uid": uid
                }
        else:
            print("Artifact %s already generated for run %s" %
                  (artifact.get_uid(), artifact._pipeline_stage))

        distutils.dir_util.mkpath(os.path.join(
            self.path,
            artifact._pipeline_stage))
        with open(os.path.join(
                self.path,
                artifact._pipeline_stage,
                self._pipeline_stage_run_filename(
                    artifact._dependency_hash,
                    artifact._definition_hash)),
                  'w') as f:
            json.dump(meta, f)

    def pipeline_stage_run_status(self, stage_config,
                                  dependency_hash):
        meta = self._get_pipeline_stage_run_meta(
            stage_config,
            dependency_hash)

        if meta == {}:
            return STAGE_DOES_NOT_EXIST
        elif 'complete' in meta:
            return STAGE_COMPLETE
        else:
            return STAGE_IN_PROGRESS

    def find_pipeline_stage_run_artifacts(self, stage_config,
                                          dependency_hash):
        """
        Finds all artifacts for a given pipeline run.
        """
        meta = self._get_pipeline_stage_run_meta(
            stage_config,
            dependency_hash)

        if 'artifacts' not in meta:
            return []
        else:
            res = []
            for uid in meta['artifacts']:
                artDict = meta['artifacts'][uid]
                art = Artifact(stage_config)
                art.item.type = artDict['item_type']
                art._specific_hash = artDict['specific_hash']
                res.append(self._find_cached_artifact(art))
            return res

    def _get_pipeline_stage_run_meta(self, stage_config,
                                     dependency_hash):
        """
        Load metadata for a given run of a pipeline stage
        """
        try:
            with open(os.path.join(
                    self.path,
                    stage_config.name,
                    self._pipeline_stage_run_filename(
                        dependency_hash,
                        stage_config.hash())),
                    'r') as f:
                contents = json.load(f)
                return contents
        except FileNotFoundError:
            return {}

    def _sorted_artifacts(self, artifact):
        """
        Returns a sorted list of artifacts, based upon pruning ordering
        """
        item_meta = self._load_item_meta(artifact._pipeline_stage,
                                         artifact.item.type)

        result = []
        for k in item_meta:
            result.append(item_meta[k])
        sorted_metadata = sorted(result, key=lambda x: x["creation_time"])

        sorted_artifacts = []
        for x in sorted_metadata:
            a = Artifact(artifact._config, artifact.item.type)
            a.meta_from_dict(x)
            sorted_artifacts.append(a)

        return sorted_artifacts
