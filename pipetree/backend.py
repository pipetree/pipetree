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
from collections import OrderedDict
from decimal import *
import distutils.dir_util
import json
import threading
import boto3
import botocore
import time
import logging

from pipetree import settings
from pipetree.utils import attach_config_to_object
from pipetree.exceptions import ArtifactMissingPayloadError, ArtifactNotFoundError
from pipetree.providers import FileStringStream, FileByteStream, S3ObjectStream
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
        cached_artifact = self._find_cached_artifact(artifact._config,
                                                     artifact.item.type,
                                                     artifact.get_uid())
        if cached_artifact is None:
            return None
        else:
            cached_artifact.load_payload(self._get_cached_artifact_payload(
                cached_artifact))
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

    def pipeline_run_status(self, uid):
        """
        Returns the status of an entire pipeline run, including the various stages within it.
        """
        raise NotImplementedError

    #def log_pipeline_run(self,

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
        "path": "./.pipetree/local_cache/",
        "metadata_file": "pipeline.meta"
    }

    def _log(self, s):
        print("LocalArtifactBackend: %s" % s)

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

        self._log("Saving generated artifact for stage %s with dependency hash %s" %
                  (artifact._pipeline_stage, artifact._dependency_hash))
        with self._write_lock:
            mode = 'w'
            if artifact._serialization_type == "bytestream":
                mode = 'wb'
            with open(os.path.join(self.path,
                                   self._relative_artifact_path(artifact)),
                      mode) as f:
                if artifact._serialization_type in ["bytestream", "stringstream"]:
                    # TODO, write in chunks
                    artifact.item.payload.open()
                    x = artifact.item.payload.read()
                    f.write(x)
                    artifact.item.payload.close()
                else:
                    f.write(artifact.serialize_payload())

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

    def _find_cached_artifact(self, stage_config, item_type, uid):
        """
        Loads the metadata, but not the payload of an artifact.

        Tries to find a cached artifact matching the provided hashes/properties
        of the given artifact object.

        If only stage & item name are supplied, will return the newest artifact
        given the pruning ordering.
        """
        item_meta = self._load_item_meta(stage_config.name, item_type)
        if uid in item_meta:
            artifact = Artifact(stage_config)
            artifact.meta_from_dict(item_meta[uid])
            artifact._loaded_from_local_cache = True
            return artifact
        return None

    def _get_cached_artifact_payload(self, artifact):
        """
        Returns the payload for a given artifact, assuming that it
        has already been produced and is cached.
        """
        if artifact._serialization_type == "bytestream":
            return FileByteStream(
                os.path.join(self.path, self._relative_artifact_path(artifact)))
        elif artifact._serialization_type == "stringstream":
            return FileStringStream(
                os.path.join(self.path, self._relative_artifact_path(artifact)))
        else:
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
            self._log("Artifact %s already generated for run %s" %
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
                cached = self._find_cached_artifact(stage_config, artDict['item_type'], uid)
                if cached is None:
                    raise ArtifactNotFoundError(artifact="%s %s" %
                                                (uid, artDict['specific_hash']))
                res.append(cached)
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


class S3ArtifactBackend(ArtifactBackend):
    """
    Provide an S3 + DynamoDB storage backend for generated artifacts
    and their metadata.
    """
    DEFAULTS = {
        "path": "./.pipetree/local_cache/",
        "enable_local_caching": True,
        "metadata_file": "pipeline.meta",
        "aws_region": settings.AWS_REGION,
        "aws_profile": settings.AWS_PROFILE,
        "s3_bucket_name": settings.S3_ARTIFACT_BUCKET_NAME,
        "dynamodb_artifact_table_name": settings.DYNAMODB_ARTIFACT_TABLE_NAME,
        "dynamodb_stage_run_table_name": settings.DYNAMODB_STAGE_RUN_TABLE_NAME
    }

    def _log(self, s):
        print("S3ArtifactBackend: %s" % s)

    def __init__(self, path=DEFAULTS['path'], **kwargs):
        super().__init__(path=path, **kwargs)
        self._localArtifactBackend = LocalArtifactBackend(path=path, **kwargs)

        try:
            self._session = boto3.Session(profile_name=self.aws_profile,
                                    region_name=self.aws_region)
        except botocore.exceptions.NoCredentialsError:
            self._session = boto3.Session(region_name=self.aws_region)
        except botocore.exceptions.ProfileNotFound:
            self._session = boto3.Session(region_name=self.aws_region)

        self._stage_run_table = None
        self._artifact_meta_table = None
        self.s3_bucket_name = self.s3_bucket_name.lower().replace("-", "").replace("_", "")
        self._setup_s3()
        self._setup_dynamo_db()


    def _setup_s3(self):
        self._s3_client = self._session.client('s3')
        try:

            self._s3_client.create_bucket(Bucket=self.s3_bucket_name,
                                          CreateBucketConfiguration={
                                              'LocationConstraint': self.aws_region})
            self._log("Created bucket %s" % self.s3_bucket_name)
        except botocore.exceptions.ClientError as err:
            self._log("Bucket already exists %s" % self.s3_bucket_name)
            return
            if "TooManyBuckets" in "%s" % err:
                print(err)
                pass


    def _setup_dynamo_db(self):
        """
        Setup local dynamodb resource

        Setup local dynamodb tables as well. We define non-indexed columns
        despite not needing them in table creation, for our own convenience.
        """
        self._dynamodb = self._session.resource('dynamodb')

        stage_run_keys = OrderedDict([
            ('stage_config_hash', 'HASH'),
            ('dependency_hash', 'RANGE')
        ])
        stage_run_fields = {
            'stage_config_hash': 'S',
            'dependency_hash': 'S',
            'stage_run_status': 'S',
            'metadata': 'S',
            'start_time': 'N',
            'end_time': 'N'
        }
        self._stage_run_table = self._dynamo_db_create_table(
            self.dynamodb_stage_run_table_name,
            stage_run_keys,
            stage_run_fields)

        artifact_meta_keys = {
            'artifact_uid': 'HASH',
        }
        artifact_meta_fields = {
            'artifact_uid': 'S',
            'artifact_meta': 'S',
            'creation_time': 'N'
        }
        self._artifact_meta_table = self._dynamo_db_create_table(
            self.dynamodb_artifact_table_name,
            artifact_meta_keys,
            artifact_meta_fields)

    def _dynamo_db_create_table(self, table_name, keys, fields,
                                write_units=1, read_units=1):
        keySchema = []
        attributeDefinitions = []
        for key in keys:
            keySchema.append({'AttributeName': key,
                              'KeyType': keys[key]})
        for key in fields:
            if key in keys:
                attributeDefinitions.append({'AttributeName': key,
                                             'AttributeType': fields[key]})
        try:
            table = self._dynamodb.create_table(
                TableName=table_name,
                KeySchema=keySchema,
                AttributeDefinitions=attributeDefinitions,
                ProvisionedThroughput={
                    'ReadCapacityUnits': read_units,
                    'WriteCapacityUnits': write_units
                }
            )
            table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
            return table
        except botocore.exceptions.ClientError:
            # If the table already exists, load and return that table.
            return self._dynamodb.Table(table_name)

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
        Saves an artifact locally on disk and in an S3 Bucket

        Artifact must have all necessary metadata, including:
         - specific_hash
         - dependency_hash
         - definition_hash
        """
        if artifact.item is None or artifact.item.payload is None:
            raise ArtifactMissingPayloadError(stage=artifact._pipeline_stage)

        # Cache the output locally and use local file for S3 upload
        self._localArtifactBackend.save_artifact(artifact)

        # Upload to S3
        key = self.s3_artifact_key(artifact)
        local_file = os.path.join(self.path, key)
        self._s3_client.upload_file(local_file,
                                    self.s3_bucket_name,
                                    key)

        self._write_artifact_meta(artifact)

    def _write_artifact_meta(self, artifact):
        """
        Writes this artifact's metadata to local storage & dynamodb
        """
        if self.enable_local_caching:
            self._localArtifactBackend._write_artifact_meta(artifact)

        # Insert artifact meta into DynamoDB
        self._artifact_meta_table.put_item(
            Item={
                'artifact_uid': artifact.get_uid(),
                'artifact_meta': json.dumps(artifact.meta_to_dict()),
                'creation_time': Decimal(time.time())
            }
        )

        # Update pipeline stage meta
        stage_run_key = {
            'stage_config_hash': str(artifact._definition_hash),
            'dependency_hash': str(artifact._dependency_hash)
        }
        response = self._stage_run_table.get_item(Key=stage_run_key)

        if 'Item' not in response:
            # Create stage run meta
            self._stage_run_table.put_item(
                Item={
                    'stage_config_hash': artifact._definition_hash,
                    'dependency_hash': str(artifact._dependency_hash),
                    'stage_run_status': STAGE_IN_PROGRESS,
                    'metadata': json.dumps({'artifacts': [
                        {'uid': artifact.get_uid(),
                         'specific_hash': artifact._specific_hash,
                         'type': artifact.item.type}
                    ]}, sort_keys=True)
                }
            )
        else:
            # Update stage meta
            meta = json.loads(response['Item']['metadata'])
            meta['artifacts'].append({
                "uid": artifact.get_uid(),
                "type": artifact.item.type,
                "specific_hash": artifact._specific_hash
            })
            self._stage_run_table.update_item(
                Key=stage_run_key,
                UpdateExpression='SET metadata = :metaVal',#, stage_run_status= :status',
                # The condition expression ensures metadata hasn't changed,
                # effectively performing an atomic CAS
                ConditionExpression='metadata = :oldMetaVal',
                ExpressionAttributeValues={
                    ':oldMetaVal': response['Item']['metadata'],
                    ':metaVal': json.dumps(meta, sort_keys=True),
                    #':status': STAGE_IN_PROGRESS
                }
            )
            item = response['Item']

    def _find_cached_artifact(self, stage_config, item_type, artifact_uid):
        """
        Loads the metadata, but not the payload of an artifact.
        """

        # Attempt to load artifact locally
        if self.enable_local_caching:
            res = self._localArtifactBackend._find_cached_artifact(
                stage_config, item_type, artifact_uid)
            if res is not None:
                return res

        # Otherwise, query DynamoDB
        art_key = {
            'artifact_uid': artifact_uid
        }
        print("Searching for cached artifact: ", stage_config.name, item_type, artifact_uid)
        response = self._artifact_meta_table.get_item(Key=art_key)

        for r in self._artifact_meta_table.scan()['Items']:
            print("SCAN", r)
        print("Response: ", response)
        if 'Item' not in response:
            return None
        else:
            artifact = Artifact(stage_config)
            artifact.meta_from_dict(json.loads(response['Item']['artifact_meta']))
            artifact._loaded_from_s3_cache = True
            return artifact
        return None

    def s3_artifact_key(self, artifact):
        return self._relative_artifact_path(artifact)

    def _get_cached_artifact_payload(self, artifact):
        """
        Returns the payload for a given artifact, assuming that it
        has already been produced and is cached on S3.
        """
        obj = self._s3_client.get_object(
            Bucket= self.s3_bucket_name,
            Key= self.s3_artifact_key(artifact)
        )
        if artifact._serialization_type == "bytestream":
            return S3ObjectStream(obj, bytestream=True)
        if artifact._serialization_type == "stringstream":
            return S3ObjectStream(obj)
        else:
            return obj['Body'].read()

    def log_pipeline_stage_run_complete(self, stage_config,
                                           dependency_hash):
        """
        Record that the pipeline stage run for the given dependency hash and
        definition hash completed successfully.
        """
        # Log locally
        if self.enable_local_caching:
            self._localArtifactBackend.log_pipeline_stage_run_complete(
            stage_config, dependency_hash)

        # Update dynamo db
        stage_run_key = {
            'stage_config_hash': str(stage_config.hash()),
            'dependency_hash': str(dependency_hash)
        }
        self._stage_run_table.update_item(
            Key=stage_run_key,
            UpdateExpression='SET stage_run_status = :status',
            # The condition expression ensures metadata hasn't changed,
            # effectively performing an atomic CAS
            ExpressionAttributeValues={
                ':status': STAGE_COMPLETE
            }
        )

    def pipeline_stage_run_status(self, stage_config,
                                  dependency_hash):
        stage_run_key = {
            'stage_config_hash': str(stage_config.hash()),
            'dependency_hash': str(dependency_hash)
        }
        response = self._stage_run_table.get_item(Key=stage_run_key)

        if 'Item' not in response:
            return STAGE_DOES_NOT_EXIST
        else:
            return response['Item']['stage_run_status']

    def find_pipeline_stage_run_artifacts(self, stage_config,
                                          dependency_hash):
        """
        Finds all artifacts for a given pipeline run, loading their
        metadata.
        """
        # Update pipeline stage metaa
        stage_run_key = {
            'stage_config_hash': stage_config.hash(),
            'dependency_hash': dependency_hash,
        }

        response = self._stage_run_table.get_item(Key=stage_run_key)
        if 'Item' not in response:
            return None
        else:
            res = []
            meta = json.loads(response['Item']['metadata'])
            for obj in meta['artifacts']:
                art = Artifact(stage_config)
                art.item.type = obj['type']
                art._specific_hash = obj['specific_hash']
                art._dependency_hash = dependency_hash
                art._definition_hash = stage_config.hash()
                res.append(self._find_cached_artifact(art._config,
                                                      art.item.type,
                                                      art.get_uid()))
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
        raise NotImplementedError
