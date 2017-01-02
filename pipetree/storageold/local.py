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
"""
local.py
=========

Provides storage mechanisms for local storage and retrieval of artifacts.
"""
import os.path
import distutils.dir_util
import shutil
import hashlib
import json

from pipetree.storage import storage

METAFILE = "pipeline.meta"
METADATA_FILES = [METAFILE]

def require_type(pipeline_stage, ty):
    """
    Helper function to require that a pipeline stage has a particular type
    """
    if "type" not in pipeline_stage:
        raise Exception("Pipeline stage has no type")

    if pipeline_stage["type"] != ty:
        raise Exception("Invalid pipeline stage type ")

def prune_versions(pipeline_stage, item_name, pipeline_options, keep_number=10):
    """
    Prunes local versions of pipeline_stage depending on settings.
    """
    
    require_type(pipeline_stage, PLI_FILE_TYPE)
    raise Exception("Pruning not yet implemented")

def item_metadata(pipeline_stage, item_name, pipeline_options):
    """
    Returns the local metadata for a given item, or None if none exists.
    Contains metadata for all instantiated artifacts from the item

    Item metadata structure: 
    {
      "pipeline_stage": "my_pipeline_stage",
      "pipeline_item": "my_pipeline_item",
      "artifacts": {
           "0xAA096750xAB076420xAF11235": 
              { "definition_hash": 0xAB07642,
                "specific_hash": 0xAF11235,
                "tags": ["my_pipeline_run", ...]
    "antecedents": {"prev_pipeline_stage/prev_pipeline_item": 0xAB224560xAB...,
                                "prev_pipeline_stage/prev_pipeline_item2": 0xAB221020xBF...,
                            },
                "time": 1482279992.034,
                "metadata": { "loss": 0.4 }
           }, 
           ...
        }
      }
    }
    """

    # TODO: Currently throws an exception if file metadata DNE, for testing purposes
    metaf = os.path.join(local_storage_path(pipeline_options), pipeline_stage["name"], item_name, METAFILE)
    with open(metaf, 'r') as mfp:
        meta = json.load(mfp)
    return meta

def hash_file(filepath):
    """
    Calculate a hash for a given file using SHA1
    """
    BLOCKSIZE = 65536
    hasher = hashlib.sha1()
    with open(filepath, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCKSIZE)
    return hasher.hexdigest()

def choose_artifact(pipeline_stage, item_name):
    """
    Choose an artifact for a given pipeline item, given the state of the current local cache. 

    Currently defaults to returning the most recently produced artifact for an item.

    Eventually a pipeline stage will be able to specify its own default artifact selection method
    for each pipeline item:
    { ..., "artifact_selection": { "trained_model_item": "min__loss" }, ... }
    """
    storage_path = local_storage_path(pipeline_options)
    meta = item_metadata(pipeline_stage, item_name, pipeline_options)
    if meta is None or meta.get("artifacts", None) is None or len(meta["artifacts"]) is 0:
        return None

    return pipetree.choose_artifact(pipeline_stage, item_name, meta)

    
def file_handle(pipeline_stage, item_name, pipeline_options, combined_hash):
    """
    Returns a file handle (object supporting .read(), .close()) to an artifact.
    """
    return open(os.path.join(local_storage_path(pipeline_options), pipeline_stage, item_name, combined_hash), 'r')

def folder(pipeline_item, pipeline_options):
    """
    Returns an array of pipeline objects for a given file_folder pipeline_item
    """
    require_type(pipeline_item, PLI_FILE_FOLDER_TYPE)    

def local_storage_path(pipeline_options):
    """
    Returns the local storage path for pipetree_storage, given a pipeline_options object.

    Assumes storage directory is ./pipetree_storage/ if not set in `pipeline_options`
    Creates storage directory if not present
    """
    storage_directory = "./pipetree_storage/"
    if "storage_directory" in pipeline_options:
        storage_directory = pipeline_options["storage_directory"]

    if not os.path.exists(storage_directory):
        distutils.dir_util.mkpath(storage_directory)    
    return storage_directory
