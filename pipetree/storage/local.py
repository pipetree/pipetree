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

Provides storage mechanisms for local storage and retrieval of files.
"""
from pipelinestorage import *
import os.path
import distutils.dir_util
import shutil
import hashlib
import json
    
METAFILE = "pipeline.meta"
METADATA_FILES = [METAFILE]

def require_type(pipeline_item, ty):
    """
    Helper function to require that a pipeline item has a particular type
    """
    if "type" not in pipeline_item:
        raise Exception("Pipeline item has no type")

    if pipeline_item["type"] != ty:
        raise Exception("Invalid pipeline item type")

def prune_versions(pipeline_item, pipeline_options):
    """
    Prunes versions of pipeline_item depending on settings.
    Common configurations:
      { "cache_prune": "keep_most_recent", "cache_size": 10... }
      { "cache_prune": "keep_lowest_meta", "cache_meta_value": "trained_model_loss" }
    """
    require_type(pipeline_item, PLI_FILE_TYPE)
    raise Exception("Pruning not yet implemented")

def file_metadata(pipeline_item, pipeline_options):
    """
    Returns the file metadata for a given pipeline item, or None if none exists
    """
    require_type(pipeline_item, PLI_FILE_TYPE)

    # TODO: Currently throws an exception if file metadata DNE for testing purposes
    
    metaf = os.path.join(local_storage_path(pipeline_options), pipeline_item["name"], METAFILE)
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

def file_handle(pipeline_item, pipeline_options, version=None):
    """
    Returns a file handle (object supporting .read(), .close()) for a given
    file type pipeline_item.

    Defaults to returning the most recently created version.
    """

    require_type(pipeline_item, PLI_FILE_TYPE)
    fpath = pipeline_item["local_filepath"]

    storage_path = local_storage_path(pipeline_options)

    versions = file_versions(pipeline_item, pipeline_options)
    if len(list(versions)) == 0:
        # Create file and continue
        hsh = hash_file(fpath)
        new_fpath = os.path.join(local_storage_path(pipeline_options), pipeline_item["name"], hsh)
        meta_fpath = os.path.join(local_storage_path(pipeline_options), pipeline_item["name"], METAFILE)
        obj = {"hashes": [hsh]}
        with open(meta_fpath, 'w') as mfp:
            json.dump(obj, mfp)
        shutil.copyfile(fpath, new_fpath)
        return open(new_fpath, 'r')
    else:
        # Determine most recent file and open that one
        if version is None:
            meta = file_metadata(pipeline_item, pipeline_options)
            version = meta["hashes"][len(meta["hashes"]) - 1]
        return open(
            os.path.join(local_storage_path(pipeline_options), pipeline_item["name"], version), 'r')

def file_versions(pipeline_item, pipeline_options):
    """
    Returns a list of all locally stored versions of a file pipeline item
    """
    require_type(pipeline_item, PLI_FILE_TYPE)
    version_folder = os.path.join(local_storage_path(pipeline_options), pipeline_item["name"])
    if not os.path.exists(version_folder):
        distutils.dir_util.mkpath(version_folder)    
    listing = os.listdir(version_folder)
    return filter(lambda x: x not in METADATA_FILES, listing)

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
