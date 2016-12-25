"""
storage/storage.py
=======

Module providing generic storage functionality
"""

PLI_FILE_TYPE = "file"
PLI_FILE_FOLDER_TYPE = "file_folder"

def file(resource_name, local_filepath):
    """
    Create a pipeline item representing a single file. 

    The pipeline item will be named `resource_name` and will be sourced from 
    `local_filepath` if available. If `local_filepath` is not available, the resource
    will only be loaded from S3. 
    """

    return {
        "name": resource_name,
        "type": PLI_FILE_TYPE,
        "local_filepath": local_filepath
    }

def file_folder(resource_name, local_filepath):
    """
    Create a pipeline item representing a folder of files. 

    The pipeline item will be named `resource_name` and will be sourced from 
    `local_filepath` if available. If `local_filepath` is not available, the resource
    will only be loaded from S3. 
    """

    return {
        "name": resource_name,
        "type": PLI_FILE_FOLDER_TYPE,
        "local_filepath": local_filepath
    }
