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
core/pipetree.py
=========

Provides core pipetree functionality, independent of storage backend
"""


def choose_artifact(pipeline_stage, item_name, meta)
    """
    Choose an artifact for a given pipeline item, given the state of the current local cache. 

    Currently defaults to returning the most recently produced artifact for an item.

    Eventually a pipeline stage will be able to specify its own default artifact selection method
    for each pipeline item:
    { ..., "artifact_selection": { "trained_model_item": "min__loss" }, ... }
    """
    
    if "artifact_selection" in pipeline_stage:
        raise Exception("Custom artifact selection methods not yet implemented")
    
    # For now, just return the most recently created artifact
    hwm = 0
    chosen_artifact = None
    for artifact in meta["artifacts"]:
        if artifact["time"] > hwm:
            hwm = artifact["time"]
            chosen_artifact = artifact
            
    return chosen_artifact
