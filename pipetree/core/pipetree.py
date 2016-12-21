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

Implementation of core pipeline semantics using pure functions
"""

def topo_sort_pipeline_stages(config_obj):
    """
    Return a sorted list of pipeline stages, given a config_obj

    Raises an exception if there was a cycle in the configuration.
    """
    raise Exception("Not yet implemented")

def need_to_run_pipeline_stage(config_obj, previous_stages_rerunning, stage_name):
    """
    Determine whether a given pipeline stage needs to be re-run. 
    
    We include a list of previous stages that we're presently re-running,
    so that if this stage depends on them we can determine if this stage needs to
    re-run once they're complete. 

    Returns an object:
    { "rerun": True/False } or
    { "determine_rerun_after": ["stage_name_0", "stage_name_1", ...] }
    """

    # If the pipeline stage has caching disabled, we need to run the stage.
    
    # Check if we're re-running any stages that we depend on. If so, bail out.

    # Step 0: Acquire artifact metadata for all input items
    #         If any items we require don't have any artifacts, we need to run the stage. 

    # Step 1: Generate pre-job-artifact-hashes for items produced by this pipeline job.
    #         If any of these artifact versions don't yet exist, we need to run the stage. 
    
    raise Exception("Not yet implemented")

def required_items(config_obj, stage_name):
    """
    Returns a list of items required to run the job for a particular stage
    Format: [{"stage_name": "my_stage_name", 
              "item_name": "my_item_name",
              "artifact_selection" (future): "min__loss" }]
    """
    raise Exception("Not yet implemented")
    

def generate_artifact_meta(pipeline_stage, item_name, specific_hash, antecedents):
    """
    Generate metadata (hashes, time, etc) for an artifact. These will be aggregated into
    the item metadata structure.
    
    This method will generate the definition_hash as well as the antecedent hash, but requires
    the calling method to provide the specific_hash for the artifact.

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
    raise Exception("Not yet implemented")

def choose_artifact(pipeline_stage, item_name, meta)
    """
    Choose an artifact for a given pipeline item, given a pipeline's metadata.

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
