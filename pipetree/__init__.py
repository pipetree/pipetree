from pipetree.stage import LocalFilePipelineStage,\
    LocalDirectoryPipelineStage, ExecutorPipelineStage,\
    ParameterPipelineStage, IdentityPipelineStage

__version__ = '0.1.0'

STAGES = {
    "LocalDirectoryPipelineStage": LocalDirectoryPipelineStage,
    "LocalFilePipelineStage": LocalFilePipelineStage,
    "ExecutorPipelineStage": ExecutorPipelineStage,
    "ParameterPipelineStage": ParameterPipelineStage,
    "IdentityPipelineStage": IdentityPipelineStage
}

from pipetree.pipeline import Pipeline
from pipetree.artifact import Item
