from pipetree.stage import LocalFilePipelineStage,\
    LocalDirectoryPipelineStage, ExecutorPipelineStage,\
    ParameterPipelineStage, IdentityPipelineStage, \
    GridSearchPipelineStage

__version__ = '0.1.0'

STAGES = {
    "LocalDirectoryPipelineStage": LocalDirectoryPipelineStage,
    "LocalFilePipelineStage": LocalFilePipelineStage,
    "ExecutorPipelineStage": ExecutorPipelineStage,
    "ParameterPipelineStage": ParameterPipelineStage,
    "GridSearchPipelineStage": GridSearchPipelineStage,
    "IdentityPipelineStage": IdentityPipelineStage
}

from pipetree.pipeline import Pipeline
from pipetree.artifact import Item
from pipetree.providers import openStream, readStream
