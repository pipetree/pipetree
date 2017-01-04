from pipetree.stage import LocalFilePipelineStage,\
    LocalDirectoryPipelineStage, ExecutorPipelineStage,\
    ParameterPipelineStage

__version__ = '0.1.0'

STAGES = {
    "LocalDirectoryPipelineStage": LocalDirectoryPipelineStage,
    "LocalFilePipelineStage": LocalFilePipelineStage,
    "ExecutorPipelineStage": ExecutorPipelineStage,
    "ParameterPipelineStage": ParameterPipelineStage
}

from pipetree.pipeline import Pipeline
