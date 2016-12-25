__version__ = '0.1.0'

from pipetree.stage import LocalDirectoryPipelineStage

_STAGES = [
    LocalDirectoryPipelineStage
]
STAGES = {cls.__name__: cls for cls in _STAGES}

from pipetree.pipeline import Pipeline
