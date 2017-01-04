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


class PipetreeError(Exception):
    """
    Base exception for all pipetree exceptions
    """

    message = 'An unknown error has occured'

    def __init__(self, **kwargs):
        msg = self.message.format(**kwargs)
        Exception.__init__(self, msg)
        self.kwargs = kwargs


class InvalidPipelineConfigError(PipetreeError):
    """
    Raised when a pipeline config file is parsed but fails
    to pass validation step.
    """
    message = 'Unable to validate config file \'{config_path}\'. {reason}'


class MissingPipelineAttributeError(PipetreeError):
    """
    Raised when a PipelineStageConfig is created with a valid name,
    but does not have the given attribute
    """
    message = 'No {attribute} attribute found for pipeline stage {stage_name}'


class IncorrectPipelineStageNameError(PipetreeError):
    message = 'Pipeline stage type must be one of {types}'


class NonPythonicNameError(PipetreeError):
    message = '\'{symbol}\' is not a valid python name'


class ArtifactSourceDoesNotExistError(PipetreeError):
    message = '{provider} cannot source artifacts from {source}'


class ArtifactProviderMissingParameterError(PipetreeError):
    message = '{provider} instantiated without parameter {parameter}'


class InvalidArtifactMetadataError(PipetreeError):
    message = 'Artifact Metadata Invalid for pipeline stage {stage},'\
              + 'property {property}'


class ArtifactProviderFailedError(PipetreeError):
    message = 'Artifact Provider {provider} failed to produce an artifact:'\
              + ' {error}'


class ArtifactMissingPayloadError(PipetreeError):
    message = 'Artifact from stage {stage} is missing its payload'


class StageDoesNotExistError(PipetreeError):
    message = 'Specified pipeline stage {stage} does not exist'


class InvalidConfigurationFileError(PipetreeError):
    message = 'Error in config for \'{configurable}\' {reason}'


class DuplicateStageNameError(PipetreeError):
    message = 'Pipeline Stage {name} already exists'
