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


AWS_REGION = "us-west-1"
AWS_PROFILE = "pipetree"

S3_ARTIFACT_BUCKET_NAME = "pipetree-artifacts"

DYNAMODB_ARTIFACT_TABLE_NAME = "pipetree-artifact-meta-table"
DYNAMODB_STAGE_RUN_TABLE_NAME = "pipetree-stage-run-table"

SQS_TASK_QUEUE_NAME = 'pipetree-executor-task-queue'
SQS_RESULT_QUEUE_NAME = 'pipetree-executor-result-queue'

class PipetreeSettings(object):
    aws_region=AWS_REGION,
    aws_profile=AWS_PROFILE,
    s3_artifact_bucket_name=S3_ARTIFACT_BUCKET_NAME,
    dynamodb_artifact_table_name=DYNAMODB_ARTIFACT_TABLE_NAME,
    dynamodb_stage_run_table_name=DYNAMODB_STAGE_RUN_TABLE_NAME,
    sqs_task_queue_name=SQS_TASK_QUEUE_NAME,
    sqs_result_queue_name=SQS_RESULT_QUEUE_NAME
