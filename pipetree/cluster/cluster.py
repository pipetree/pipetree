import pkg_resources
import shutil
import os.path
import os

from redleader.managers import CodeDeployManager
from redleader.cluster import Cluster, AWSContext
import redleader.resources as r

import pipetree
import pipetree.settings as settings
import pipetree.utils as utils

class PipetreeCluster(object):
    DEFAULTS = {
        "s3_artifact_bucket_name": settings.S3_ARTIFACT_BUCKET_NAME,
        "aws_region": settings.AWS_REGION,
        "aws_profile": settings.AWS_PROFILE,
        "dynamodb_artifact_table_name": settings.DYNAMODB_ARTIFACT_TABLE_NAME,
        "dynamodb_stage_run_table_name": settings.DYNAMODB_STAGE_RUN_TABLE_NAME,
        "sqs_task_queue_name": settings.SQS_TASK_QUEUE_NAME,
        "sqs_result_queue_name": settings.SQS_RESULT_QUEUE_NAME,
        "cluster_spec": [{"type": "t2.micro", "storage": "20"}],
        "cluster_name": "pipetreeCluster"
    }
    def __init__(self, **kwargs):
        self.validate(kwargs)
        for arg in kwargs:
            setattr(self, "_%s" % arg, kwargs[arg])
        self.load_defaults()
        self._context = AWSContext(aws_profile=self._aws_profile)

    def load_defaults(self):
        for k in self.DEFAULTS:
            if(not hasattr(self, "_%s" % k)):
                setattr(self, "_%s" % k, self.DEFAULTS[k])

    def validate(self, args):
        return True

    def _application_name(self):
        return "%sApplication" % self._cluster_name

    def _deployment_group_name(self):
        return "%sDeploymentGroup" % self._cluster_name

    def generate_redleader_cluster(self):
        context = self._context
        cluster = Cluster(self._cluster_name, context)
        s3_bucket = r.S3BucketResource(context, self._s3_artifact_bucket_name)
        task_queue = r.SQSQueueResource(context, self._sqs_task_queue_name)
        result_queue = r.SQSQueueResource(context, self._sqs_result_queue_name)
        application_name = self._application_name()
        deployment_group_name = self._deployment_group_name()
        deployment_group = r.CodeDeployDeploymentGroupResource(context,
                                                              application_name,
                                                              deployment_group_name)
        logs = r.CloudWatchLogs(context)

        cluster.add_resource(task_queue)
        cluster.add_resource(result_queue)
        cluster.add_resource(s3_bucket)
        cluster.add_resource(deployment_group)
        cluster.add_resource(logs)

        for server in self._cluster_spec:
            ec2Instance = r.CodeDeployEC2InstanceResource(
                context,
                deployment_group,
                permissions=[r.ReadWritePermission(s3_bucket),
                             r.ReadWritePermission(task_queue),
                             r.ReadWritePermission(result_queue),
                             r.ReadWritePermission(logs)
                ],
                storage=server["storage"],
                instance_type=server["type"]
            )
            cluster.add_resource(ec2Instance)

        return cluster

    def deploy_cluster(self):
        rl_cluster = self.generate_redleader_cluster()
        rl_cluster.blocking_deploy(verbose=True)

    def delete_cluster(self):
        rl_cluster = self.generate_redleader_cluster()
        rl_cluster.blocking_delete(verbose=True)

    def generate_codedeploy_package(self, source_path):
        """
        Generate a code deploy package from the docker application
        living at `source_path`.
        """

        # Copy our template codedeploy app into source_path/pipetree_codedeploy_app
        code_deploy_path = pkg_resources.resource_filename(__name__, "codedeploy_app")
        working_path = os.path.join(source_path, "pipetree_codedeploy_app")
        try:
            shutil.rmtree(working_path)
        except FileNotFoundError:
            pass
        shutil.copytree(code_deploy_path, working_path)

        # Copy all files from source_path into pipetree_codedeploy_app/src/
        for x in os.listdir(source_path):
            if x != "pipetree_codedeploy_app":
                srcf = os.path.join(source_path, x)
                if(os.path.isdir(srcf)):
                    shutil.copytree(srcf, os.path.join(working_path, "src", x))
                else:
                    shutil.copy(srcf, os.path.join(working_path, "src", x))
        print(os.listdir(working_path))
        return working_path

    def deploy_application(self, source_path):
        """
        Generate and deploy our code deploy application
        """
        codedeploy_app_path = self.generate_codedeploy_package(source_path)
        rl_cluster = self.generate_redleader_cluster()
        client = self._context.get_client('codedeploy')
        manager = CodeDeployManager(self._context)
        return manager.create_deployment(self._application_name(),
                                         self._deployment_group_name(),
                                         path=codedeploy_app_path,
                                         bucket_name=self._s3_artifact_bucket_name)
