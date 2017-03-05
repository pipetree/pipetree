import pkg_resources
import datetime
import shutil
import os.path
import json
import time
import os

from redleader.managers import CodeDeployManager
from redleader.cluster import Cluster, AWSContext
import redleader.resources as r

import pipetree
import pipetree.settings as settings
import pipetree.utils as utils
from pipetree.arbiter import RemoteSQSArbiter

import botocore.exceptions

class PipetreeCluster(object):
    DEFAULTS = {
        "s3_artifact_bucket_name": settings.S3_ARTIFACT_BUCKET_NAME,
        "aws_region": settings.AWS_REGION,
        "aws_profile": settings.AWS_PROFILE,
        "dynamodb_artifact_table_name": settings.DYNAMODB_ARTIFACT_TABLE_NAME,
        "dynamodb_stage_run_table_name": settings.DYNAMODB_STAGE_RUN_TABLE_NAME,
        "sqs_task_queue_name": settings.SQS_TASK_QUEUE_NAME,
        "sqs_result_queue_name": settings.SQS_RESULT_QUEUE_NAME,
        "servers": [{"type": "t2.micro", "storage": "20"}],
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

    def load_config(self, config_dict):
        # Need to set the cluster name manually before we
        # programmatically generate defaults that utilize it
        self._config_dict = config_dict
        cfg = self._gen_internal_config(config_dict)
        for k in cfg:
            setattr(self, "_%s" % k, cfg[k])

    def _gen_internal_config(self, config_dict):
        config = {}
        # Need to set the cluster name manually before we
        # programmatically generate defaults that utilize it
        for k in self.DEFAULTS:
            if k in config_dict:
                config[k] = config_dict[k]
            elif "_name" in k:
                # Generate default names based on cluster name for uniqueness
                s = "%s%s" % (self._pretty_sanitize_name(config_dict['cluster_name']),
                              self._pretty_sanitize_name(k.split("_name")[0]))
                config[k] = s
            else:
                config[k] = self.DEFAULTS[k]
        return config

    def validate(self, args):
        return True

    def _application_name(self):
        return "%sApplication" % self._pretty_sanitize_name(self._cluster_name)

    def _deployment_group_name(self):
        return "%sDeploymentGroup" % self._pretty_sanitize_name(self._cluster_name)

    def _underscore_to_camelcase(self, name):
        words = name.split("_")
        res = ""
        for word in words:
            res += word[0].upper() + word[1:].lower()
        return res

    def _sanitize_name(self, name):
        # TODO
        return name.replace("_", "").replace("-", "").replace(" ", "")

    def _pretty_sanitize_name(self, name):
        return self._sanitize_name(self._underscore_to_camelcase(name))

    def _rl_cluster_name(self):
        """ Remove offending characters from cloudformation stack name """
        return self._sanitize_name(self._cluster_name)

    def generate_redleader_cluster(self):
        context = self._context
        cluster = Cluster(self._rl_cluster_name(), context)
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

        self._rl_servers = []
        for server in self._servers:
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
            self._rl_servers.append(ec2Instance)

        return cluster

    def create_cluster(self):
        rl_cluster = self.generate_redleader_cluster()
        #print(json.dumps(rl_cluster.cloud_formation_template(), indent=4))
        self.ensure_log_group_exists("pipetreeClusterLogs")
        try:
            rl_cluster.blocking_deploy(verbose=True)
            # Resources often aren't available immediately after
            # cloud formation stack successfully gets created
            time.sleep(10)

        except botocore.exceptions.ClientError as e:
            if "AlreadyExistsException" in "%s" % e:
                print("Cluster already exists.")
            else:
                raise e

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

        # Generate an appropriate server_config.json
        generated_config = self._gen_internal_config(self._config_dict)
        with open(os.path.join(source_path, "server_config.json"), 'w') as f:
            json.dump(generated_config, f)
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
                                         bucket_name=self._s3_artifact_bucket_name.lower())

    def ensure_log_group_exists(self, log_group_name):
        client = self._context.get_client('logs')
        try:
            response = client.create_log_group(
                logGroupName=log_group_name
            )
        except botocore.exceptions.ClientError as e:
            if "AlreadyExistsException" in "%s" % e:
                print("Log group already exists.")
            else:
                raise e

    def _format_log(self, d):
        date = datetime.datetime.fromtimestamp(
                int(d['ingestionTime']) / 1000
        ).strftime('%Y-%m-%d %H:%M:%S')
        return "%s: %s" % (date, d['message'])

    def tail_logs_for_resource(self, resource, client, num_log_entries=100, next_token=None):
        response = {}
        try:
            response = client.get_log_events(
                logGroupName='pipetreeContainerLogs',
                limit=num_log_entries,
                logStreamName=resource)
        except botocore.exceptions.ClientError as e:
            if "ResourceNotFound" in ("%s" % e):
                print("Couldn't find logs for resource %s."
                      " The logstream may not yet be created." % resource)
            else:
                raise e

        if 'events' not in response:
            return []
        events = response['events']
        events.sort(key=lambda x: x['ingestionTime'])
        return map(self._format_log, events)

    def tail_logs(self, num_log_entries=100):
        cluster = self.generate_redleader_cluster()

        resource_ids = []
        for resource in self._rl_servers:
            resource_ids.append(cluster._mod_identifier(resource.get_id()))

        print("Fetching logs for the following servers: %s" % ", ".join(resource_ids))

        client = self._context.get_client('logs')
        logs = {}
        for resource in resource_ids:
            logs[resource] = []
            for msg in self.tail_logs_for_resource(resource, client, num_log_entries):
                logs[resource].append(msg)
        return logs

    def run_arbiter(self, filepath, monitor=None, pipeline_run_id=None):
        arbiter = RemoteSQSArbiter(
            filepath,
            s3_bucket_name=self._s3_artifact_bucket_name,
            aws_region = self._aws_region,
            aws_profile=self._aws_profile,
            artifact_table_name= self._dynamodb_artifact_table_name,
            stage_run_table_name=self._dynamodb_stage_run_table_name,
            task_queue_name=self._sqs_task_queue_name,
            result_queue_name=self._sqs_result_queue_name,
            pipeline_run_id=pipeline_run_id,
            monitor=monitor
        )
        arbiter.run_event_loop()
