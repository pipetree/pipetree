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
import os
import json
import time
import click
import shutil
import subprocess

from pipetree.cli.utils import _get_config_path, _assert_in_project_dir, _load_json_file
from pipetree import __version__ as pipetree_version
from pipetree.templates import DEFAULT_CONFIG, DEFAULT_HANDLERS,\
    DEFAULT_PIPELINE_CONFIG, DEFAULT_CLUSTER_CONFIG
from pipetree.pipeline import PipelineFactory
from pipetree.exceptions import PipetreeError
from pipetree.arbiter import LocalArbiter
from pipetree.cluster import PipetreeCluster

@click.group()
@click.version_option(version=pipetree_version, message='%(prog)s %(version)s')
@click.option('--project_dir', help='The project directory. '
              'Defaults to the current directory.')
@click.option('--debug/--no-debug', default=False,
              help='Write debug logs to standard error.')
@click.pass_context
def cli(ctx, project_dir, debug=False):
    if not ctx.obj:
        ctx.obj = {}
    if project_dir is None:
        project_dir = os.getcwd()
    ctx.obj['project_dir'] = project_dir
    ctx.obj['debug'] = debug

@cli.command()
@click.argument('project_name', required=True)
@click.pass_context
def init(ctx, project_name):
    if os.path.isdir(project_name):
        click.echo('Already a directory named: %s' % project_name)
        raise click.Abort()
    pipetree_dir = os.path.join(project_name, '.pipetree')
    config = os.path.join(pipetree_dir, 'config.json')
    pipeline_config = os.path.join(project_name, 'pipeline.json')
    cluster_config = os.path.join(project_name, 'cluster.json')
    os.makedirs(pipetree_dir)
    os.makedirs(os.path.join(project_name, "src"))
    with open(config, 'w') as f:
        f.write(DEFAULT_CONFIG % project_name)
    with open(pipeline_config, 'w') as f:
        f.write(DEFAULT_PIPELINE_CONFIG % project_name)
    with open(cluster_config, 'w') as f:
        f.write(DEFAULT_CLUSTER_CONFIG % project_name)

    setup_example(project_name)

    py_package = os.path.join(project_name, project_name)
    os.makedirs(py_package)
    with open(os.path.join(py_package, '__init__.py'), 'w'):
        pass
    with open(os.path.join(py_package, 'main.py'), 'w') as f:
        f.write(DEFAULT_HANDLERS)
    click.echo("Created new project %s" % project_name)

def setup_example(project_name):
    lib_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cat_src_dir = os.path.join(lib_dir, 'examples', 'cats')
    cat_dst_dir = os.path.join(project_name, 'cat_imgs')
    shutil.copytree(cat_src_dir, cat_dst_dir)

@cli.group()
@click.pass_context
def config(ctx):
    _assert_in_project_dir(ctx.obj['project_dir'])


@config.command('set')
@click.argument('setting_name', required=True)
@click.argument('new_value', required=True)
@click.pass_context
def config_set(ctx, setting_name, new_value):
    with open(_get_config_path(ctx), 'r') as f:
        cfg = json.loads(f.read())
        cfg[setting_name] = new_value
    with open(_get_config_path(ctx), 'w') as f:
        f.write(json.dumps(cfg, indent=4))


@config.command('get')
@click.argument('setting_name', required=True)
@click.pass_context
def config_get(ctx, setting_name):
    with open(_get_config_path(ctx), 'r') as f:
        cfg = json.loads(f.read())
        click.echo(cfg.get(setting_name,
                           'Setting \'%s\' not found.' % setting_name))


@config.command('edit')
@click.pass_context
def config_edit(ctx):
    subprocess.check_call([os.environ.get('EDITOR', 'vi'),
                           os.path.join(ctx.obj['project_dir'],
                                        '.pipetree',
                                        'config.json')])


@cli.command('verify-config')
@click.argument('config', required=True)
@click.pass_context
def verify_pipeline_config(ctx, config):
    factory = PipelineFactory()
    try:
        factory.generate_pipeline_from_file(config)
    except FileNotFoundError:
        click.echo('Could not find file: %s' % config)
    except json.decoder.JSONDecodeError as e:
        click.echo('JSON decoding error "%s"' % str(e))
    except PipetreeError as e:
        click.echo(str(e))
    else:
        click.echo('Successfully generated pipeline from %s' % config)


@cli.command('local')
@click.option('--pipeline-config', default="./pipeline.json")
@click.pass_context
def local(ctx, pipeline_config="./pipeline.json"):
    """Runs a local instance of the pipetree arbiter
    loading the pipeline config specified at FILEPATH"""
    try:
        arbiter = LocalArbiter(pipeline_config)
        arbiter.run_event_loop()
    except Exception as e:
        if ctx.obj['debug']:
            raise e
        else:
            print(e)

@cli.group('cluster')
@click.option('--aws-profile', default=None,
              help='AWS Profile to use for cluster commands')
@click.pass_context
def cluster(ctx, aws_profile):
    if not ctx.obj:
        ctx.obj = {}
    ctx.obj['aws_profile'] = aws_profile

@cluster.command('create')
@click.option('--cluster-config', default="./cluster.json")
@click.pass_context
def cluster_create(ctx, cluster_config):
    cluster = PipetreeCluster(aws_profile=ctx.obj['aws_profile'])
    cluster_cfg =  _load_json_file(cluster_config)
    click.echo("Creating cluster with config:")
    click.echo(json.dumps(cluster_cfg, indent=4))
    cluster.load_config(cluster_cfg)
    cluster.create_cluster()

@cluster.command('delete')
@click.option('--cluster-config', default="./cluster.json")
@click.pass_context
def cluster_delete(ctx, cluster_config):
    cluster_cfg =  _load_json_file(cluster_config)
    cluster = PipetreeCluster(aws_profile=ctx.obj['aws_profile'])
    cluster.load_config(cluster_cfg)
    click.echo("Deleting cluster %s" % cluster_cfg['cluster_name'])
    cluster.delete_cluster()

@cluster.command('logs')
@click.option('--cluster-config', default="./cluster.json")
@click.option('--n', default=100,
                help="Maximum number log entries to retrieve per server")
@click.pass_context
def cluster_logs(ctx, cluster_config, n):
    cluster_cfg =  _load_json_file(cluster_config)
    cluster = PipetreeCluster(aws_profile=ctx.obj['aws_profile'])
    cluster.load_config(cluster_cfg)
    logs = cluster.tail_logs(n)
    for resource in logs:
        click.echo("Logs for server %s:" % resource)
        for msg in logs[resource]:
            print(msg)

@cluster.command('run')
@click.option('--pipeline-config', default="./pipeline.json")
@click.option('--cluster-config', default="./cluster.json")
@click.pass_context
def cluster_run(ctx, pipeline_config,  cluster_config):
    pipeline_cfg = _load_json_file(pipeline_config)
    pipetree_cluster = PipetreeCluster(aws_profile=ctx.obj['aws_profile'])
    cluster_cfg =  _load_json_file(cluster_config)
    click.echo("Creating cluster with config:")
    click.echo(json.dumps(cluster_cfg, indent=4))
    pipetree_cluster.load_config(cluster_cfg)
    pipetree_cluster.create_cluster()
    """
    (dId, status) = pipetree_cluster.deploy_application(os.path.join(os.getcwd()))
    if status != "Succeeded":
        print("Deployment failed with status '%s'" % status)
        return
    """
    pipetree_cluster.run_arbiter(os.path.join(ctx.obj['project_dir'], pipeline_config))

def main():
    cli(obj={})
