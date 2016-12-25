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
import click
import subprocess

from pipetree.cli.utils import _get_config_path, _assert_in_project_dir
from pipetree import __version__ as pipetree_version
from pipetree.templates import DEFAULT_CONFIG, DEFAULT_HANDLERS
from pipetree.pipeline import PipelineFactory
from pipetree.exceptions import PipetreeError


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
    os.makedirs(pipetree_dir)
    with open(config, 'w') as f:
        f.write(DEFAULT_CONFIG % project_name)

    py_package = os.path.join(project_name, project_name)
    os.makedirs(py_package)
    with open(os.path.join(py_package, '__init__.py'), 'w'):
        pass
    with open(os.path.join(py_package, '%s.py' % project_name), 'w') as f:
        f.write(DEFAULT_HANDLERS)
    click.echo("Created new project %s" % project_name)


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


def main():
    cli(obj={})
