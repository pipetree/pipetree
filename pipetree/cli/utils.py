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
import os.path
import click


def _get_config_path(ctx):
    project_dir = ctx.obj['project_dir']
    return os.path.join(project_dir, '.pipetree', 'config.json')


def _assert_in_project_dir(path):
    if '.pipetree' not in os.listdir(path):
        click.echo('Fatal: Not a pipetree directory. Consider running "pipetree init" to start your project. ')
        raise click.Abort()

def _load_json_file(path):
    _assert_file_exists(path)
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception as e:
        click.echo('Fatal: Malformed JSON in file: %s. (%s)' % (path, e))
        raise click.Abort()

def _assert_file_exists(path):
    if not os.path.isfile(path):
        click.echo('Fatal: Required file %s doesn\'t exist.' % path)
        raise click.Abort()
