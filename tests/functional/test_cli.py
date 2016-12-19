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
import unittest
from unittest import mock
from click.testing import CliRunner

from pipetree.cli import cli
from pipetree.templates import DEFAULT_CONFIG


class TestInit(unittest.TestCase):
    def setUp(self):
        self.dirname = 'foo'
        self.config_path = '%s/.pipetree/config.json' % self.dirname

        self.runner = CliRunner()
        self.fs = self.runner.isolated_filesystem()
        self.fs.__enter__()
        self.runner.invoke(cli, ['init', self.dirname])

    def tearDown(self):
        self.fs.__exit__(None, None, None)

    def test_project_dir_name(self):
        self.assertEqual(os.listdir('.')[0],
                         self.dirname)

    def test_pipetree_dir(self):
        self.assertIn('.pipetree',
                      os.listdir(self.dirname))

    def test_config_json(self):
        with open(self.config_path, 'r') as f:
            self.assertEqual(DEFAULT_CONFIG % self.dirname,
                             f.read())

class TestConfig(unittest.TestCase):
    def setUp(self):
        self.dirname = 'foo'
        self.runner = CliRunner()
        self.fs = self.runner.isolated_filesystem()
        self.fs.__enter__()
        self.runner.invoke(cli, ['init', self.dirname])
        os.chdir(self.dirname)

    def tearDown(self):
        self.fs.__exit__(None, None, None)

    def test_get_config(self):
        result = self.runner.invoke(cli, ['config', 'get', 'project_name'])
        self.assertEqual(result.output, '%s\n' % self.dirname)

    def test_set_config(self):
        self.runner.invoke(cli, ['config', 'set', 'project_name', 'bar'])
        result = self.runner.invoke(cli, ['config', 'get', 'project_name'])
        self.assertEqual(result.output, 'bar\n')
        self.runner.invoke(cli, ['config', 'set', 'project_name', self.dirname])
