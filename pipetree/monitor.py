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
import reprint
import asciitree
import math
import time
import random

class Monitor(object):
    def __init__(self):
        pass

    def log_stage_run_init(self, stage, pipeline_run_id):
        pass

    def log_artifact_produced(self, stage, artifact, pipeline_run_id):
        pass

    def log_stage_run_complete(self, stage, pipeline_run_id):
        pass

    async def run(self):
        pass

class CLIMonitor(Monitor):
    def __init__(self, **kwargs):
        super().__init__()
        self._aws_manager = kwargs['aws_manager']

    def log_stage_run_init(self, stage, pipeline_run_id):
        print("Stage %s initialized for pipeline run %s" % (stage, pipeline_run_id))

    def log_artifact_produced(self, stage, artifact, pipeline_run_id):
        print("Artifact %s produced for stage %s, pipeline run %s" % (artifact, stage, pipeline_run_id))

    def log_stage_run_complete(self, stage, pipeline_run_id):
        print("Stage %s complete for pipeline run %s" % (stage, pipeline_run_id))

    async def run(self):
        root = Node('root', [
            Node('sub1', []),
            Node('sub2', [
                Node('sub2sub1', [])
            ]),
            Node('sub3', [
                Node('sub3sub1', [
                    Node('sub3sub1sub1', [])
                ]),
                Node('sub3sub2', [])
            ])
        ])

        buf = asciitree.draw_tree(root).split("\n")
        print("\n".join(buf))
        return
        with reprint.output(output_type="list", initial_len=10, interval=0) as output_list:
            for x in range(40):
                for y in range(7):
                    label = "[ ]"
                    if x > 10:
                        label = "[*]"
                    if x >= 39:
                        label = "[C]"
                    t = min_pad(" %s min" % (x * y), 8)
                    progress = "[" + ("=" * x) + (" " * (40 - x))+ "]"
                    progress = center_text(progress, "Server %s" % y)
                    output_list[y] = "\t "+ progress + "\t" + t + "\t" + label + "\t" + buf[y]
                time.sleep(0.2)

    def display_state(self, pipeline_run_id):
        print("DISPLAY STATE")

def min_pad(text, l):
    if len(text) > l:
        return text
    return text + (" " * (l - len(text)))

def center_text(buf, text):
    if len(text) > len(buf):
        text = text[:len(buf) - 2]
    start = max(0, math.floor((len(buf) - len(text)) / 2))
    lbuf = list(buf)
    for x in range(len(text)):
        lbuf[start + x] = text[x]
    return "".join(lbuf)

class Node(object):
    def __init__(self, name, children):
        self.name = name
        self.children = children

    def __str__(self):
        return self.name
