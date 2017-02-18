#!/bin/bash

echo "SETTING UP REDLEADER"
# Fetch and setup latest redleader
cd /var/pipetree_deps/
git clone https://github.com/mmcdermo/redleader.git
cd redleader
pip3.5 install -e .

echo "SETTING UP PIPETREE DEV"
# Fetch and setup latest pipetree
cd /var/pipetree_dev/
ls
pip3.5 install -r requirements.txt
pip3.5 install -e .
