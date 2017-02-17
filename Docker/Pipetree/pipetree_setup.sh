#!/bin/bash

# Fetch and setup latest pipetree
git clone https://github.com/pipetree/pipetree.git
cd pipetree
git fetch --all
git checkout cluster
pip3.5 install -r requirements.txt
pip3.5 install -e .

# Fetch and setup latest redleader
cd ../
git clone https://github.com/mmcdermo/redleader.git
cd redleader
pip3.5 install -e .
