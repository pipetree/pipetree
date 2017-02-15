#!/bin/bash
sudo yum update
sudo yum -y install python35
sudo yum -y install git
sudo python3 get-pip.py
echo "Installed pip"
pip install boto3
mkdir dependencies
cd dependencies
echo "About to clone into pipetree"
git clone https://github.com/pipetree/pipetree.git
cd pipetree

# Temp for testing
git fetch --all
git checkout cluster

# Install pipetree
sudo pip install -e .
echo "After install complete"
