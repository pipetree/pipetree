#!/bin/bash
sudo yum update -y
sudo yum install -y docker
sudo usermod -a -G docker ec2-user
sudo service docker start
docker rm $(docker ps -a -q) -f
docker rmi $(docker images -q) -f
echo "BEFORE INSTALL"
