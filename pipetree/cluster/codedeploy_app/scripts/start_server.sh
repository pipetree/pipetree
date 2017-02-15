#!/bin/bash
echo "About to build user image"
ls /var/pipetree/
docker build /var/pipetree/ -t pipetree-user-image
echo "User Image Built"
docker run -v /var/pipetree/:/var/pipetree/ -t pipetree-user-image
