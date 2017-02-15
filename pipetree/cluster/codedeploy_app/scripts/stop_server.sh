#!/bin/bash
killall python
docker stop $(docker ps -a -q)
ls
