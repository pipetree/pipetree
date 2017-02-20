#!/bin/bash
echo "About to build user image"
ls /var/pipetree/
docker build /var/pipetree/ -t pipetree-user-image
echo "User Image Built"
#export INSTANCE_ID=$(curl http://169.254.169.254/latest/meta-data/instance-id)
#export INSTANCE_TYPE=$(curl http://169.254.169.254/latest/meta-data/instance-type)
export RL_ID=$(cat /home/ec2-user/red_leader_resource_id)
echo "RL ID $RL_ID"
# Note that we need to pipe STDOUT, STDERR and STDIN to /dev/null AND background the task
# in order for the ApplicationStart phase of codedeploy not to hang.
# See: http://stackoverflow.com/questions/32085476/aws-code-deploy-appspec-isnt-starting-my-server
docker run -v /var/pipetree/:/var/pipetree/  --log-driver=awslogs --log-opt awslogs-region=us-west-1  --log-opt awslogs-group=pipetreeContainerLogs --log-opt awslogs-stream=${RL_ID} -t pipetree-user-image > /dev/null 2> /dev/null < /dev/null &
exit 0
