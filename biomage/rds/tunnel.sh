#!/bin/bash
ENVIRONMENT=$1
REGION=$2
LOCAL_PORT=$3
ENDPOINT_TYPE=$4

function show_requirements() {
	echo '
---------------------
There was an error. 
---------------------

Please check if the following packages are installed: postgresql, jq.
Installation: brew install <pkgname>

Or if the aws ssm plugin is installed:
Installation:
	curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/mac/sessionmanager-bundle.zip" -o "sessionmanager-bundle.zip"
	unzip sessionmanager-bundle.zip
	sudo ./sessionmanager-bundle/install -i /usr/local/sessionmanagerplugin -b /usr/local/bin/session-manager-plugin

or check source for other ssm install options https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html
	'
	exit
}

function cleanup() {
	if [ $? -ne 0 ]; then
		show_requirements
	fi


	ssh -O exit -S temp-ssh.sock *
	rm temp*
	echo "Finished cleaning up"
}

trap cleanup EXIT

RDSHOST="$(aws rds describe-db-cluster-endpoints \
	--region $REGION \
	--db-cluster-identifier aurora-cluster-$ENVIRONMENT \
	--filter Name=db-cluster-endpoint-type,Values=\'$ENDPOINT_TYPE\' \
	--query 'DBClusterEndpoints[0].Endpoint' \
	| tr -d '"')"

INSTANCE_DATA=$(aws ec2 describe-instances \
	--filters "Name=tag:Name,Values=rds-$ENVIRONMENT-ssm-agent" \
	--output json \
	--query "Reservations[*].Instances[*].{InstanceId:InstanceId, AvailabilityZone:Placement.AvailabilityZone}")

INSTANCE_ID=$(echo $INSTANCE_DATA | jq -r '.[0][0].InstanceId')
AVAILABILITY_ZONE=$(echo $INSTANCE_DATA | jq -r '.[0][0].AvailabilityZone')

ssh-keygen -t rsa -f temp -N ''

AWS_PAGER="" aws ec2-instance-connect send-ssh-public-key --instance-id $INSTANCE_ID --availability-zone $AVAILABILITY_ZONE --instance-os-user ssm-user --ssh-public-key file://temp.pub

ssh -i temp -N -f -M -S temp-ssh.sock -L "$LOCAL_PORT:${RDSHOST}:5432" "ssm-user@${INSTANCE_ID}" -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" -o ProxyCommand="aws ssm start-session --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p"

echo "Finished setting up, run \"biomage rds login -i $ENVIRONMENT -r $REGION\" in a different tab"
echo 
echo "------------------------------"
echo "Press enter to close session."
echo "------------------------------"
read
echo