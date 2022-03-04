#!/bin/bash
ENVIRONMENT=$1
REGION=$2
LOCAL_PORT=$3
ENDPOINT_TYPE=$4

function cleanup() {
	ssh -O exit -S temp-ssh.sock *
	rm temp*
	echo "Finished cleaning up"
}

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

trap cleanup EXIT

ssh -i temp -N -f -M -S temp-ssh.sock -L "$LOCAL_PORT:${RDSHOST}:5432" "ssm-user@${INSTANCE_ID}" -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" -o ProxyCommand="aws ssm start-session --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p"

echo "Finished setting up, run \"biomage rds login -i $ENVIRONMENT -r $REGION\" in a different tab"
echo 
echo "------------------------------"
echo "Press enter to close session."
echo "------------------------------"
read
echo