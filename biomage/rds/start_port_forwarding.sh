#!/bin/bash
RDSHOST="$(aws rds describe-db-cluster-endpoints \
	--region eu-west-1 \
	--db-cluster-identifier aurora-cluster-$1 \
	--filter Name=db-cluster-endpoint-type,Values='writer' \
	--query 'DBClusterEndpoints[0].Endpoint' \
	| tr -d '"')"

INSTANCE_DATA=$(aws ec2 describe-instances \
	--filters "Name=tag:Name,Values=rds-$1-ssm-agent" \
	--output json \
	--query "Reservations[*].Instances[*].{InstanceId:InstanceId, AvailabilityZone:Placement.AvailabilityZone}")

INSTANCE_ID=$(echo $INSTANCE_DATA | jq -r '.[0][0].InstanceId')
AVAILABILITY_ZONE=$(echo $INSTANCE_DATA | jq -r '.[0][0].AvailabilityZone')

ssh-keygen -t rsa -f temp -N ''

aws ec2-instance-connect send-ssh-public-key --instance-id $INSTANCE_ID --availability-zone $AVAILABILITY_ZONE --instance-os-user ssm-user --ssh-public-key file://temp.pub

ssh -i temp -N -f -M -S temp-ssh.sock -L "$2:${RDSHOST}:5432" "ssm-user@${INSTANCE_ID}" -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" -o ProxyCommand="aws ssm start-session --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p"

echo "Finished setting up, run \"biomage rds login\" in a different tab"
echo 
echo "------------------------------"
echo "Press any key to close session."
echo "------------------------------"
read
echo

ssh -O exit -S temp-ssh.sock *

rm temp*