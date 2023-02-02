#!/bin/bash
ENVIRONMENT=$1
SANDBOX_ID=$2
REGION=$3
LOCAL_PORT=$4
ENDPOINT_TYPE=$5
AWS_PROFILE=$6

function show_requirements() {
	YELLOW='\033[1;33m'
	BACK_TO_NORMAL_COLOR='\033[0m'

	echo -e "${YELLOW}
---------------------
There was an error.
---------------------
${BACK_TO_NORMAL_COLOR}
Check if there were any error messages during the execution.

If error is unclear please check if the following packages are installed: postgresql, jq.
Installation: brew install <pkgname>

Or if the aws ssm plugin is installed:
Installation:
	curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/mac/sessionmanager-bundle.zip" -o "sessionmanager-bundle.zip"
	unzip sessionmanager-bundle.zip
	sudo ./sessionmanager-bundle/install -i /usr/local/sessionmanagerplugin -b /usr/local/bin/session-manager-plugin

or check source for other ssm install options https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html
	"
	exit
}

function on_error() {
	if [ $? -ne 0 ]; then
		show_requirements
	fi
}

trap cleanup INT TERM

RDSHOST="$(aws rds describe-db-cluster-endpoints \
	--region $REGION \
	--db-cluster-identifier aurora-cluster-$ENVIRONMENT-$SANDBOX_ID \
	--filter Name=db-cluster-endpoint-type,Values=\'$ENDPOINT_TYPE\' \
	--query 'DBClusterEndpoints[0].Endpoint' \
	--profile $AWS_PROFILE \
	| tr -d '"')"

INSTANCE_DATA=$(aws ec2 describe-instances \
	--filters "Name=tag:Name,Values=rds-$ENVIRONMENT-ssm-agent" "Name=instance-state-name,Values=running" \
	--output json \
	--region $REGION \
	--query "Reservations[*].Instances[*].{InstanceId:InstanceId, AvailabilityZone:Placement.AvailabilityZone}" \
	--profile $AWS_PROFILE)

INSTANCE_ID=$(echo $INSTANCE_DATA | jq -r '.[0][0].InstanceId')
AVAILABILITY_ZONE=$(echo $INSTANCE_DATA | jq -r '.[0][0].AvailabilityZone')

if [[ -z "$RDSHOST" || -z "$INSTANCE_ID" ]];
then
	exit 1
fi

tmp_socket_prefix=/tmp/tmp-tunnel

rm -f "${tmp_socket_prefix}"

ssh-keygen -t rsa -f $tmp_socket_prefix -N ''

AWS_PAGER="" aws ec2-instance-connect send-ssh-public-key --region $REGION --instance-id $INSTANCE_ID --availability-zone $AVAILABILITY_ZONE --instance-os-user ssm-user --ssh-public-key file://$tmp_socket_prefix.pub --profile $AWS_PROFILE

ssh -i $tmp_socket_prefix -N -f -M -S $tmp_socket_prefix-ssh.sock -L "$LOCAL_PORT:${RDSHOST}:5432" "ssm-user@${INSTANCE_ID}" -o "IdentitiesOnly yes" -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" -o ProxyCommand="aws ssm start-session --target %h --region ${REGION} --profile ${AWS_PROFILE} --document-name AWS-StartSSHSession --parameters portNumber=%p"