import click

from ..utils.constants import STAGING

from subprocess import PIPE, run

@click.command()

@click.option(
    "-i",
    "--input_env",
    required=False,
    default=STAGING,
    show_default=True,
    help="Input environment of the RDS server.",
)

# Disabled, only 5432 works for now
# @click.option(
#     "-p",
#     "--local_port",
#     required=False,
#     default=5432,
#     show_default=True,
#     help="Local port from which to connect.",
# )

def start_port_forwarding(input_env, local_port = 5432):
    """
    Sets up a port forwarding session for the rds server in a given environment.\n

    E.g.:
    biomage rds start-port-forwarding -i staging
    """

    run(f"./biomage/rds/start_port_forwarding.sh {input_env} {local_port}", shell=True)

# Everything that is commented out here is a python version in case it is useful if we try to reimplement it in python instead
# def start_port_forwarding(input_env, local_port):
#     """
#     Sets up a port forwarding session for the rds server in a given environment.\n

#     E.g.:
#     biomage rds start_port_forwarding -i staging -p 5432
#     """
#     
#     rds_client = boto3.client("rds")
#     ec2_client = boto3.client("ec2")
    
#     [rds_agent_instance_id, rds_agent_availability_zone] = get_rds_agent_params(ec2_client)

#     rds_endpoint = get_rds_writer_endpoint(input_env, rds_client)

#     # ec2_instance_connect_client = boto3.client('ec2-instance-connect')

#     # public_key = generate_temp_ssh_keys()

#     # with open('temp.pub', 'r') as public_key_file:
#     #     ec2_instance_connect_client.send_ssh_public_key(
#     #         InstanceId=rds_agent_instance_id,
#     #         InstanceOSUser="ssm-user",
#     #         SSHPublicKey= public_key_file.read(),
#     #         AvailabilityZone=rds_agent_availability_zone
#     #     )

#     Popen(f"""ssh -i temp -N -f -M -S temp-ssh.sock -L "5432:{rds_endpoint}:5432" "ssm-user@{rds_agent_instance_id}" -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" -o ProxyCommand="aws ssm start-session --target {rds_agent_instance_id} --document-name AWS-StartSSHSession --parameters portNumber=5432" """, shell=True)

#     input("Press any key to close session.")

#     Popen("ssh -O exit -S temp-ssh.sock *", shell=True)

#     # p = Popen(
#     #     f"""ssh-keygen -t rsa -f temp -N ''
#     #         aws ec2-instance-connect send-ssh-public-key --instance-id $INSTANCE_ID --availability-zone $AVAILABILITY_ZONE --instance-os-user ssm-user --ssh-public-key file://temp.pub
#     #         ssh -i temp -N -f -M -S temp-ssh.sock -L "5432:${rds_endpoint}:5432" "ssm-user@${rds_agent_instance_id}" -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" -o ProxyCommand="aws ssm start-session --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p"
    
#     #         echo "Press any key to close session."
#     #         read -rsn
#     #         echo

#     #         ssh -O exit -S temp-ssh.sock *

#     #         rm temp*
#     #     """
#     # )

#     # ssh-keygen -t rsa -f temp -N ''

#     # aws ec2-instance-connect send-ssh-public-key --instance-id $INSTANCE_ID --availability-zone $AVAILABILITY_ZONE --instance-os-user ssm-user --ssh-public-key file://temp.pub

#     # ssh -i temp -N -f -M -S temp-ssh.sock -L "5432:${RDSHOST}:5432" "ssm-user@${INSTANCE_ID}" -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" -o ProxyCommand="aws ssm start-session --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p"

#     # echo "Press any key to close session."
#     # read -rsn
#     # echo

#     # ssh -O exit -S temp-ssh.sock *

#     # rm temp*

#     # RDSHOST="$(aws rds describe-db-cluster-endpoints \
# 	# --region eu-west-1 \
# 	# --db-cluster-identifier aurora-cluster-staging \
# 	# --filter Name=db-cluster-endpoint-type,Values='writer' \
# 	# --query 'DBClusterEndpoints[0].Endpoint' \
# 	# | tr -d '"')"

#     # PG_ROLE=dev_role

#     # PASSWORD="$(aws rds generate-db-auth-token --hostname=$RDSHOST --port=5432 --region=eu-west-1 --username=$PG_ROLE)"

#     # PGPASSWORD=$PASSWORD psql --host=localhost --port=5432 --username=dev_role --dbname=aurora_db

# # Find the ec2 agent and get its instance od and the availabilty zone it is in
# def get_rds_agent_params(ec2_client):
#     response = ec2_client.describe_instances(
#         Filters=[
#             {
#                 'Name': 'tag:Name',
#                 'Values': ['rds-staging-ssm-agent']
#             },
#         ],
#     )

#     instanceProperties = response["Reservations"][0]["Instances"][0]

#     rds_agent_instance_id = instanceProperties["InstanceId"]
#     rds_agent_availability_zone = instanceProperties["Placement"]["AvailabilityZone"]

#     return [rds_agent_instance_id, rds_agent_availability_zone]

# def get_rds_writer_endpoint(input_env, rds_client):
#     response = rds_client.describe_db_cluster_endpoints(
#         DBClusterIdentifier=f"aurora-cluster-{input_env}",
#         Filters=[
#             {
#                 'Name': 'db-cluster-endpoint-type',
#                 'Values': ['writer']
#             },
#         ],
#     )

#     return response["DBClusterEndpoints"][0]["Endpoint"]

# def generate_temp_ssh_keys():
#     key = rsa.generate_private_key(
#         backend=crypto_default_backend(),
#         public_exponent=65537,
#         key_size=2048
#     )

#     # private_key = key.private_bytes(
#     #     crypto_serialization.Encoding.PEM,
#     #     crypto_serialization.PrivateFormat.PKCS8,
#     #     crypto_serialization.NoEncryption()
#     # )

#     save_key(key, 'temp')

#     public_key = key.public_key().public_bytes(
#         crypto_serialization.Encoding.OpenSSH,
#         crypto_serialization.PublicFormat.OpenSSH
#     )

#     return public_key

# def save_key(key, filename):
#     private_key = key.private_bytes(
#         encoding=crypto_serialization.Encoding.PEM,
#         format=crypto_serialization.PrivateFormat.TraditionalOpenSSL,
#         encryption_algorithm=crypto_serialization.NoEncryption()
#     )

#     public_key = key.public_key().public_bytes(
#         crypto_serialization.Encoding.OpenSSH,
#         crypto_serialization.PublicFormat.OpenSSH
#     )

#     with open(filename, 'wb') as private_file:
#         private_file.write(private_key)

#     with open(f'{filename}.pub', 'wb') as pub_out:
#         pub_out.write(public_key)
#         pub_out.write(b"\n")