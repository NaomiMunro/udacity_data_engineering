import pandas as pd
import boto3
import json
import configparser
import sys

def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    try:
        print('1.1 Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument = json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                               'Effect': 'Allow',
                               'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)
    
def attach_policy(iam, DWH_IAM_ROLE_NAME):
    print('1.2 Attaching Policy')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                          PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
                          )['ResponseMetadata']['HTTPStatusCode']

def get_iam_role_arn(iam, DWH_IAM_ROLE_NAME):
    print('1.3 Get the IAM role ARN')
    return iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

def create_redshift_cluster(redshift, cluster_type, node_type, num_nodes, db, cluster_id, user, password, role_arn):
    print('1.4 Create the cluster')
    try:
        response = redshift.create_cluster(        
            # HW
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=int(num_nodes),

            # Identifiers and Credentials
            DBName = db,
            ClusterIdentifier=cluster_id,
            MasterUsername=user,
            MasterUserPassword=password,

            # Roles (for s3 access)
            IamRoles=[role_arn]   
        )
    except Exception as e:
        print(e)
    
def check_status(redshift, ec2, cluster_id, port):
    print("Checking status")
    cluster = redshift.describe_clusters(ClusterIdentifier=cluster_id)['Clusters'][0]
    status = cluster['ClusterStatus']

    if status == 'available':
        endpoint = cluster['Endpoint']['Address']
        arn = cluster['IamRoles'][0]['IamRoleArn']
        print("DWH_ENDPOINT :: ", endpoint)
        print("DWH_ROLE_ARN :: ", arn)

        try:
            vpc = ec2.Vpc(id=cluster['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            print(defaultSg)

            defaultSg.authorize_ingress(
                GroupName= defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(port),
                ToPort=int(port)
            )
        except Exception as e:
            print(e)
    else:
        print("Cluster status: " + status)
    
def delete_cluster(redshift, iam, cluster_id, role_name):
    redshift.delete_cluster( ClusterIdentifier=cluster_id,  SkipFinalClusterSnapshot=True)
    iam.detach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=role_name)

def main():
    command = sys.argv[1]

    config = configparser.ConfigParser()
    config.read_file(open('redshift.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    session = boto3.Session(
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name='us-west-2'
    )

    ec2 = session.resource('ec2')
    s3 = session.resource('s3')
    iam = session.client('iam')
    redshift = session.client('redshift')
    
    if command == 'create':
        create_iam_role(iam, DWH_IAM_ROLE_NAME)
        attach_policy(iam, DWH_IAM_ROLE_NAME)
        role_arn = get_iam_role_arn(iam, DWH_IAM_ROLE_NAME)
        create_redshift_cluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB,
                                DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD,  role_arn)
    elif command == 'status':
        check_status(redshift, ec2, DWH_CLUSTER_IDENTIFIER, DWH_PORT)
    elif command == 'delete':
        delete_cluster(redshift, iam, DWH_CLUSTER_IDENTIFIER, DWH_IAM_ROLE_NAME)

if __name__ == "__main__":
    main()
    
