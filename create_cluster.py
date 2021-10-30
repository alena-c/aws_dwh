# PREREQUISITES:
# - IAM user in your AWS account with `AdministratorAccess`
# - AWS secret and access key
# - Edit the file `dwh_start.cfg` and under [AWS] fill
# KEY= YOUR_AWS_KEY
# SECRET= YOUR_AWS_SECRET

import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError


# Load DWH Params from a file
config = configparser.ConfigParser()
config.read_file(open('dwh_start.cfg'))

KEY, SECRET = [*config['AWS'].values()]
DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_IAM_ROLE_NAME, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT = [*config['DWH'].values()]
print(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

# Create boto3 clients (EC2, S3, IAM, and Redshift)
ec2 = boto3.resource('ec2',
                     region_name="us-west-2",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                    )
s3 = boto3.resource('s3',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                   )
iam = boto3.client('iam',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   region_name='us-west-2'
                  )
redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                       )

#  Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
try:
    print('Creating a new IAM Role')
    dwhRole = iam.create_role(
        Path='/',
        RoleName = DWH_IAM_ROLE_NAME,
        Description = 'Allows Redshift clusters to call AWS services on your behalf.',
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'}
        )
    )
except Exception as e:
    print(e)

# Attach Policy
iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
                      )['ResponseMetadata']['HTTPStatusCode']
print('Policy Attached')

# Print out the IAM role ARN
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
print(f'IAM role ARN:\n{roleArn}')

# Create Redshift Cluster
try:
    response = redshift.create_cluster(        
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #parameter for role (to allow s3 access)
        IamRoles=[roleArn]
    )
except Exception as e:
    print(e)

print('Check the status with check_cluster_status.ipynb')