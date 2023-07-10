import boto3
import jmespath
import datetime

# Variables for S3 Bucket name and Bucket Prefix
BUCKET = "BUCKET_NAME"
PREFIX = "BUCKET_PREFIX"

# Assume IAM Role
session = boto3.Session(profile_name="default")
sts = session.client("sts")
assumed_role_object = sts.assume_role(
    RoleArn="ROLE_ARN",
    RoleSessionName="ROLE_SESSION_NAME",
    DurationSeconds=43200
)
credentials = assumed_role_object['Credentials']

# Connection to Amazon S3
s3 = boto3.client(
    's3',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'],
)

paginator = s3.get_paginator('list_objects_v2')
response_iterator = paginator.paginate(
    Bucket=BUCKET,
    Delimiter='string',
    Prefix=PREFIX
)

num = 0
for i in response_iterator:
    start_time = datetime.datetime.utcnow()
    list_of_keys = jmespath.search('Contents[].[Key,VersionId]', i)
    objects_xml = []
    for i in list_of_keys:
        objects_xml.append({'Key': i[0], 'VersionId': i[1]})
    s3_delete_output = s3.delete_objects(
        Bucket=BUCKET,
        Delete={
            'Objects': objects_xml,
            'Quiet': False,
        },
    )
    num += 1000
    end_time = datetime.datetime.utcnow()
    elasped_time = (end_time - start_time).total_seconds()
    print("Total objects deleted: ", f"{num:,}", "Elapsed Time: ", elasped_time)
