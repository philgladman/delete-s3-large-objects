import boto3
import jmespath
import datetime

# Variables for S3 Bucket name
BUCKET = "BUCKET_NAME"
REGION = "REGION_NAME"

# Connection to Amazon S3
s3 = boto3.client(
    's3',
    region_name=REGION
)

bucket_prefixes = s3.list_objects_v2(
    Bucket=BUCKET,
    Delimiter="/"
)

bucket_prefixes = jmespath.search('CommonPrefixes[].Prefix', bucket_prefixes)

for prefix in bucket_prefixes:
    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=BUCKET,
        Delimiter='string',
        Prefix=prefix
    )

    num = 0
    for i in response_iterator:
        start_time = datetime.datetime.utcnow()
        list_of_keys = jmespath.search('Contents[].[Key]', i)
        objects_xml = []
        for i in list_of_keys:
            objects_xml.append({'Key': i[0]})
        s3_delete_output = s3.delete_objects(
            Bucket=BUCKET,
            Delete={
                'Objects': objects_xml,
                'Quiet': False,
            },
        )
        num += len(objects_xml)
        end_time = datetime.datetime.utcnow()
        elasped_time = (end_time - start_time).total_seconds()
        print(f"Total objects deleted from s3://{BUCKET}/{prefix}: {num} Elapsed Time: {elasped_time}")
