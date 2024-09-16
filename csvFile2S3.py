import boto3

client = boto3.client(
    's3',
    aws_access_key_id='AKIA6RL6JU7KLBKYITUK',
    aws_secret_access_key='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',
    region_name='ap-northeast-1'
)

Filename = './sum_work_files/pb_access_marker_machine_amounts.csv'
Bucket = 'brownreverse-kpi-bi-bucket'
Key = 'pb_access_marker_machine_amounts.csv'
client.upload_file(Filename, Bucket, Key)