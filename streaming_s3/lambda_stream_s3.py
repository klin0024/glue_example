import json
import urllib.parse
import boto3

kinesis = boto3.client('kinesis')

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    obj = "s3://{bucket}/{key}".format(bucket=bucket,key=key)
    streamName = "glue"
    partitionKey = "glue"
    
    print("object: "+ obj )

    try:
        data = {"obj": obj}
        response = kinesis.put_record(
                StreamName=streamName,
                Data=json.dumps(data),
                PartitionKey=partitionKey)
        print(response)
        return response
    except Exception as e:
        print(e)
        raise e