import json
import urllib.parse
import boto3

glue = boto3.client('glue')

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    obj = "s3://{bucket}/{key}".format(bucket=bucket,key=key)
    jobName = "trigger"
    
    print("object: "+ obj )

    try:
        response = glue.start_job_run(
               JobName = jobName,
               Arguments = { '--OBJECT': obj } 
               )
        
        print(response)
        return response
    except Exception as e:
        print(e)
        raise e