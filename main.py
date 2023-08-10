import os
import json
import base64
import requests
import boto3
import uuid
from botocore.exceptions import NoCredentialsError
from requests.adapters import HTTPAdapter, Retry


def main():
    # initialization
    # 1. Prepare environments;
    # 2. AWS services resources(sqs/sns/s3);
    # 3. Parameter map;
    # 4. SD api base url;
    awsRegion = os.getenv("AWS_REGION")
    sqsQueueUrl = os.getenv("SQS_QUEUE_URL")
    snsTopicArn = os.getenv("SNS_TOPIC_ARN")
    s3Bucket = os.getenv("S3_BUCKET")

    print(awsRegion)
    print(sqsQueueUrl)
    print(snsTopicArn)
    print(s3Bucket)

    sqsRes = boto3.resource('sqs')
    snsRes = boto3.resource('sns')
    s3Res = boto3.resource('s3')

    taskTransMap = {'text-to-image': 'txt2img', 'image-to-image': 'img2img',
               'extras-single-image': 'extra-single-image', 'extras-batch-images': 'extra-batch-images', 'interrogate': 'interrogate'}

    apiBaseUrl = "http://localhost:8080/sdapi/v1/"

    # main loop
    # todo: scaleDown hook
    # 1. Pull msg from sqs;
    # 2. Translate parameteres;
    # 3. (opt)Download and encode;
    # 4. Call SD API;
    # 5. Upload and notify;
    # 6. Delete msg;


if __name__ == '__main__':
    main()
