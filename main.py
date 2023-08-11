import os
import io
import base64
import json
import datetime
import requests
import boto3
import logging
import uuid
from botocore.exceptions import ClientError
from PIL import PngImagePlugin, Image

logger = logging.getLogger(__name__)
awsRegion = os.getenv("AWS_REGION")
sqsQueueUrl = os.getenv("SQS_QUEUE_URL")
snsTopicArn = os.getenv("SNS_TOPIC_ARN")
s3Bucket = os.getenv("S3_BUCKET")


def main():
    # initialization
    # 1. Prepare environments;
    # 2. AWS services resources(sqs/sns/s3);
    # 3. Parameter map;
    # 4. SD api base url;

    print(awsRegion)
    print(sqsQueueUrl)
    print(snsTopicArn)
    print(s3Bucket)

    sqsRes = boto3.resource('sqs')
    queue = sqsRes.Queue(sqsQueueUrl)
    SQS_WAIT_TIME_SECONDS = 20

    snsRes = boto3.resource('sns')
    s3Res = boto3.resource('s3')
    bucket = s3Res.Bucket(s3Bucket)

    taskTransMap = {'text-to-image': 'txt2img', 'image-to-image': 'img2img',
                    'extras-single-image': 'extra-single-image', 'extras-batch-images': 'extra-batch-images', 'interrogate': 'interrogate'}

    apiBaseUrl = "http://localhost:8080/sdapi/v1/"

    # main loop
    # todo: Implement scaleDown hook signal
    # 1. Pull msg from sqs;
    # 2. Translate parameteres;
    # 3. (opt)Download and encode;
    # 4. Call SD API;
    # 5. Upload and notify;
    # 6. Delete msg;
    while True:
        received_messages = receive_messages(queue, 1, SQS_WAIT_TIME_SECONDS)
        for message in received_messages:
            payload = json.loads(message.body)

            taskHeader = payload.pop('alwayson_scripts', None)
            taskType = taskHeader['task']
            apiFullPath = apiBaseUrl + taskTransMap[taskType]

            if taskType == 'text-to-image':
                r = invoke_txt2img(apiFullPath, payload)
                imgOutputs = post_invocations(bucket, r['images'], 80)
                print(json.dumps(notify(imgOutputs, r, taskHeader)))
                delete_message(message)


def receive_messages(queue, max_number, wait_time):
    """
    Receive a batch of messages in a single request from an SQS queue.

    :param queue: The queue from which to receive messages.
    :param max_number: The maximum number of messages to receive. The actual number
                       of messages received might be less.
    :param wait_time: The maximum time to wait (in seconds) before returning. When
                      this number is greater than zero, long polling is used. This
                      can result in reduced costs and fewer false empty responses.
    :return: The list of Message objects received. These each contain the body
             of the message and metadata and custom attributes.
    """
    try:
        messages = queue.receive_messages(
            MaxNumberOfMessages=max_number,
            WaitTimeSeconds=wait_time
        )
        for msg in messages:
            logger.info("Received message: %s: %s", msg.message_id, msg.body)
    except ClientError as error:
        logger.exception("Couldn't receive messages from queue: %s", queue)
        raise error
    else:
        return messages


def delete_message(message):
    """
    Delete a message from a queue. Clients must delete messages after they
    are received and processed to remove them from the queue.

    :param message: The message to delete. The message's queue URL is contained in
                    the message's metadata.
    :return: None
    """
    try:
        message.delete()
        logger.info("Deleted message: %s", message.message_id)
    except ClientError as error:
        logger.exception("Couldn't delete message: %s", message.message_id)
        raise error


def decode_to_image(encoding):
    image = None
    try:
        if encoding.startswith("http://") or encoding.startswith("https://"):
            response = requests.get(encoding)
            if response.status_code == 200:
                encoding = response.text
                image = Image.open(io.BytesIO(response.content))
        # elif encoding.startswith("s3://"):
        else:
            if encoding.startswith("data:image/"):
                encoding = encoding.split(";")[1].split(",")[1]
            image = Image.open(io.BytesIO(base64.b64decode(encoding)))
        return image
    except Exception as err:
        return None


def export_pil_to_bytes(image, quality):
    with io.BytesIO() as output_bytes:

        use_metadata = False
        metadata = PngImagePlugin.PngInfo()
        for key, value in image.info.items():
            if isinstance(key, str) and isinstance(value, str):
                metadata.add_text(key, value)
                use_metadata = True
        image.save(output_bytes, format="PNG", pnginfo=(
            metadata if use_metadata else None), quality=quality if quality else 80)

        bytes_data = output_bytes.getvalue()

    return bytes_data


def invoke_txt2img(url, json):
    response = requests.post(url=url, json=json)
    return response.json()


def notify(images, response, header):
    n_iter = response['parameters']['n_iter']
    batch_size = response['parameters']['batch_size']
    parameters = {}
    parameters['id_task'] = header['id_task']
    parameters['status'] = 1
    parameters['image_url'] = ','.join(
        images[: n_iter * batch_size])
    parameters['seed'] = ','.join(
        str(x) for x in json.loads(response['info'])['all_seeds'])
    parameters['error_msg'] = ''
    parameters['image_mask_url'] = ','.join(
        images[n_iter * batch_size:])
    return {
        'images': [''],
        'parameters': parameters,
        'info': ''
    }


def post_invocations(bucket, b64images, quality):
    saveDir = datetime.date.today().strftime("%Y-%m-%d")
    if True:
        images = []
        for b64image in b64images:
            bytesData = export_pil_to_bytes(
                decode_to_image(b64image), quality)
            imageId = datetime.datetime.now().strftime(
                f"%Y%m%d%H%M%S-{uuid.uuid4()}")
            suffix = 'png'
            bucket.put_object(
                Body=bytesData,
                Key=f'{saveDir}/{imageId}.{suffix}',
                ContentType=f'image/{suffix}'
            )
            images.append(f's3://{s3Bucket}/{saveDir}/{imageId}.{suffix}')
        return images
    else:
        return b64images


if __name__ == '__main__':
    main()
