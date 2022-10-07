import boto3
import uuid
import logging

from os import path
from PIL import Image
from urllib.parse import unquote_plus


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def resize_image(image_path, resized_path):
    with Image.open(image_path) as image:
        image.thumbnail(tuple(x / 2 for x in image.size))
        image.save(resized_path)

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        logger.info('Key: %s', key)

        tmpkey = key.replace('/', '')

        logger.info('Temp Key: %s', tmpkey)

        download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
        upload_path = '/tmp/resized-{}'.format(tmpkey)
        s3_client.download_file(bucket, key, download_path)
        resize_image(download_path, upload_path)

        if path.isfile(upload_path):
            logger.info('Image has been created locally')
        else:
            logger.info('Resized image does not exist')

        s3_client.upload_file(upload_path, '{}-resized'.format(bucket), key)