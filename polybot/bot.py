import telebot
from loguru import logger
import os
import time
from telebot.types import InputFile
import boto3
import requests
import ast
from collections import Counter
import json


class Bot:

    def __init__(self, token, telegram_chat_url):
        # create a new instance of the TeleBot class.
        # all communication with Telegram servers are done using self.telegram_bot_client
        self.telegram_bot_client = telebot.TeleBot(token)

        # remove any existing webhooks configured in Telegram servers
        self.telegram_bot_client.remove_webhook()
        time.sleep(0.5)

        # set the webhook URL
        self.telegram_bot_client.set_webhook(url=f'{telegram_chat_url}/{token}/' ,timeout=60, certificate=open('/home/ubuntu/YOURPUBLIC.pem', 'r'))

        self.prev_path = ""

        logger.info(f'Telegram Bot information\n\n{self.telegram_bot_client.get_me()}')

    def send_text(self, chat_id, text):
        # added timeout 5 sec
        self.telegram_bot_client.send_message(chat_id, text, timeout=5)

    def send_text_with_quote(self, chat_id, text, quoted_msg_id):
        self.telegram_bot_client.send_message(chat_id, text, reply_to_message_id=quoted_msg_id, timeout=5)

    def is_current_msg_photo(self, msg):
        return 'photo' in msg

    def download_user_photo(self, msg):
        """
        Downloads the photos that sent to the Bot to `photos` directory (should be existed)
        :return:
        """
        if not self.is_current_msg_photo(msg):
            raise RuntimeError(f'Message content of type \'photo\' expected')

        file_info = self.telegram_bot_client.get_file(msg['photo'][-1]['file_id'])
        data = self.telegram_bot_client.download_file(file_info.file_path)
        folder_name = file_info.file_path.split('/')[0]

        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        with open(file_info.file_path, 'wb') as photo:
            photo.write(data)

        return file_info.file_path

    def send_photo(self, chat_id, img_path):
        if not os.path.exists(img_path):
            raise RuntimeError("Image path doesn't exist")
        # added timeout 5 sec
        self.telegram_bot_client.send_photo(
            chat_id,
            InputFile(img_path),
            timeout=5
        )

    def handle_message(self, msg):
        """Bot Main message handler"""
        logger.info(f'Incoming message: {msg}')
        self.send_text(msg['chat']['id'], f'Your original message: {msg["text"]}')


class ObjectDetectionBot(Bot):
    def handle_message(self, msg):
        logger.info(f'Incoming message: {msg}')

        images_bucket = os.environ['BUCKET_NAME']
        s3 = boto3.client('s3')

        if self.is_current_msg_photo(msg):
            photo_path = self.download_user_photo(msg)
            img_name=os.path.basename(photo_path)

            try:
                s3.upload_file(photo_path, images_bucket, img_name)
            except Exception as e:
                logger.error(e)


            #  send an HTTP request to the `yolo5` service for prediction
            # Define the URL and parameters

            try:
                logger.info('Sending message to SQS')
                sqs = boto3.client('sqs', region_name='eu-north-1')
                queue_url = 'https://sqs.eu-north-1.amazonaws.com/700935310038/mohmmad-sqs'
                message = {
                    'imgName': os.path.basename(photo_path),
                    'chat_id': msg['chat']['id']
                }
                sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(message)
                )
                logger.info('message sent to SQS')
            except Exception as e:
                logger.error(f'Error sending message to SQS: {e}')
                self.send_text(msg['chat']['id'],
                               f'Error sending message to SQS: {e}')
                return
            except Exception as e:
                logger.error(f'http request has failed {e}')

    def count_items(self,lst):
        counts = Counter(lst)
        return dict(counts)