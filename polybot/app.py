import flask
from flask import request
import os
from bot import ObjectDetectionBot
import boto3
from botocore.exceptions import ClientError
import json
from loguru import logger

app = flask.Flask(__name__)

client = boto3.session.Session().client(service_name="secretsmanager",
                                        region_name="eu-north-1")

try:
    response = client.get_secret_value(SecretId="MOHMMAD_TELEGRAM_TOKEN")
except ClientError as e:
    raise e

TELEGRAM_TOKEN = json.loads(response['SecretString'])['TELEGRAM_APP_TOKEN']
TELEGRAM_APP_URL = os.environ['TELEGRAM_APP_URL']



@app.route('/', methods=['GET'])
def index():
    return 'Ok'


@app.route(f'/{TELEGRAM_TOKEN}/', methods=['POST'])
def webhook():
    req = request.get_json()
    bot.handle_message(req['message'])
    return 'Ok'

@app.route(f'/results', methods=['POST'])
def results():
    prediction_id = request.args.get('predictionId')

    # use the prediction_id to retrieve results from DynamoDB and send to the end-user
    # create a DynamoDB resource object
    dynamodb = boto3.resource('dynamodb', region_name="eu-north-1")

    # Specify the name of your DynamoDB table
    table = dynamodb.Table('mohmmad-poly-tb')

    # retrieve results from DynamoDB
    table_response = table.get_item(
        Key={
            'prediction_id': prediction_id,
        }
    )

    if 'Item' in table_response:
        item = table_response['Item']
        chat_id = item['chat_id']
        labels = item['labels']
        text_results = "Prediction Results:\n"
        detected_items = [label['class'] for label in labels]
        text_results += "\n".join(detected_items)
        bot.send_text(chat_id, text_results)
        return 'Ok'
    else:
        return 'Prediction ID not found', 404





@app.route(f'/loadTest/', methods=['POST'])
def load_test():
    req = request.get_json()
    bot.handle_message(req['message'])
    return 'Ok'


if __name__ == "__main__":
    bot = ObjectDetectionBot(TELEGRAM_TOKEN, TELEGRAM_APP_URL)

    app.run(host='0.0.0.0', port=8443, ssl_context=('/home/ubuntu/YOURPUBLIC.pem', '/home/ubuntu/YOURPRIVATE.key'))