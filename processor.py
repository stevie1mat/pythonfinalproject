import os
import json
import base64
import boto3
from decimal import Decimal

try:
    # We are loading the table name from environment variable
    table_name = os.environ['TABLE_NAME']

    # We are connecting to DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    print(f"Connected to DynamoDB and found the table '{table_name}'.")
except Exception as e:
    print("Failed to connect to DynamoDB or find the table:", str(e))

def lambda_handler(event, context):
    print("Lambda started. Event received!")

    # We will loop through each record in the event
    for record in event['Records']:
        try:
            # We have to decode the data from Kinesis
            data_bytes = base64.b64decode(record['kinesis']['data'])  # Base64 is converted to bytes here
            data_str = data_bytes.decode('utf-8')                      # Bytes is converted to string here
            print("Data from stream:", data_str)

            # We will convert the string to dictionary
            data = json.loads(data_str)

            # We will add a new value: comfort_index which is a fake formula
            temp = data['temperature']
            hum = data['humidity']
            comfort_index = temp - (0.55 * (1 - hum / 100) * (temp - 14.5))
            data['comfort_index'] = round(comfort_index, 2)

            # We will convert float values to decimal because DynamoDB needs this
            data['temperature'] = Decimal(str(data['temperature']))
            data['humidity'] = Decimal(str(data['humidity']))
            data['comfort_index'] = Decimal(str(data['comfort_index']))

            # We will save it to DynamoDB
            table.put_item(Item=data)
            print("Data saved to DynamoDB!")

        except Exception as e:
            # If something breaks we will show the error
            print("ERROR:", str(e))

    print("Lambda finished.")
    return {
        'statusCode': 200,
        'body': 'Done'
    }
