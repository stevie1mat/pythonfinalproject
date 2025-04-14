import os                  
import boto3                
import json                  
import random               
import time                  
from datetime import datetime  
from dotenv import load_dotenv  
from botocore.exceptions import BotoCoreError, ClientError  

# We will load environment variables from the .env file
load_dotenv()

# We will get the Kinesis stream name and AWS region from environment variables
STREAM_NAME = os.environ["STREAM_NAME"]
AWS_REGION = os.environ["AWS_REGION_GENERATOR"]

# We will use try catch to see if we can initialize the Kinesis client using boto3
try:
    kinesis = boto3.client('kinesis', region_name=AWS_REGION)
    print(f"Successfully connected to Kinesis in region {AWS_REGION}.\n")
except Exception as e:
    print("Failed to connect to Kinesis:", str(e))
    exit(1)

# We will this function to generate fake sensor data. This is a dictionary.
def generate_data():
    data = {
        "sensor_id": random.randint(1, 4),  
        "temperature": random.uniform(20.0, 30.0), 
        "humidity": random.uniform(30.0, 50.0), 
        "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')  # We will fetch the current UTC time
    }

    # We will print the generated data to the console and see the data
    print("Generated Sensor Data:")
    print("Sensor ID:", data["sensor_id"])
    print("Temperature:", round(data["temperature"], 2))
    print("Humidity:", round(data["humidity"], 2))
    print("Timestamp:", data["timestamp"])

    return data


while True:
        # We will generate new sensor reading
        data = generate_data()  
        print("Sending data to Kinesis stream:", STREAM_NAME)

        # We will use try & catch to see if we are able to send data to Kinesis
        try:
            response = kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(data),  # We will convert the data to JSON format
                PartitionKey=str(data["sensor_id"])  # We will also group by sensor ID
            )
            print("Data was sent successfully | Record ID:", response['SequenceNumber'])

        except (BotoCoreError, ClientError) as e:
            print("Error sending data to Kinesis:", str(e))

        # We will wait for 10 seconds before sending the next record
        time.sleep(10)
