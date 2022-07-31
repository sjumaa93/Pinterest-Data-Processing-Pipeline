from kafka import KafkaConsumer
from json import loads
from json import dumps
import json
import boto3
from numpy import insert
from sqlalchemy import create_engine

batch_consumer = KafkaConsumer('MyFirstKafkaTopic', bootstrap_servers = "localhost:9092")
client = boto3.client('s3')

for data in batch_consumer:
    with open("raw_data.json", "w") as f:
        json.dump(data, f)

#client.upload_file('raw_data.json', 'bucketnamehere', 'data.json')