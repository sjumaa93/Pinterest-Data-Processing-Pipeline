from time import sleep
from kafka import KafkaConsumer
from json import loads
from json import dumps
import json
import boto3
from numpy import insert
from sqlalchemy import create_engine

s3 = boto3.resource('s3')

batch_consumer = KafkaConsumer(
    'MyFirstKafkaTopic', bootstrap_servers = "localhost:9092",
    value_deserializer = lambda pinmessage: loads(pinmessage), 
    #auto_offset_reset = "earliest"
    )

batch_consumer.subscribe(topics = "MyFirstKafkaTopic")

file_index = 0

for message in batch_consumer:
    file_index += 1
    sleep(2)
    file_name = 'data/data_' + str(file_index) + '.json'
    with open(file_name, "w") as f:
        json.dump(message, f)
    s3.meta.client.upload_file(f'./{file_name}', 'pinterest-data-bucket', f'{file_name}')