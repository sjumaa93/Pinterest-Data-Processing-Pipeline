from kafka import KafkaConsumer
from json import loads
import json
import boto3

batch_consumer = KafkaConsumer('MyFirstKafkaTopic', bootstrap_servers = "localhost:9092")

for data in batch_consumer:
    print(data)