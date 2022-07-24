from kafka import KafkaConsumer
from json import loads
import json


streaming_consumer = KafkaConsumer('MyFirstKafkaTopic', bootstrap_servers = "localhost:9092")

for data in streaming_consumer:
    print(data)