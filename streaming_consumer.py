from kafka import KafkaConsumer
from json import loads
import json
import pyspark


streaming_consumer = KafkaConsumer('MyFirstKafkaTopic', bootstrap_servers = "localhost:9092")

for data in streaming_consumer:
    print(data)