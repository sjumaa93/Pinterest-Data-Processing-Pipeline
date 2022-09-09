from optparse import Option
import os
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.1.0 s3_to_spark.py pyspark-shell'

spark = SparkSession.builder \
    .appName("SparkToCassandra") \
    .getOrCreate()

models_dataframe = spark \
    .read \
    .format('csv')\
    .options(header='True', inferSchema='True', delimiter=',') \
    .load('/Users/saifjumaa/Desktop/data.csv')

models_dataframe.show()