from optparse import Option
from kafka import KafkaConsumer
from json import loads
import json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark
import os
from pyspark.sql.functions import when, col, regexp_replace
from pyspark.sql import functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import split, col
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import IntegerType, Row
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import re


# streaming_consumer = KafkaConsumer('MyFirstKafkaTopic', bootstrap_servers = "localhost:9092")
kafkatopic = "MyFirstKafkaTopic"
bootstrap_server = "localhost:9092"

#for data in streaming_consumer:
#    print(data)

# Adding the packages required to get data from S3  
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.2.10 pyspark-shell'

# Creating our Spark configuration
conf = SparkConf() \
        .setAppName('S3toSpark') \
        .setMaster('local[*]')

sc=SparkContext(conf=conf)

# create spark session
spark=SparkSession(sc)

spark = SparkSession.builder \
    .appName("streaming") \
        .getOrCreate()

df = spark \
    .readStream \
    .format("Kafka") \
    .option("kafka.bootstrap.servers", bootstrap_server) \
    .option("subscribe", kafkatopic) \
    .option("startingOffsets", "latest") \
    .load()

# Select the value part of the kafka message and cast it to a string.
df = df.selectExpr("CAST(value as STRING)", "timestamp as timestamp")

jsonSchema = StructType([StructField("category", StringType()),
            StructField("index", IntegerType()),
            StructField("unique_id", StringType()),
            StructField("title", StringType()),
            StructField("description", StringType()),
            StructField("follower_count", StringType()),
            StructField("tag_list", StringType()),
            StructField("is_image_or_video", StringType()),
            StructField("image_src", StringType()),
            StructField("downloaded", IntegerType()),
            StructField("save_location", StringType())])

# Convert JSON to multiple columns
df = df.withColumn("value", F.from_json(df["value"], jsonSchema)).select(col("value.*"), "timestamp")


# change tag list list type
df_new = df.withColumn("tag_list", split(df.tag_list, ","))

    # Change follow count into full number
df_new = df_new.withColumn('follower_count', regexp_replace('follower_count', 'k', '000'))
df_new = df_new.withColumn('follower_count', regexp_replace('follower_count', 'M', '000000'))
df_new = df_new.withColumn('follower_count', regexp_replace('follower_count', 'B', '000000000'))

    # Remove text Local Save in from Save Location
df_new = df_new.withColumn('save_location', regexp_replace('save_location', 'Local save in ', ''))

    # Change downloaded, follower count & index into integer
df_new = df_new.withColumn('downloaded', df_new["downloaded"].cast("int"))
df_new = df_new.withColumn('follower_count', df_new["follower_count"].cast("int"))
df_new = df_new.withColumn('index', df_new["index"].cast("int"))

# df_new.writeStream.outputMode("append").format("console").start().awaitTermination()

def wrtie_to_database(df_2, epoch_id):

    df_2.write \
        .mode('append') \
        .format('jdbc') \
        .option('url', f'jdbc:postgresql://localhost:5432/pinterest') \
        .option('user', 'saifjumaa') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'public.pin_data') \
        .save()


df_new.writeStream \
    .foreachBatch(wrtie_to_database) \
    .start() \
    .awaitTermination()