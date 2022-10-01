from multiprocessing.resource_sharer import stop
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import when, col, regexp_replace
import os


from pyspark.sql import functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import split, col
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import IntegerType, Row
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import re

def run_s3_to_spark():

    # Adding the packages required to get data from S3  
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.1.0 pyspark-shell"

    # Creating our Spark configuration
    conf = SparkConf() \
        .setAppName('S3toSpark') \
        .setMaster('local[*]')

    sc=SparkContext(conf=conf)

    # Configure the setting to read from the S3 bucket
    accessKeyId=os.environ['AWS_ACCESS_KEY']
    secretAccessKey=os.environ['AWS_SECRET_KEY']
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set('fs.s3a.access.key', accessKeyId)
    hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
    hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

    # Create our Spark session
    spark=SparkSession(sc)

    # Read from the S3 bucket
    df = spark.read.json('s3a://pinterest-data-bucket/data/*.json') # You may want to change this to read csv depending on the files your reading from the bucket


    # spark session builder
    spark = SparkSession.builder \
        .appName("SparkToCassandra") \
            .getOrCreate()

    #Replace empty cells with null values
    df = df.replace({'User Info Error': None}, subset = ['follower_count']) \
            .replace({"No Title Data Available": None}, subset = ['title']) \
            .replace({'No description available Story format': None}, subset = ['description']) \
            .replace({'Image src error.': None}, subset = ['image_src'])

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

    # drop index
    df_new = df_new.drop('downloaded', 'index')

    df_new.printSchema()

    df_new.show()


    # Send to cassandra
#    df_new.write \
#        .format('org.apache.spark.sql.cassandra')\
#                .mode('append') \
#                .options(table='pinterest', keyspace='data') \
#                .option("spark.cassandra.connection.host", "127.0.0.1") \
#                .option("spark.cassandra.connection.port", "9042") \
#                .save()

    spark.stop()


run_s3_to_spark()