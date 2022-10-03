# Pinterest-Data-Processing-Pipeline
Pinterest has world-class machine learning engineering systems. They have billions of user interactions such as image uploads or image clicks which they need to process every day to inform the decisions to make. 

In this project, I built the system in the cloud that takes in those events and runs them through two separate pipelines. One for computing real-time metrics (like profile popularity, which would be used to recommend that profile in real-time), and another for computing metrics that depend on historical data (such as the most popular category this year). 

UML Diagram: https://i.ibb.co/pXQsmFK/projext-3.jpg

# Data Ingestion: Apache Kafka
- The data source is an infinite posting loop, which simulates a users activity on-site and posts the JSONs to a localhost port.
- API/project_pin_API.py contains an API to receive the user posts, it also sets up the kafka producer to add the posts to the topic.
- batch_consumer.py creates the Kafka consumer, and passes all messages it finds in the topic to an s3 bucket.

# Batch Processing: Apache Spark, Apache Cassandra, Presto
- A spark session reads the data from the s3 bucket, that data is stored in a spark dataframe. With some initial batch cleaning such as:
    - Converting follower count into a real number, e.g. 10k into 10,000.
    - Dropping colomns with information that isn't useful.
    - Converting strings into integers where neccasary.
- Cassandra was setup and configured locally ready to receive the data from Spark for long term storage.
- Cassandra is then inegrated with Presto. This allows us to run ad-hoc queries on the data we have so far.

# Batch Monitoring: Cassandra, Prometheus, Grafana
- A JMX exporter allowed me to connect Cassandra with prometheus. This was then visualized in a Grafana dashboard in order to minotor cassandras instance attributes.
- Grafana Dashboard: https://i.ibb.co/QFqZhzS/Batch-Processing-Monitoring.png

# Streaming: Kafka, Apache Spark Streaming
- streaming_consumer.py was created to stream the data from Kafka after some data cleaning.

# Storage: Postgres
- I improved the Spark code so that it puts the processed data into the local Postgres database.
- To do this I used Postgres, as it's open source and can be easily queried using pgAdmin4 or Presto.
Data in Postgres: https://i.ibb.co/ZLtBx96/database.png

# Monitoring: Prometheus, Grafana
- Using postgres metrics exported, I connected postgres to prometheus and exported those metrics to Grafana for monitoring.