import json

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_json, col, get_json_object
from pyspark.sql.types import StructType, StructField, StringType, FloatType


minio_conf_lict = [
    ("fs.s3a.endpoint", 'http://minio:9000'),
    ("fs.s3a.access.key", 'minio-root-user'),
    ("fs.s3a.secret.key", 'minio-root-password'),
    ("fs.s3a.path.style.access", "true")]

manual_schema = StructType([
    StructField('company', StringType(), True),
    StructField('quote', FloatType(), True),
    StructField('date', StringType(), True)])

conf = SparkConf().setAll(minio_conf_lict)

spark = (SparkSession
         .builder
         .appName('kafka-reader-app')
         .config(conf=conf)
         .getOrCreate())

sc = spark.sparkContext
sc.setLogLevel("WARN")

# schema = spark.read.json(map(lambda x: json.loads(x), ))

kafka_data = (spark.read
              .format('kafka')
              .option('kafka.bootstrap.servers', 'kafka:9092')
              .option('subscribe', 'quotes')
              .load()
              .select(col('value').cast("string")).alias('schema').select(col('schema.*')).first())

kafka_dict = kafka_data.asDict().get('value')
schema = spark.read.json(sc.parallelize([kafka_dict])).schema

df_from_kafka = (spark
                 .readStream
                 .format('kafka')
                 .option('kafka.bootstrap.servers', 'kafka:9092')
                 .option('subscribe', 'quotes')
                 .load()
                 .select(from_json(col("value").cast("string"), schema=schema)
                         .alias("parsed_value"))
                 .select(col("parsed_value.*")))

# df_from_kafka.writeStream.format("console").start().awaitTermination()

(df_from_kafka
 .writeStream
 .format("parquet")
 .option("maxPartitionBytes", 256 * 1024 * 1024)
 .option("path", f"s3a://stock-quotes/data")
 .option("checkpointLocation", f"s3a://stock-quotes/data/checkpoints")
 .partitionBy('date')
 .start()
 .awaitTermination())



