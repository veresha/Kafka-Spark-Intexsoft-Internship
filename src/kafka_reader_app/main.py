from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from datetime import datetime
# from src.config import MINIO_ENDPOINT, MINIO_SECRET_KEY, MINIO_ACCESS_KEY, MINIO_BUCKET_NAME, KAFKA_SERVER, APP_NAME


minio_conf_lict = [
    ("fs.s3a.endpoint", 'http://minio:9000'),
    ("fs.s3a.access.key", 'minio-root-user'),
    ("fs.s3a.secret.key", 'minio-root-password'),
    ("fs.s3a.path.style.access", "true")]

conf = SparkConf().setAll(minio_conf_lict)

spark = (SparkSession
         .builder
         .appName('kafka-reader-app')
         .config(conf=conf)
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

df_from_kafka = (spark
                 .readStream
                 .format('kafka')
                 .option('kafka.bootstrap.servers', 'kafka:9092')
                 .option('subscribe', 'quotes')
                 .load())

(df_from_kafka
 .writeStream
 .format("parquet")
 .option("maxPartitionBytes", 256 * 1024 * 1024)
 .option("path", f"s3a://stock-quotes/data")
 .option("checkpointLocation", f"s3a://stock-quotes/data/checkpoints")
 .partitionBy("timestamp")
 .start()
 .awaitTermination())



