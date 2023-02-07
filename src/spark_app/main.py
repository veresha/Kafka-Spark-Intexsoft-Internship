from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


conf_lict = [
    ("fs.s3a.endpoint", "http://minio:9000"),
    ("fs.s3a.access.key", "minio-root-user"),
    ("fs.s3a.secret.key", "minio-root-password"),
    ("fs.s3a.path.style.access", "true")]

spark = (SparkSession
         .builder
         .appName('kafka-spark-app')
         .config(conf=SparkConf().setAll(conf_lict))
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

df = (spark
      .readStream
      .format('kafka')
      .option('kafka.bootstrap.servers', 'kafka:9092')
      .option('subscribe', 'quotes')
      .load())

(df
 .writeStream
 .format("parquet")
 .option("maxPartitionBytes", 256 * 1024 * 1024)
 .option("path", "s3a://stock-quotes/data")
 .option("checkpointLocation", "s3a://stock-quotes/data/checkpoints")
 .start()
 .awaitTermination())


