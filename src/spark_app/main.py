from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


conf_lict = [
    ("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"),
    ("fs.s3a.endpoint", "minio:9000"),
    ("fs.s3a.access.key", "minio-root-user"),
    ("fs.s3a.secret.key", "minio-root-password"),
    ("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
    ("fs.s3a.path.style.access", "true"),
    ("com.amazonaws.services.s3.enableV4", "true")]

spark = (SparkSession
         .builder
         .appName('kafka-spark-app')
         .config(conf=SparkConf().setAll(conf_lict))
         .getOrCreate())

# spark.sparkContext.setLogLevel("WARN")

df = (spark
      .readStream
      .format('kafka')
      .option('kafka.bootstrap.servers', 'kafka:9092')
      .option('subscribe', 'quotes')
      .load())

# query = (df
#          .writeStream
#          .format("parquet")
#          .option("checkpointLocation", "/usr/checkpoints")
#          .option("path", "/usr/src/data")
#          .selectExpr("CAST(value AS STRING)")
#          .start())

# query.awaitTermination()

(df
 .writeStream
 .format("parquet")
 .option("maxPartitionBytes", 256 * 1024 * 1024)
 .option("path", f"s3a://stock-quotes/data/test")
 .start()
 .awaitTermination())


