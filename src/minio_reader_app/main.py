from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


minio_conf_lict = [
    ("fs.s3a.endpoint", 'http://minio:9000'),
    ("fs.s3a.access.key", 'minio-root-user'),
    ("fs.s3a.secret.key", 'minio-root-password'),
    ("fs.s3a.path.style.access", "true")]

conf = SparkConf().setAll(minio_conf_lict)

spark = (SparkSession
         .builder
         .appName('minio-reader-app')
         .config(conf=conf)
         .getOrCreate())

df_from_minio = (spark
                 .read
                 .option("header", "true")
                 .option("interSchema", "true")
                 .option('subscribe', 'quotes')
                 .parquet("s3a://stock-quotes/data")
                 .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)"))

df_from_minio.registerTempTable("quotes")
df2 = spark.sql("select * from quotes")

print(df2.show())

