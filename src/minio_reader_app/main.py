from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import window, avg, max, min, count
from config import minio_conf_lict, MINIO_BUCKET_NAME, MINIO_BUCKET_PATH, CLICKHOUSE_JARS, CLICKHOUSE_DRIVER, \
    CLICKHOUSE_URL, CLICKHOUSE_DB_NAME

conf = SparkConf().setAll(minio_conf_lict)

spark = (SparkSession
         .builder
         .appName('minio-reader-app')
         .config("spark.jars", CLICKHOUSE_JARS)
         .config(conf=conf)
         .getOrCreate())

df_from_minio = (spark
                 .read
                 .option("interSchema", "true")
                 .option('subscribe', MINIO_BUCKET_NAME)
                 .parquet(MINIO_BUCKET_PATH))

df_from_minio.registerTempTable("quotes")
df = spark.sql("select * from quotes").orderBy('date')


w = df.groupBy("company", window("date", "3 minutes")).agg(
    avg("quote").alias("avg"),
    max("quote").alias("max"),
    min("quote").alias("min"),
    count("quote").alias("records_count"))

df = (w.select('company',
               w.window.start.cast('string').alias('period_start'),
               "records_count",
               "avg",
               "max",
               "min")
      .orderBy('company', 'period_start'))

df.show(truncate=False)

(df.write
 .format("jdbc")
 .mode("append")
 .option('driver', CLICKHOUSE_DRIVER)
 .option('url', CLICKHOUSE_URL)
 .option('user', 'default')
 .option('password', '')
 .option('dbtable', CLICKHOUSE_DB_NAME)
 .save())
