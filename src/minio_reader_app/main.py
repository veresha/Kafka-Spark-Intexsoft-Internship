from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import window, avg, max, min, count
import config as c


conf = SparkConf().setAll(c.minio_conf_lict)

spark = (SparkSession
         .builder
         .appName('minio-reader-app')
         .config("spark.jars", c.CLICKHOUSE_JARS)
         .config(conf=conf)
         .getOrCreate())

df_from_minio = (spark
                 .read
                 .option("interSchema", "true")
                 .option('subscribe', c.MINIO_BUCKET_NAME)
                 .parquet(c.MINIO_BUCKET_PATH))

df_from_minio.registerTempTable("quotes")
df = spark.sql("select * from quotes").orderBy('date')


w = df.groupBy("company", window("date", "5 days")).agg(
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
 .option('driver', c.CLICKHOUSE_DRIVER)
 .option('url', c.CLICKHOUSE_URL)
 .option('user', 'default')
 .option('password', '')
 .option('dbtable', f'{c.CLICKHOUSE_DB_NAME}.{c.CLICKHOUSE_TABLE_NAME}')
 .save())
