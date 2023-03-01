from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_json, col
import config as c


conf = SparkConf().setAll(c.minio_conf_lict)

spark = (SparkSession
         .builder
         .appName('kafka-reader-app')
         .config(conf=conf)
         .getOrCreate())

sc = spark.sparkContext
sc.setLogLevel("WARN")

kafka_data = (spark.read
              .format('kafka')
              .option('kafka.bootstrap.servers', c.KAFKA_SERVER)
              .option('subscribe', c.KAFKA_TOPIC)
              .load()
              .select(col('value').cast("string")).alias('schema').select(col('schema.*')).first())


kafka_dict = kafka_data.asDict().get('value')
schema = spark.read.json(sc.parallelize([kafka_dict])).schema

df_from_kafka = (spark
                 .readStream
                 .format('kafka')
                 .option('kafka.bootstrap.servers', c.KAFKA_SERVER)
                 .option('subscribe', c.KAFKA_TOPIC)
                 .load()
                 .select(from_json(col("value").cast("string"), schema=schema)
                         .alias("parsed_value"))
                 .select(col("parsed_value.*")))

(df_from_kafka
 .writeStream
 .format("parquet")
 .option("maxPartitionBytes", 256 * 1024 * 1024)
 .option("path", c.MINIO_BUCKET_PATH)
 .option("checkpointLocation", f"{c.MINIO_BUCKET_PATH}/checkpoints")
 .partitionBy(['company', 'date'])
 .start()
 .awaitTermination())



