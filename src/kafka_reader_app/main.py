from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_json, col
from config import minio_conf_lict, KAFKA_SERVER, MINIO_BUCKET_PATH, KAFKA_TOPIC

conf = SparkConf().setAll(minio_conf_lict)

spark = (SparkSession
         .builder
         .appName('kafka-reader-app')
         .config(conf=conf)
         .getOrCreate())

sc = spark.sparkContext
sc.setLogLevel("WARN")

kafka_data = (spark.read
              .format('kafka')
              .option('kafka.bootstrap.servers', KAFKA_SERVER)
              .option('subscribe', KAFKA_TOPIC)
              .load()
              .select(col('value').cast("string")).alias('schema').select(col('schema.*')).first())


kafka_dict = kafka_data.asDict().get('value')
schema = spark.read.json(sc.parallelize([kafka_dict])).schema

df_from_kafka = (spark
                 .readStream
                 .format('kafka')
                 .option('kafka.bootstrap.servers', KAFKA_SERVER)
                 .option('subscribe', KAFKA_TOPIC)
                 .load()
                 .select(from_json(col("value").cast("string"), schema=schema)
                         .alias("parsed_value"))
                 .select(col("parsed_value.*")))

# df_from_kafka.writeStream.format("console").start().awaitTermination()

(df_from_kafka
 .writeStream
 .format("parquet")
 .option("maxPartitionBytes", 256 * 1024 * 1024)
 .option("path", MINIO_BUCKET_PATH)
 .option("checkpointLocation", f"{MINIO_BUCKET_PATH}/checkpoints")
 .partitionBy(['company', 'date'])
 .start()
 .awaitTermination())



