import pyspark.sql.functions as f
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

import config as c


def get_schema(spark_session: SparkSession) -> StructType:
    sc = spark_session.sparkContext
    sc.setLogLevel("WARN")
    kafka_data = (spark_session.read
                  .format('kafka')
                  .option('kafka.bootstrap.servers', c.KAFKA_SERVER)
                  .option('subscribe', c.KAFKA_TOPIC)
                  .load()
                  .select(f.col('value').cast("string")).alias('schema')
                  .select(f.col('schema.*')).first())
    kafka_dict = kafka_data.asDict().get('value')
    schema = spark_session.read.json(sc.parallelize([kafka_dict])).schema
    return schema


def get_spark() -> SparkSession:
    conf = SparkConf().setAll(c.minio_conf_lict)
    spark = (SparkSession
             .builder
             .appName(c.APP_NAME)
             .config(conf=conf)
             .getOrCreate())
    return spark


def get_df_from_kafka() -> DataFrame:
    return (get_spark()
            .readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', c.KAFKA_SERVER)
            .option('subscribe', c.KAFKA_TOPIC)
            .load()
            .select(f.from_json(f.col("value").cast("string"), schema=get_schema(get_spark()))
                    .alias("parsed_value"))
            .select(f.col("parsed_value.*")))


(get_df_from_kafka()
 .writeStream
 .format("parquet")
 .option("maxPartitionBytes", 256 * 1024 * 1024)
 .option("path", c.MINIO_BUCKET_PATH)
 .option("checkpointLocation", f"{c.MINIO_BUCKET_PATH}/checkpoints")
 .partitionBy(['company', 'date'])
 .start()
 .awaitTermination())
