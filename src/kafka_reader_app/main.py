import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
import config as c
from spark_session import SparkInstance


class SparkWorker:

    def __init__(self):
        self.spark = SparkInstance(app_name=c.APP_NAME, conf=c.minio_conf_lict)

    def get_schema(self) -> StructType:
        kafka_data = (self.spark.session.read
                      .format('kafka')
                      .option('kafka.bootstrap.servers', c.KAFKA_SERVER)
                      .option('subscribe', c.KAFKA_TOPIC)
                      .load()
                      .select(f.col('value').cast("string")).alias('schema')
                      .select(f.col('schema.*')).first())
        kafka_dict = kafka_data.asDict().get('value')
        return self.spark.session.read.json(self.spark.context.parallelize([kafka_dict])).schema

    def get_df_from_kafka(self) -> DataFrame:
        return (self.spark.session
                .readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', c.KAFKA_SERVER)
                .option('subscribe', c.KAFKA_TOPIC)
                .load()
                .select(f.from_json(f.col("value").cast("string"), schema=self.get_schema())
                        .alias("parsed_value"))
                .select(f.col("parsed_value.*")))


def push_data_to_minio(df):
    (df
     .writeStream
     .format("parquet")
     .option("maxPartitionBytes", 256 * 1024 * 1024)
     .option("path", f's3a://{c.MINIO_DATA_BUCKET_NAME}/data')
     .option("checkpointLocation", f's3a://{c.MINIO_CHECKPOINTS_BUCKET_NAME}/checkpoints')
     .partitionBy(['company', 'date'])
     .start()
     .awaitTermination())


spark = SparkWorker()
df_from_kafka = spark.get_df_from_kafka()
push_data_to_minio(df_from_kafka)
