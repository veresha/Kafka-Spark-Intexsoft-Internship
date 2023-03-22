import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from spark_instance import SparkInstance

import config as c


class SparkWorker:

    def __init__(self):
        self.spark = SparkInstance(app_name=c.APP_NAME, conf=c.minio_conf_lict)

    def get_df_from_minio(self) -> DataFrame:
        return (self.spark.session
                .read
                .option("interSchema", "true")
                .option('subscribe', c.MINIO_DATA_BUCKET_NAME)
                .parquet(f's3a://{c.MINIO_DATA_BUCKET_NAME}/data'))

    def aggregate_data(self) -> DataFrame:
        w = self.get_df_from_minio().groupBy("company", f.window("date", c.AGGREGATION_PERIOD)).agg(
            f.avg("quote").alias("avg"),
            f.max("quote").alias("max"),
            f.min("quote").alias("min"),
            f.count("quote").alias("records_count"))

        df = (w.select('company',
                       w.window.start.cast('string').alias('period_start'),
                       "records_count",
                       "avg",
                       "max",
                       "min")
              .orderBy('company', 'period_start'))

        df.show(truncate=False)
        return df

    def push_data_to_clickhouse(self):
        (self.aggregate_data().write
         .format("jdbc")
         .mode("append")
         .option('driver', c.CLICKHOUSE_DRIVER)
         .option('url', c.CLICKHOUSE_URL)
         .option('user', c.CLICKHOUSE_USER)
         .option('password', c.CLICKHOUSE_PASS)
         .option('dbtable', f'{c.CLICKHOUSE_DB_NAME}.{c.CLICKHOUSE_TABLE_NAME}')
         .save())


spark = SparkWorker()
spark.push_data_to_clickhouse()


