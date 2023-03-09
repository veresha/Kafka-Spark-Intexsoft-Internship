import pyspark.sql.functions as f
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, DataFrame

import config as c


def get_spark() -> SparkSession:
    conf = SparkConf().setAll(c.minio_conf_lict)

    return (SparkSession
            .builder
            .appName(c.APP_NAME)
            .config("spark.jars", c.CLICKHOUSE_JARS)
            .config(conf=conf)
            .getOrCreate())


def get_df_from_minio() -> DataFrame:
    return (get_spark()
            .read
            .option("interSchema", "true")
            .option('subscribe', c.MINIO_DATA_BUCKET_NAME)
            .parquet(f's3a://{c.MINIO_DATA_BUCKET_NAME}'))


def aggregate_data() -> DataFrame:
    get_df_from_minio().registerTempTable("quotes")
    df = get_spark().sql("select * from quotes").orderBy('date')

    w = df.groupBy("company", f.window("date", c.AGGREGATION_PERIOD)).agg(
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


def push_data_to_clickhouse():
    (aggregate_data().write
     .format("jdbc")
     .mode("append")
     .option('driver', c.CLICKHOUSE_DRIVER)
     .option('url', c.CLICKHOUSE_URL)
     .option('user', c.CLICKHOUSE_USER)
     .option('password', c.CLICKHOUSE_PASS)
     .option('dbtable', f'{c.CLICKHOUSE_DB_NAME}.{c.CLICKHOUSE_TABLE_NAME}')
     .save())
