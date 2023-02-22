from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from datetime import datetime, timedelta

from df_processing import df_division_by_time, get_statistic_data, df_division_by_company

minio_conf_lict = [
    ("fs.s3a.endpoint", 'http://minio:9000'),
    ("fs.s3a.access.key", 'minio-root-user'),
    ("fs.s3a.secret.key", 'minio-root-password'),
    ("fs.s3a.path.style.access", "true")]

conf = SparkConf().setAll(minio_conf_lict)

spark = (SparkSession
         .builder
         .appName('minio-reader-app')
         .config("spark.jars", "/usr/src/clickhouse-jdbc-0.3.2-patch11-all.jar")
         .config(conf=conf)
         .getOrCreate())

df_from_minio = (spark
                 .read
                 .option("interSchema", "true")
                 .option('subscribe', 'quotes')
                 .parquet("s3a://stock-quotes/data"))

df_from_minio.registerTempTable("quotes")
df = spark.sql("select * from quotes").orderBy('date')
print(df.show(truncate=False))

companies_dfs_list = df_division_by_company(df)

interval = 6
for df in companies_dfs_list:
    dfs_list = df_division_by_time(df, interval)

    for df2 in dfs_list:
        df2.show()
        statistic_data = get_statistic_data(df2)
        statistic_data.show()
        # then write to clickhouse



# (df2.write
#  .format("jdbc")
#  .mode("append")
#  .option('driver', 'com.clickhouse.jdbc.ClickHouseDriver')
#  .option('url', 'jdbc:clickhouse://clickhouse:8123')
#  .option('user', 'default')
#  .option('password', '')
#  .option('dbtable', 'quotes')
#  .save())
