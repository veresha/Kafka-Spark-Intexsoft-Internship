import os

APP_NAME = 'minio-reader-app'

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio-root-user')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio-root-password')
MINIO_DATA_BUCKET_NAME = os.getenv('MINIO_DATA_BUCKET_NAME', 'stock-quotes')

CLICKHOUSE_JARS = os.getenv('CLICKHOUSE_JARS', '/usr/src/clickhouse-jdbc-0.3.2-patch11-all.jar')
CLICKHOUSE_DRIVER = os.getenv('CLICKHOUSE_DRIVER', 'com.clickhouse.jdbc.ClickHouseDriver')
CLICKHOUSE_URL = os.getenv('CLICKHOUSE_URL', 'jdbc:clickhouse://clickhouse:8123')
CLICKHOUSE_DB_NAME = os.getenv('CLICKHOUSE_DB_NAME', 'quotesDB')
CLICKHOUSE_TABLE_NAME = os.getenv('CLICKHOUSE_TABLE_NAME', 'quotes')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASS = os.getenv('CLICKHOUSE_PASS', '')

AGGREGATION_PERIOD = os.getenv('AGGREGATION_PERIOD', '5 days')

minio_conf_lict = [
    ("fs.s3a.endpoint", MINIO_ENDPOINT),
    ("fs.s3a.access.key", MINIO_ACCESS_KEY),
    ("fs.s3a.secret.key", MINIO_SECRET_KEY),
    ("fs.s3a.path.style.access", "true"),
    ("spark.jars", CLICKHOUSE_JARS)]
