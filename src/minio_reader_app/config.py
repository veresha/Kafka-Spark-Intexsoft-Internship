import os


KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'kafka:9092')

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio-root-user')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio-root-password')
MINIO_BUCKET_NAME = CLICKHOUSE_DB_NAME = os.getenv('MINIO_BUCKET_NAME', 'stock-quotes')
MINIO_BUCKET_PATH = os.getenv('MINIO_BUCKET_PATH', 's3a://stock-quotes/data')

CLICKHOUSE_JARS = os.getenv('CLICKHOUSE_JARS', '/usr/src/clickhouse-jdbc-0.3.2-patch11-all.jar')
CLICKHOUSE_DRIVER = os.getenv('CLICKHOUSE_DRIVER', 'com.clickhouse.jdbc.ClickHouseDriver')
CLICKHOUSE_URL = os.getenv('CLICKHOUSE_URL', 'jdbc:clickhouse://clickhouse:8123')

minio_conf_lict = [
    ("fs.s3a.endpoint", MINIO_ENDPOINT),
    ("fs.s3a.access.key", MINIO_ACCESS_KEY),
    ("fs.s3a.secret.key", MINIO_SECRET_KEY),
    ("fs.s3a.path.style.access", "true")]
