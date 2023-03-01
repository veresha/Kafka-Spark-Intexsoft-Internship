import os


KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'quotes')

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio-root-user')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio-root-password')
MINIO_BUCKET_PATH = os.getenv('MINIO_BUCKET_PATH', 's3a://stock-quotes/data')

minio_conf_lict = [
    ("fs.s3a.endpoint", MINIO_ENDPOINT),
    ("fs.s3a.access.key", MINIO_ACCESS_KEY),
    ("fs.s3a.secret.key", MINIO_SECRET_KEY),
    ("fs.s3a.path.style.access", "true")]
