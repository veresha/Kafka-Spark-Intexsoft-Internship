import os


APP_NAME = os.getenv('APP_NAME', 'kafka-spark-minio_reader_app')

KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'kafka:9092')

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio-root-user')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio-root-password')
MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME', 'stock-quotes')

