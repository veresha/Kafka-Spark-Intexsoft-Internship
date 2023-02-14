#! ./bin/spark-submit

spark-submit --master spark://spark-kafka:7077 /usr/src/kafka_reader_app/main.py

spark-submit --master spark://spark-minio:7077 /usr/src/minio_reader_app/main.py

