# Kafka-Spark-Intexsoft-Internship 
It's a second part of ETL-pipeline application.
First part: <https://github.com/veresha/Stock-Quotes-Intexsoft-Internship>
***
App include:
- **kafka_reader_app** - read Kafka topic, format data to Spark Dataframe, send data to Minio
- **minio_reader_app** - read minio bucket, make data aggregation, send aggregated data to Clickhouse
