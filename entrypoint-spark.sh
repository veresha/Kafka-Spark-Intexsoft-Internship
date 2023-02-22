#! ./bin/spark-submit

spark-submit --master spark://spark-kafka:7077 /usr/src/main.py

spark-submit /usr/src/main.py

clickhouse-client -q "CREATE TABLE IF NOT EXISTS quotes.quote ENGINE=engine"

