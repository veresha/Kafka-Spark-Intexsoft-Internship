start:
	docker compose up -d --build;

	sleep 5;
	docker exec -it spark-kafka bash spark-submit --master spark://spark-kafka:7077 /usr/src/main.py
