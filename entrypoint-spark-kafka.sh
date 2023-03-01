#! ./bin/bash

while true ;do
sleep 3;

spark-submit --master spark://spark-kafka:7077 --total-executor-cores 1 /usr/src/main.py;

#spark-submit --master spark://spark-kafka:7077 /usr/src/main.py;

done