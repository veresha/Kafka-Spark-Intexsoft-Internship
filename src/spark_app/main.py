from pyspark.sql import SparkSession


spark = (SparkSession
         .builder
         .appName('kafka-spark-app')
         .getOrCreate())

# spark.sparkContext.setLogLevel('WARN')


source = (spark
          .readStream
          .format('kafka')
          .option('kafka.bootstrap.servers', 'kafka:9092')
          .option('subscribe', 'quotes')
          .load()
          .selectExpr("CAST(value AS STRING)"))

query = source.writeStream.format("console")\
    .option("truncate", "false") \
    .outputMode("append")\
    .start() \
    .awaitTermination()


# source.printSchema()
