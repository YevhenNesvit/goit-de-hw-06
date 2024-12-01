from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, window, col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from configs import kafka_config

# Створення SparkSession
spark = SparkSession.builder.appName("IoT_Alert_Monitoring").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0").getOrCreate()

# Читання потоку даних з Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{kafka_config["bootstrap_servers"]}") \
    .option("subscribe", "building_sensors_nesvit") \
    .load()

# Розпаковка даних з Kafka
sensor_data = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
sensor_data_parsed = sensor_data.selectExpr("json_tuple(value, 'sensor_id', 'timestamp', 'temperature', 'humidity') AS (sensor_id, timestamp, temperature, humidity)")

# Перетворення стовпця timestamp на тип TIMESTAMP
sensor_data_parsed = sensor_data_parsed.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

# Агрегація за допомогою Sliding Window (1 хвилина, інтервал 30 секунд)
windowed_data = sensor_data_parsed \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        "sensor_id"
    ) \
    .agg(
        avg("temperature").alias("t_avg"),
        avg("humidity").alias("h_avg")
    )

windowed_data.writeStream \
    .outputMode("update") \
    .format("console") \
    .start() \
    .awaitTermination()
