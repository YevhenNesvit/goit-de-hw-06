import pandas as pd
from pyspark.sql.functions import lit
from data_aggregation import aggregated_data, spark
from pyspark.sql.functions import col
from data_aggregation import windowed_data
from configs import kafka_config

# Завантаження CSV-файлу
alert_conditions = pd.read_csv("alerts_conditions.csv")

alert_conditions_spark_df = spark.createDataFrame(alert_conditions)

# Виконання cross join між потоком даних та умовами алертів
combined_df = windowed_data.crossJoin(alert_conditions_spark_df)

# Фільтруємо дані, щоб перевірити, чи підпадають вони під умови алерту
alerts_df = combined_df.filter(
    (col("h_avg").between(col("humidity_min"), col("humidity_max"))) |
    (col("t_avg").between(col("temperature_min"), col("temperature_max")))
)

# Вибірка колонок для результатів
alerts_df = alerts_df.select(
    "sensor_id",
    "timestamp",
    "t_avg",
    "h_avg",
    "code",
    "message"
)

alerts_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

alerts_df \
    .selectExpr("CAST(sensor_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{kafka_config["bootstrap_servers"]}") \
    .option("topic", "alerts_nesvit") \
    .start() \
    .awaitTermination()
