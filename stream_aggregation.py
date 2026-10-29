from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

KAFKA_SERVER = "localhost:9092"
INPUT_TOPIC = "anna_malkova_building_sensors"

spark = (
    SparkSession.builder
    .appName("HW06_Sensor_Aggregation")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4",
    )
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
])

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

sensors = (
    kafka_stream
    .select(
        F.from_json(
            F.col("value").cast("string"), schema
        ).alias("data")
    )
    .select(
        F.col("data.sensor_id").alias("sensor_id"),
        F.to_timestamp(
            F.col("data.timestamp")
        ).alias("event_time"),
        F.col("data.temperature").alias("temperature"),
        F.col("data.humidity").alias("humidity"),
    )
    .filter(F.col("event_time").isNotNull())
)

aggregated = (
    sensors
    .withWatermark("event_time", "10 seconds")
    .groupBy(
        F.window("event_time", "1 minute", "30 seconds")
    )
    .agg(
        F.round(
            F.avg("temperature"), 2
        ).alias("avg_temperature"),
        F.round(
            F.avg("humidity"), 2
        ).alias("avg_humidity"),
        F.count("*").alias("measurements_count"),
    )
)

print("\n=== HW06 SPARK STREAMING AGGREGATION ===")
print("Window: 1 minute")
print("Sliding interval: 30 seconds")
print("Watermark: 10 seconds\n")

query = (
    aggregated.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 20)
    .trigger(processingTime="10 seconds")
    .start()
)

try:
    query.awaitTermination()
finally:
    spark.stop()
