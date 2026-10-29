from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType
)

KAFKA_SERVER = "localhost:9092"
INPUT_TOPIC = "anna_malkova_building_sensors"
OUTPUT_TOPIC = "anna_malkova_spark_alerts"

# Create output topic if needed
admin = KafkaAdminClient(
    bootstrap_servers=KAFKA_SERVER,
    client_id="hw06_admin"
)
try:
    if OUTPUT_TOPIC not in admin.list_topics():
        admin.create_topics([
            NewTopic(
                name=OUTPUT_TOPIC,
                num_partitions=1,
                replication_factor=1
            )
        ])
    print(f"Output topic ready: {OUTPUT_TOPIC}", flush=True)
finally:
    admin.close()

spark = (
    SparkSession.builder
    .appName("HW06_Spark_Streaming_Alerts")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4"
    )
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

sensor_schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType())
])

conditions_schema = StructType([
    StructField("id", IntegerType()),
    StructField("humidity_min", DoubleType()),
    StructField("humidity_max", DoubleType()),
    StructField("temperature_min", DoubleType()),
    StructField("temperature_max", DoubleType()),
    StructField("code", IntegerType()),
    StructField("message", StringType())
])

conditions = (
    spark.read
    .option("header", True)
    .schema(conditions_schema)
    .csv("alerts_conditions.csv")
)

print("\n=== ALERT CONDITIONS ===", flush=True)
conditions.show(truncate=False)

source = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

measurements = (
    source
    .select(
        F.from_json(
            F.col("value").cast("string"),
            sensor_schema
        ).alias("data")
    )
    .select(
        F.to_timestamp("data.timestamp").alias("event_time"),
        F.col("data.temperature").alias("temperature"),
        F.col("data.humidity").alias("humidity")
    )
    .filter(F.col("event_time").isNotNull())
)

aggregated = (
    measurements
    .withWatermark("event_time", "10 seconds")
    .groupBy(
        F.window("event_time", "1 minute", "30 seconds")
    )
    .agg(
        F.round(F.avg("temperature"), 2).alias("avg_temperature"),
        F.round(F.avg("humidity"), 2).alias("avg_humidity"),
        F.count("*").alias("measurements_count")
    )
)


def process_batch(batch_df, batch_id):
    # Cross join the aggregated readings with alert conditions
    joined = batch_df.crossJoin(F.broadcast(conditions))

    temperature_matches = (
        (
            (F.col("temperature_min") == -999)
            & (F.col("temperature_max") == -999)
        )
        | F.col("avg_temperature").between(
            F.col("temperature_min"),
            F.col("temperature_max")
        )
    )

    humidity_matches = (
        (
            (F.col("humidity_min") == -999)
            & (F.col("humidity_max") == -999)
        )
        | F.col("avg_humidity").between(
            F.col("humidity_min"),
            F.col("humidity_max")
        )
    )

    alerts = (
        joined
        .filter(temperature_matches & humidity_matches)
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "avg_temperature",
            "avg_humidity",
            "measurements_count",
            "code",
            "message"
        )
        .cache()
    )

    try:
        count = alerts.count()
        print(f"\n=== BATCH {batch_id}: {count} ALERTS ===", flush=True)

        if count == 0:
            return

        alerts.show(truncate=False)

        kafka_output = alerts.select(
            F.col("code").cast("string").alias("key"),
            F.to_json(
                F.struct(
                    "window_start",
                    "window_end",
                    "avg_temperature",
                    "avg_humidity",
                    "measurements_count",
                    "code",
                    "message"
                )
            ).alias("value")
        )

        kafka_output.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_SERVER) \
            .option("topic", OUTPUT_TOPIC) \
            .save()

        print(
            f"Sent {count} alerts to {OUTPUT_TOPIC}",
            flush=True
        )
    finally:
        alerts.unpersist()


query = (
    aggregated.writeStream
    .outputMode("update")
    .foreachBatch(process_batch)
    .option("checkpointLocation", ".checkpoints/hw06_alerts")
    .trigger(processingTime="10 seconds")
    .start()
)

print("\n=== SPARK STREAMING STARTED ===", flush=True)
print(f"Input:  {INPUT_TOPIC}", flush=True)
print(f"Output: {OUTPUT_TOPIC}", flush=True)

try:
    query.awaitTermination()
finally:
    spark.stop()
