from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HW06_Kafka_Connection_Test")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4",
    )
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "anna_malkova_building_sensors")
    .option("startingOffsets", "earliest")
    .load()
)

print("\n=== SPARK + KAFKA CONNECTION: OK ===\n")

df.selectExpr(
    "CAST(value AS STRING) AS sensor_data"
).show(5, truncate=False)

spark.stop()
