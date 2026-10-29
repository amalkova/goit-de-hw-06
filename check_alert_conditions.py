from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("HW06_Alert_Conditions")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

conditions = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("alerts_conditions.csv")
)

print("\n=== ALERT CONDITIONS FROM CSV ===")
conditions.show(truncate=False)

sample = spark.createDataFrame(
    [(30.77, 58.8), (44.0, 82.0), (28.0, 35.0)],
    ["avg_temperature", "avg_humidity"]
)

joined = sample.crossJoin(conditions)

temp_matches = (
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
    .filter(temp_matches & humidity_matches)
    .select(
        "avg_temperature",
        "avg_humidity",
        "code",
        "message"
    )
)

print("\n=== MATCHED ALERTS ===")
alerts.show(truncate=False)

spark.stop()
