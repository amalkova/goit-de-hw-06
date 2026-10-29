import json
from kafka import KafkaConsumer

TOPIC = "anna_malkova_spark_alerts"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=15000,
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)

print(f"\n=== ALERTS RECEIVED FROM {TOPIC} ===\n")

count = 0

try:
    for message in consumer:
        print(f"Topic: {message.topic}")
        print(f"Partition: {message.partition}")
        print(f"Offset: {message.offset}")
        print(json.dumps(
            message.value,
            indent=2,
            ensure_ascii=False
        ))
        print("-" * 60)

        count += 1
        if count >= 10:
            break
finally:
    consumer.close()

print(f"\nTotal alerts read: {count}")
