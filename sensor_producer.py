
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "anna_malkova_building_sensors"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

sensor_id = uuid.uuid4().hex[:8]

print(f"Sensor started: {sensor_id}")
print(f"Sending data to: {TOPIC}")

try:
    while True:
        sensor_data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature": round(random.uniform(25, 45), 2),
            "humidity": round(random.uniform(15, 85), 2)
        }

        producer.send(TOPIC, value=sensor_data).get(timeout=10)

        print(f"Sent: {sensor_data}", flush=True)

        time.sleep(2)

except KeyboardInterrupt:
    print(f"\nSensor {sensor_id} stopped.")

finally:
    producer.flush()
    producer.close()