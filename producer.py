from kafka import KafkaProducer
import json
import time

TOPIC = "user_topic"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open("data.json", "r") as f:
    data = json.load(f)

producer.send(TOPIC, value=data)
print(f"Sent: {data}")
time.sleep(1)

producer.flush()
print("✅ All data sent to Kafka")