from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "semiconductor-events",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for msg in consumer:
    print(msg.value)