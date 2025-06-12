from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

with open("/app/sample_data.jsonl", "r") as f:
    for line in f:
        record = json.loads(line)
        producer.send("raw_telecom_data", record)
        time.sleep(0.2)  # Simulate stream
