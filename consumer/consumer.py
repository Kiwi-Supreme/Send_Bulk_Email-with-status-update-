from kafka import KafkaConsumer
import json
from dotenv import load_dotenv
load_dotenv()

consumer = KafkaConsumer(
    "BulkEmailSender",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Kafka consumer listening...")
