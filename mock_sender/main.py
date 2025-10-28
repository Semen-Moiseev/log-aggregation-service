import json
import time
import os
from faker import Faker
from confluent_kafka.admin import AdminClient
from confluent_kafka import Producer, KafkaException

fake = Faker()
topic = "logs"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

while True:
	try:
		admin = AdminClient({'bootstrap.servers': KAFKA_BROKER})
		admin.list_topics(timeout=5)
		print("Connected to Kafka!")
		break
	except KafkaException:
		print("Waiting for Kafka...")
		time.sleep(3)

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def generate_log():
	return {
		"service": fake.random_element(["auth", "payments", "orders", "api"]),
		"level": fake.random_element(["INFO", "WARNING", "ERROR"]),
		"timestamp": fake.iso8601(),
		"message": fake.sentence()
	}

if __name__ == "__main__":
	print("Mock log sender started...")
	while True:
		log = generate_log()
		producer.produce(topic, json.dumps(log).encode("utf-8"))
		producer.flush()
		print(f"Sent log: {log}")
		time.sleep(2)