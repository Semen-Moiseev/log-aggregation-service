import json
import os
from time import sleep
from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
TOPIC = "logs"

es = Elasticsearch(ELASTICSEARCH_HOST)

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'log-collector-group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([TOPIC])

print("Collector started, waiting for messages...")

while True:
	msg = consumer.poll(1.0)
	if msg is None:
		continue
	if msg.error():
		if msg.error().code() != KafkaError._PARTITION_EOF:
			print(f"Kafka error: {msg.error()}")
			continue

	try:
		log = json.loads(msg.value().decode("utf-8"))
		es.index(index="logs", document=log)
		print(f"Saved log: {log}")
	except Exception as e:
		print(f"Error processing message: {e}")