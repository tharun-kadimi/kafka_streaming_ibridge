from flask import Flask, jsonify
from confluent_kafka import Consumer
import json
import threading

# Initialize Flask app
app = Flask(__name__)

# Kafka consumer configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'iB360_access_records'

consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'consumer-group-1',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe([TOPIC_NAME])

# Consumer thread function
def consume_messages():
    while True:
        msg = consumer.poll(1.0)  # Poll every 1 second
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            message_value = json.loads(msg.value().decode('utf-8'))
            print(f"Consumed message: {message_value}")

        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON message: {e}")



# Start Kafka consumer in a separate thread
consumer_thread = threading.Thread(target=consume_messages, daemon=True)
consumer_thread.start()

# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)
