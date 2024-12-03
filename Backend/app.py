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

# Variable to store the latest messages
message_store = []  # List to store multiple messages
MAX_STORE_SIZE = 50  # Limit the number of stored messages

# Consumer thread function
def consume_messages():
    global message_store
    while True:
        msg = consumer.poll(1.0)  # Poll every 1 second
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            message_value = json.loads(msg.value().decode('utf-8'))
            message_store.append(message_value)  # Add message to store
            # Limit the number of stored messages
            if len(message_store) > MAX_STORE_SIZE:
                message_store.pop(0)
            print(f"Consumed message: {message_value}")

        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON message: {e}")

# API endpoint to get the latest messages
@app.route('/api/messages', methods=['GET'])
def get_messages():
    return jsonify(message_store)

# Start Kafka consumer in a separate thread
consumer_thread = threading.Thread(target=consume_messages, daemon=True)
consumer_thread.start()

# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)
