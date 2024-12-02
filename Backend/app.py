from flask import Flask, jsonify, render_template
from confluent_kafka import Consumer
import json
import threading
import time

# Initialize Flask app
app = Flask(__name__)

# Kafka consumer configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'iB360_access_records'

consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'consumer-group-1',
    'auto.offset.reset': 'earliest'  # Start consuming from the earliest message
}

consumer = Consumer(consumer_config)
consumer.subscribe([TOPIC_NAME])

# Variable to store the latest messages
latest_message = {}

# Consumer thread function
def consume_messages():
    global latest_message
    while True:
        msg = consumer.poll(1.0)  # Poll every 1 second
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            message_value = json.loads(msg.value().decode('utf-8'))
            latest_message = message_value  # Update the latest message
            print(f"Consumed message: {latest_message}")

        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON message: {e}")

# Start Kafka consumer in a separate thread
consumer_thread = threading.Thread(target=consume_messages)
consumer_thread.daemon = True
consumer_thread.start()

@app.route('/')
def index():
    # Render a template for the frontend (React)
    return render_template('index.html')

@app.route('/api/latest_message')
def get_latest_message():
    # Return the latest message as a JSON response
    return jsonify(latest_message)

if __name__ == '__main__':
    app.run(debug=True)
