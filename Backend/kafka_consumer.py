from flask import Flask, jsonify
from confluent_kafka import Consumer
import json
import threading
import os 
# Initialize Flask app
# app = Flask(__name__)
# global messages
# messages=[]
# Kafka consumer configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'iB360_access_records'
output_file = r'C:\Users\tharu\OneDrive\Desktop\ibridge_data\frontend\public\messages.json'

# Create or truncate the file
# if not os.path.exists("public"):
#     os.makedirs("public")
with open(output_file, "w") as f:
    json.dump([], f)

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
            # messages.append(message_value)
            with open(output_file, "r+") as f:
                data = json.load(f)
                data.append(message_value)
                f.seek(0)
                json.dump(data, f, indent=4)
            print(f"Consumed message: {message_value}")

        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON message: {e}")



# Start Kafka consumer in a separate thread
consumer_thread = threading.Thread(target=consume_messages, daemon=True)
consumer_thread.start()

# Run the Flask app
# if __name__ == '__main__':
#     consume_messages.run(debug=True)
consume_messages()
