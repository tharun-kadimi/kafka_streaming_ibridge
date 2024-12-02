from confluent_kafka import Consumer
import json

# Kafka broker and topic details
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'iB360_access_records'

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'consumer-group-1',
    'auto.offset.reset': 'earliest'  # Start consuming from the earliest message
}

# Initialize the Kafka consumer
consumer = Consumer(consumer_config)
consumer.subscribe([TOPIC_NAME])

print(f"Listening to topic: {TOPIC_NAME}")

try:
    while True:
        # Poll for messages
        msg = consumer.poll(1.0)
        if msg is None:  # No message received in this poll interval
            continue
        if msg.error():  # Handle consumer errors
            print(f"Consumer error: {msg.error()}")
            continue

        # Process the received message
        try:
            message_value = json.loads(msg.value().decode('utf-8'))
            print(f"Received: {message_value}")
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON message: {e}")
except KeyboardInterrupt:
    print("\nConsumer interrupted by user.")
finally:
    # Close the consumer gracefully
    consumer.close()
