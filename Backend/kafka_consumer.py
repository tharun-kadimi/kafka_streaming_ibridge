from confluent_kafka import Consumer, KafkaException, KafkaError
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

def process_message(message):
    try:
        # Attempt to decode the message value from JSON format
        message_value = json.loads(message.decode('utf-8'))
        print(f"Received message: {message_value}")
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON message: {e}")

try:
    while True:
        # Poll for messages with a timeout of 1 second
        msg = consumer.poll(5.0)
        
        if msg is None:  # No message received in this poll interval
            continue
        if msg.error():  # Handle consumer errors
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
            else:
                raise KafkaException(msg.error())
        
        # Process the received message
        process_message(msg.value())

except KeyboardInterrupt:
    print("\nConsumer interrupted by user.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Close the consumer gracefully
    consumer.close()
    print("Kafka consumer stopped.")
