import os
import json
import logging
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from confluent_kafka import Producer
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database and Kafka configurations
DB_HOST = os.getenv('DB_HOST', 'ibridge.ckyleyx0qknc.ap-south-1.rds.amazonaws.com')
DB_PORT = int(os.getenv('DB_PORT', 3306))
DB_DATABASE = os.getenv('DB_DATABASE', 'capability_dev')
DB_USERNAME = os.getenv('DB_USERNAME', 'dev_capability')
DB_PASSWORD = quote_plus(os.getenv('DB_PASSWORD', 'Icapabilitydev@2021'))

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'iB360_access_records')

# Create database connection
def get_db_connection():
    connection_string = f"mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"
    return create_engine(connection_string).connect()

# Kafka delivery report
def delivery_report(err, msg):
    if err:
        logging.error(f"Delivery failed for record {msg.key()}: {err}")
    else:
        logging.info(f"Record delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Produce messages to Kafka
def produce_messages(producer, topic, data):
    for _, row in data.iterrows():
        message = row.to_dict()
        try:
            producer.produce(
                topic,
                key=str(row.get('id', '')),
                value=json.dumps(message),
                callback=delivery_report
            )
            logging.info(f"Sent: {message}")
        except Exception as e:
            logging.error(f"Error producing message: {e}")
        producer.flush()

# Main function with while loop
def main():
    try:
        db_connection = get_db_connection()
        logging.info("Database connection established.")

        producer_config = {
            'bootstrap.servers': KAFKA_BROKER,
            'client.id': 'pandas-kafka-producer'
        }
        producer = Producer(producer_config)

        logging.info("Starting Kafka producer.")
        while True:
            # Fetch new data
            query = "SELECT * FROM tbl_access_logs WHERE updated_at > NOW() - INTERVAL 1 MINUTE"
            access_df = pd.read_sql(query, db_connection)

            # Process data if available
            if not access_df.empty:
                access_df = access_df.applymap(lambda x: x.isoformat() if isinstance(x, pd.Timestamp) else x)
                produce_messages(producer, TOPIC_NAME, access_df)
            else:
                logging.info("No new data to send.")

            # Wait before checking again
            # time.sleep(1)  # Check for new data every 10 seconds

    except KeyboardInterrupt:
        logging.info("Producer interrupted by user.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        producer.flush()
        logging.info("Kafka producer stopped.")

if __name__ == "__main__":
    main()
