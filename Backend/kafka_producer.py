import os
import json
import logging
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from confluent_kafka import Producer
import time

# Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database and Kafka configurations
DB_HOST = os.getenv('DB_HOST', 'ibridge.ckyleyx0qknc.ap-south-1.rds.amazonaws.com')
DB_PORT = int(os.getenv('DB_PORT', 3306))
DB_DATABASE = os.getenv('DB_DATABASE', 'capability_dev')
DB_USERNAME = os.getenv('DB_USERNAME', 'dev_capability')
DB_PASSWORD = quote_plus(os.getenv('DB_PASSWORD', 'Icapabilitydev@2021'))
DB_TABLE = os.getenv('DB_TABLE', 'tbl_access_logs')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'iB360_access_records')

# Create database connection
def get_db_connection():
    connection_string = f"mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"
    return create_engine(connection_string).connect()

# Kafka delivery report
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Produce messages to Kafka
def produce_messages(producer, topic, data):
    for _, row in data.iterrows():
        message = row.to_dict()
        try:
            producer.produce(
                topic,
                key=str(row.get('id', '')),  # Use 'id' as the key if available
                value=json.dumps(message),
                callback=delivery_report
            )
            print(f"Sent: {message}")
        except Exception as e:
            print(f"Error producing message: {e}")
    producer.flush()

# Main function to keep producer running
def main():
    last_processed_id = 0  # Initialize to process all records initially
    try:
        db_connection = get_db_connection()
        print("Database connection established.")

        producer_config = {
            'bootstrap.servers': KAFKA_BROKER,
            'client.id': 'pandas-kafka-producer'
        }
        producer = Producer(producer_config)

        print("Starting Kafka producer.")
        while True:
            # Fetch data
            if last_processed_id == 0:
                query = f"SELECT * FROM {DB_TABLE} ORDER BY id ASC"  # Fetch all records for the first run
            else:
                query = f"SELECT * FROM {DB_TABLE} WHERE id > {last_processed_id} ORDER BY id ASC"

            access_df = pd.read_sql(query, db_connection)

            if not access_df.empty:
                # Update last processed ID
                last_processed_id = access_df['id'].max()

                # Convert timestamps to ISO format
                for column in access_df.select_dtypes(include=['datetime', 'datetimetz']).columns:
                    access_df[column] = access_df[column].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

                # Produce messages to Kafka
                produce_messages(producer, TOPIC_NAME, access_df)
            else:
                print("No new records found.")
                break

            # Wait before checking for new data again
              # Adjust the interval as needed

    except KeyboardInterrupt:
        print("Producer interrupted by user.")
    except Exception as e:
        print(f"An error occurred: {e}", exc_info=True)
    finally:
        producer.flush()
main()        