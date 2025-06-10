import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
import sys

sys.path.append('.')
from common.config import *

def setup_game_environment():
    print("Setting up Kafka environment for ksqlDB-based Dot Collector...")
    admin_config = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}

    # ... (logika łączenia i retry jak w oryginale)
    max_retries = 5
    retry_delay = 10 
    for attempt in range(max_retries):
        try:
            admin_client = AdminClient(admin_config)
            admin_client.list_topics(timeout=5)
            print("Successfully connected to Kafka.")
            break
        except KafkaException as e:
            print(f"Attempt {attempt + 1}/{max_retries}: Failed to connect to Kafka: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Could not connect to Kafka after multiple retries. Aborting.")
                return

    # Lista wszystkich tematów potrzebnych w nowej architekturze
    topics_to_create = [
        NewTopic(PLAYER_ACTIONS_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(DOT_EVENTS_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(PLAYER_STATE_UPDATES_TOPIC, num_partitions=1, replication_factor=1),
        # Tematy tabel nie muszą być tworzone ręcznie, ksqlDB zrobi to za nas,
        # ale jawne utworzenie ich nie zaszkodzi.
        NewTopic(PLAYERS_TABLE_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(DOTS_TABLE_TOPIC, num_partitions=1, replication_factor=1)
    ]

    futures = admin_client.create_topics(topics_to_create)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" in str(e).upper():
                print(f"Topic '{topic}' already exists.")
            else:
                print(f"Failed to create topic '{topic}': {e}")
    print("\nKafka topic setup complete!")

if __name__ == "__main__":
    setup_game_environment()