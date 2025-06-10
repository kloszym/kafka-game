import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
import sys

# Add project root to path to allow importing from 'common'
sys.path.append('.')
from common.config import KAFKA_BOOTSTRAP_SERVERS, PLAYER_ACTIONS_TOPIC, GAME_STATE_TOPIC

def setup_game_environment():
    """Connects to Kafka and creates the necessary topics for the game."""
    print("Setting up Kafka environment for Dot Collector...")
    admin_config = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}

    # Retry logic for connecting to Kafka, as it might take a moment to start up
    max_retries = 5
    retry_delay = 10  # seconds
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

    topics_to_create = [
        NewTopic(PLAYER_ACTIONS_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(GAME_STATE_TOPIC, num_partitions=1, replication_factor=1)
    ]

    # Create the topics
    futures = admin_client.create_topics(topics_to_create)
    for topic, future in futures.items():
        try:
            future.result()  # The result itself is None on success
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            # Check if the topic already exists, which is not an error
            if "TOPIC_ALREADY_EXISTS" in str(e).upper():
                print(f"Topic '{topic}' already exists.")
            else:
                print(f"Failed to create topic '{topic}': {e}")
    print("\nKafka topic setup complete!")

if __name__ == "__main__":
    setup_game_environment()