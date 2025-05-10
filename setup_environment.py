import time
from confluent_kafka.admin import AdminClient, NewTopic
# Import your config
import sys
sys.path.append('.') # if running from root project dir
from common.config import KAFKA_BOOTSTRAP_SERVERS, PLAYER_ACTIONS_TOPIC, GAME_STATE_TOPIC

def setup_game_environment():
    print("Setting up Kafka environment for Dot Collector...")
    admin_config = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    # ... (Kafka connection retry logic as you have) ...
    try:
        admin_client = AdminClient(admin_config)
        admin_client.list_topics(timeout=5) # Test connection
        print("Successfully connected to Kafka")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return

    topics_to_create = [
        NewTopic(PLAYER_ACTIONS_TOPIC, num_partitions=1, replication_factor=1), # Single partition can be fine for this
        NewTopic(GAME_STATE_TOPIC, num_partitions=1, replication_factor=1)
    ]

    futures = admin_client.create_topics(topics_to_create)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic {topic} created successfully or already exists.")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"Topic {topic} already exists.")
            else:
                print(f"Failed to create topic {topic}: {e}")
    print("Kafka topic setup complete!")

if __name__ == "__main__":
    setup_game_environment()