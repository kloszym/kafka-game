from confluent_kafka import Producer, Consumer, KafkaError

# Note: Ensure 'confluent-kafka' is installed (`pip install confluent-kafka`)
from .config import KAFKA_BOOTSTRAP_SERVERS

class KafkaService:
    """A wrapper for managing Kafka Producer and Consumer instances."""
    def __init__(self, client_id_suffix):
        self.client_id = f"dot-collector-{client_id_suffix}"
        self.producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': f"{self.client_id}-producer",
            'acks': '1' # Acknowledge after leader has written
        }
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'auto.offset.reset': 'latest', # Start reading at the end of the log
            # 'client.id' is set per consumer instance
        }

    def get_producer(self):
        """Returns a new Kafka Producer instance."""
        return Producer(self.producer_config)

    def get_consumer(self, topics, group_id_suffix):
        """
        Returns a new Kafka Consumer instance subscribed to the given topics.
        A unique group_id is essential for broadcasting messages (like game state)
        or for creating a unique consumer for a service (like the game server).
        """
        group_id = f"dot-collector-group-{group_id_suffix}"
        config = self.consumer_config.copy()
        config['group.id'] = group_id
        config['client.id'] = f"{self.client_id}-consumer-{group_id_suffix}"

        consumer = Consumer(config)
        consumer.subscribe(topics)
        print(f"Consumer created for group '{group_id}' on topics {topics}")
        return consumer