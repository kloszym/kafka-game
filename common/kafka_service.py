# (Mostly the same as yours, but uses KAFKA_BOOTSTRAP_SERVERS from config.py)
from confluent_kafka import Producer, Consumer
# Make sure to import your config
from .config import KAFKA_BOOTSTRAP_SERVERS # Or adjust path based on how you run

class KafkaService:
    def __init__(self, client_id_suffix): # Suffix can be user_id or "game-server"
        self.client_id_suffix = client_id_suffix
        self.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS

    def get_producer(self):
        producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': f'collector-producer-{self.client_id_suffix}'
        }
        return Producer(producer_config)

    def get_consumer(self, topics, group_id_suffix, offset_reset='latest'):
        consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': f'collector-consumer-{group_id_suffix}', # Make group_id unique per consumer type
            'auto.offset.reset': offset_reset,
            'enable.auto.commit': True # Or False if you want manual commits
        }
        consumer = Consumer(consumer_config)
        consumer.subscribe(topics)
        return consumer