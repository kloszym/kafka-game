# This IP should be the LAN IP of the machine RUNNING DOCKER (and thus Kafka)
# It MUST match the IP in KAFKA_ADVERTISED_LISTENERS in docker-compose.yml
KAFKA_BROKER_IP = "192.168.239.218"
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_BROKER_IP}:9092"

# Kafka Topics
PLAYER_ACTIONS_TOPIC = "player_actions"
GAME_STATE_TOPIC = "game_state"

# Game settings
CANVAS_WIDTH = 800
CANVAS_HEIGHT = 600
PLAYER_SIZE = 25
DOT_SIZE = 10
GAME_TICK_RATE = 1.0 / 20.0 # 20 updates per second