# common/config.py

# This IP should be the LAN IP of the machine RUNNING DOCKER (and thus Kafka)
KAFKA_BROKER_IP = "192.168.31.253"
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_BROKER_IP}:9092"

PLAYER_ACTIONS_TOPIC = "player_actions"
GAME_STATE_TOPIC = "game_state"

# Game settings
CANVAS_WIDTH = 800
CANVAS_HEIGHT = 600
PLAYER_SIZE = 20
DOT_SIZE = 10
GAME_TICK_RATE = 1.0 / 20.0 # 20 updates per second