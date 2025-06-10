"""
common/config.py

Shared configuration for the Kafka Dot Collector game.
"""

# --- Kafka Configuration ---
# This IP should be the LAN IP of the machine RUNNING DOCKER (and thus Kafka)
# It MUST match the IP in KAFKA_ADVERTISED_LISTENERS in docker-compose.yml
KAFKA_BROKER_IP = "192.168.239.218"
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_BROKER_IP}:9092"

# Kafka Topics
PLAYER_ACTIONS_TOPIC = "player_actions"
GAME_STATE_TOPIC = "game_state"


# --- Game Settings ---
CANVAS_WIDTH = 800
CANVAS_HEIGHT = 600
PLAYER_SIZE = 25
DOT_SIZE = 10
GAME_TICK_RATE = 1.0 / 20.0  # 20 updates per second

# --- New Gameplay Rules ---
WINNING_SCORE = 30
MAX_DOTS = 30
PLAYER_TIMEOUT_SECONDS = 15.0 # How long before a non-responsive player is removed
PLAYER_SPEED = 200 # pixels per second, used by the server

# --- New Dot Types ---
DOT_TYPE_STANDARD = "standard"
DOT_TYPE_SHARED_POSITIVE = "team_positive"
DOT_TYPE_SHARED_NEGATIVE = "team_negative"