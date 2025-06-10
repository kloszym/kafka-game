# common/config.py
KAFKA_BROKER_IP = "192.168.239.218" # !!! Upewnij się, że to Twój IP !!!
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_BROKER_IP}:9092"

# Tematy dla architektury hybrydowej
PLAYER_ACTIONS_TOPIC = "player_actions" # Wejście: Klient -> Serwer
GAME_EVENTS_TOPIC = "game_events"       # Szybka ścieżka: Serwer -> Klient (surowe zdarzenia)
PLAYER_STATE_UPDATES_TOPIC = "player_state_updates" # Wejście do ksqlDB: Serwer -> ksqlDB
DOT_EVENTS_TOPIC = "dot_events" # Wejście do ksqlDB: Serwer -> ksqlDB

# Tematy tabel - wolna, autorytatywna ścieżka
PLAYERS_TABLE_TOPIC = "players_table_topic" # ksqlDB -> Klient
DOTS_TABLE_TOPIC = "dots_table_topic"     # ksqlDB -> Klient

# Ustawienia gry
CANVAS_WIDTH=800; CANVAS_HEIGHT=600; PLAYER_SIZE=25; DOT_SIZE=10
GAME_TICK_RATE=1.0/30.0; PLAYER_SPEED=250
WINNING_SCORE=30; MAX_DOTS=30; PLAYER_TIMEOUT_SECONDS=15.0
DOT_TYPE_STANDARD="standard"; DOT_TYPE_SHARED_POSITIVE="team_positive"; DOT_TYPE_SHARED_NEGATIVE="team_negative"