# game_server/server.py

import time
import json
import random
import uuid
import threading

# --- Python Path Modification ---
import sys
import os
# Get the absolute path of the project root directory (kafka-dot-collector)
# This assumes server.py is in kafka-dot-collector/game_server/
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root) # Add project root to the beginning of sys.path
# --- End of Python Path Modification ---

from common.kafka_service import KafkaService
from common.config import (
    PLAYER_ACTIONS_TOPIC, GAME_STATE_TOPIC, CANVAS_WIDTH, CANVAS_HEIGHT,
    PLAYER_SIZE, DOT_SIZE, GAME_TICK_RATE
)

class GameLogicServer:
    def __init__(self):
        self.kafka_service = KafkaService("game-server") # Suffix for client.id
        self.producer = self.kafka_service.get_producer()
        # Server consumes actions with its own consumer group
        self.consumer = self.kafka_service.get_consumer(
            [PLAYER_ACTIONS_TOPIC],
            group_id_suffix="game-server-actions-consumer" # Unique group ID
        )

        self.players = {}  # { "player_id": {"x": ..., "y": ..., "score": ..., "username": ..., "last_seen": ...} }
        self.dots = []     # [ {"id": "dot1", "x": ..., "y": ...} ]
        self.num_dots = 5
        self.running = True
        self.player_speed = 10 # Pixels per move action

        # Initialize dots
        for i in range(self.num_dots):
            self.spawn_dot(str(uuid.uuid4()))

    def spawn_dot(self, dot_id_to_replace=None):
        # If an ID is provided, find that dot to replace it. Otherwise, add a new one if below num_dots.
        if dot_id_to_replace:
            self.dots = [d for d in self.dots if d['id'] != dot_id_to_replace]

        new_dot_id = dot_id_to_replace or str(uuid.uuid4())
        new_dot = {
            "id": new_dot_id,
            "x": random.randint(DOT_SIZE // 2, CANVAS_WIDTH - DOT_SIZE // 2),
            "y": random.randint(DOT_SIZE // 2, CANVAS_HEIGHT - DOT_SIZE // 2)
        }
        self.dots.append(new_dot)
        print(f"Spawned dot {new_dot_id} at ({new_dot['x']}, {new_dot['y']})")


    def process_player_action(self, action_data):
        player_id = action_data.get("player_id")
        username = action_data.get("username")
        action = action_data.get("action")

        if not player_id:
            print("Server: Received action without player_id")
            return

        if player_id not in self.players:
            self.players[player_id] = {
                "x": random.randint(PLAYER_SIZE // 2, CANVAS_WIDTH - PLAYER_SIZE // 2), # Random start
                "y": random.randint(PLAYER_SIZE // 2, CANVAS_HEIGHT - PLAYER_SIZE // 2),
                "score": 0,
                "username": username or f"Player_{player_id[:4]}",
                "last_seen": time.time()
            }
            print(f"Player {self.players[player_id]['username']} ({player_id}) joined/reconnected.")

        player = self.players[player_id]
        player["last_seen"] = time.time() # Update last seen time

        if action == "move":
            dx = action_data.get("dx", 0)
            dy = action_data.get("dy", 0)

            player["x"] += dx * self.player_speed
            player["y"] += dy * self.player_speed

            # Boundary checks
            player["x"] = max(PLAYER_SIZE // 2, min(player["x"], CANVAS_WIDTH - PLAYER_SIZE // 2))
            player["y"] = max(PLAYER_SIZE // 2, min(player["y"], CANVAS_HEIGHT - PLAYER_SIZE // 2))
        elif action == "disconnect_request": # Example for explicit disconnect
            print(f"Player {player['username']} requested disconnect. Removing.")
            if player_id in self.players:
                del self.players[player_id]

    def check_collisions(self):
        for player_id, player_data in list(self.players.items()): # list() for safe iteration if modifying
            for dot in list(self.dots): # list() for safe iteration
                # Simple AABB collision (center to center distance)
                dist_x = abs(player_data["x"] - dot["x"])
                dist_y = abs(player_data["y"] - dot["y"])

                # If distance between centers is less than sum of half-sizes
                if dist_x < (PLAYER_SIZE / 2 + DOT_SIZE / 2) and \
                   dist_y < (PLAYER_SIZE / 2 + DOT_SIZE / 2):
                    player_data["score"] += 1
                    print(f"Player {player_data['username']} collected dot {dot['id']}! Score: {player_data['score']}")
                    self.spawn_dot(dot['id']) # Respawn this dot by replacing it
                    break # Player collects one dot per tick

    def cleanup_inactive_players(self):
        current_time = time.time()
        inactive_threshold = 30 # seconds
        for player_id, player_data in list(self.players.items()):
            if current_time - player_data.get("last_seen", 0) > inactive_threshold:
                print(f"Player {player_data['username']} ({player_id}) timed out. Removing.")
                del self.players[player_id]


    def consume_actions(self):
        print("Server: Action consumer started...")
        while self.running:
            msg = self.consumer.poll(timeout=0.1) # Poll with a short timeout
            if msg is None:
                continue
            if msg.error():
                # Filter out _PARTITION_EOF which is common and not an error
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Server: Consumer error: {msg.error()}")
                    # Implement more robust error handling or reconnection if needed
                    # For now, we'll just log and continue or break for severe errors
                    if msg.error().fatal():
                        print("Server: Fatal consumer error. Stopping consumer thread.")
                        break
                    continue

            try:
                action_data = json.loads(msg.value().decode('utf-8'))
                # print(f"Server received action: {action_data}") # Debug: can be very verbose
                self.process_player_action(action_data)
            except json.JSONDecodeError:
                print(f"Server: Could not decode JSON from player_actions: {msg.value()}")
            except Exception as e:
                print(f"Server: Error processing action: {e}. Data: {msg.value()}")
        self.consumer.close()
        print("Server: Action consumer stopped.")


    def game_loop(self):
        print("Server: Game loop started...")
        while self.running:
            loop_start_time = time.time()

            self.check_collisions()
            self.cleanup_inactive_players() # Periodically check for inactive players

            # Ensure all dots are present if some were collected and not replaced immediately
            while len(self.dots) < self.num_dots:
                self.spawn_dot() # Add missing dots

            # --- START: MODIFIED SECTION ---
            # Create a payload for players that includes the player_id inside each player object.
            # This makes the data easier for clients to consume.
            players_payload = {}
            for pid, pdata in self.players.items():
                # Make a copy to avoid modifying the server's internal state structure
                player_info = pdata.copy()
                player_info['player_id'] = pid # Explicitly add the player_id
                players_payload[pid] = player_info
            # --- END: MODIFIED SECTION ---

            game_state_payload = {
                "players": players_payload, # Use the modified payload
                "dots": self.dots,
                "timestamp": int(time.time() * 1000)
            }
            try:
                self.producer.produce(
                    GAME_STATE_TOPIC,
                    value=json.dumps(game_state_payload).encode('utf-8')
                )
                self.producer.poll(0)
            except BufferError:
                print("Server: Producer queue full. Polling to clear...")
                self.producer.poll(0.1) # Try to flush
            except Exception as e:
                print(f"Server: Error producing game state: {e}")

            elapsed_time = time.time() - loop_start_time
            sleep_time = GAME_TICK_RATE - elapsed_time
            if sleep_time > 0:
                time.sleep(sleep_time)

        print("Server: Game loop stopped.")

    def start(self):
        # Import KafkaError here if needed for the consumer error handling
        global KafkaError
        from confluent_kafka import KafkaError


        self.consumer_thread = threading.Thread(target=self.consume_actions, name="ServerActionConsumer")
        self.consumer_thread.daemon = True
        self.consumer_thread.start()

        self.game_loop_thread = threading.Thread(target=self.game_loop, name="ServerGameLoop")
        self.game_loop_thread.daemon = True
        self.game_loop_thread.start()

        print("Server started. Press Ctrl+C to stop.")
        try:
            while self.running: # Keep main thread alive, check running flag
                if not self.consumer_thread.is_alive() or not self.game_loop_thread.is_alive():
                    if self.running: # If they died unexpectedly
                        print("Server: A worker thread has died. Shutting down.")
                        self.running = False
                time.sleep(1)
        except KeyboardInterrupt:
            print("Server: Shutdown initiated by Ctrl+C...")
        finally:
            self.shutdown()

    def shutdown(self):
        print("Server: Shutting down internal components...")
        self.running = False # Signal threads to stop

        if hasattr(self, 'consumer_thread') and self.consumer_thread.is_alive():
            print("Server: Waiting for consumer thread to join...")
            self.consumer_thread.join(timeout=5)
            if self.consumer_thread.is_alive():
                print("Server: Consumer thread did not join in time.")

        if hasattr(self, 'game_loop_thread') and self.game_loop_thread.is_alive():
            print("Server: Waiting for game loop thread to join...")
            self.game_loop_thread.join(timeout=5)
            if self.game_loop_thread.is_alive():
                print("Server: Game loop thread did not join in time.")

        if hasattr(self, 'producer'):
            print("Server: Flushing producer...")
            self.producer.flush(timeout=5) # Ensure pending messages are sent
        
        print("Server: Shutdown complete.")

if __name__ == "__main__":
    server = GameLogicServer()
    server.start()