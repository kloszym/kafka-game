"""
game_server/server.py

The main game server logic.
- Consumes player actions from Kafka.
- Maintains the authoritative game state.
- Runs the game simulation (player movement, dot collection).
- Broadcasts the updated game state back to Kafka.
"""
import threading
import time
import json
import uuid
import random
from math import sqrt

# --- Python Path Modification ---
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- End of Python Path Modification ---

from common.kafka_service import KafkaService
from common.config import (
    PLAYER_ACTIONS_TOPIC, GAME_STATE_TOPIC, CANVAS_WIDTH, CANVAS_HEIGHT,
    PLAYER_SIZE, DOT_SIZE, GAME_TICK_RATE, WINNING_SCORE, MAX_DOTS,
    PLAYER_TIMEOUT_SECONDS, PLAYER_SPEED, DOT_TYPE_STANDARD,
    DOT_TYPE_SHARED_POSITIVE, DOT_TYPE_SHARED_NEGATIVE
)
from confluent_kafka import KafkaError

class GameServer:
    """Manages the server-side game logic and state."""
    def __init__(self):
        print("Initializing Game Server...")
        self.kafka_service = KafkaService("server")
        self.producer = self.kafka_service.get_producer()
        # A unique group_id ensures the server gets all player actions.
        self.consumer = self.kafka_service.get_consumer(
            [PLAYER_ACTIONS_TOPIC], group_id_suffix="server-actions"
        )
        self.running = True

        self.game_state = {
            "players": {},
            "dots": [],
            "shared_score": 0,
            "winner": None
        }
        self.player_velocities = {}
        self.player_last_seen = {}
        self.state_lock = threading.Lock()

    # --- START: MODIFICATION 1 of 2 ---
    def _generate_dot(self):
        """Creates a new dot with a random type and position."""
        # MODIFICATION: Changed dot spawn weights for better balance.
        dot_type = random.choices(
            [DOT_TYPE_STANDARD, DOT_TYPE_SHARED_POSITIVE, DOT_TYPE_SHARED_NEGATIVE],
            weights=[0.5, 0.25, 0.25], # Standard, Green, Red
            k=1
        )[0]
        return {
            'id': str(uuid.uuid4()),
            'x': random.randint(DOT_SIZE, CANVAS_WIDTH - DOT_SIZE),
            'y': random.randint(DOT_SIZE, CANVAS_HEIGHT - DOT_SIZE),
            'type': dot_type
        }
    # --- END: MODIFICATION 1 of 2 ---

    def consume_actions(self):
        """Continuously consumes player actions from the Kafka topic."""
        print("Action consumer thread started.")
        while self.running:
            msg = self.consumer.poll(timeout=0.1)
            if msg is None: continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Server Consumer error: {msg.error()}")
                continue

            try:
                action = json.loads(msg.value().decode('utf-8'))
                self._process_action(action)
            except (json.JSONDecodeError, UnicodeDecodeError, Exception) as e:
                print(f"Server error processing message: {e}")
        self.consumer.close()
        print("Action consumer thread stopped.")

    def _process_action(self, action):
        """Updates the server's internal state based on a player action."""
        player_id = action.get("player_id")
        if not player_id: return

        with self.state_lock:
            # If player is new, initialize their state
            if player_id not in self.game_state["players"]:
                print(f"New player connected: {action.get('username')} ({player_id})")
                self.game_state["players"][player_id] = {
                    "player_id": player_id,
                    "username": action.get("username", "Unknown"),
                    "x": random.randint(PLAYER_SIZE, CANVAS_WIDTH - PLAYER_SIZE),
                    "y": random.randint(PLAYER_SIZE, CANVAS_HEIGHT - PLAYER_SIZE),
                    "score": 0,
                }
            # Update velocity and last seen time for all actions
            self.player_velocities[player_id] = (action.get("dx", 0), action.get("dy", 0))
            self.player_last_seen[player_id] = time.time()

    def _update_game_state(self, dt):
        """The main game logic tick."""
        with self.state_lock:
            if self.game_state["winner"]: return  # Stop game logic if there's a winner

            # --- Update Player Positions ---
            for pid, player in self.game_state["players"].items():
                dx, dy = self.player_velocities.get(pid, (0, 0))
                player['x'] += dx * PLAYER_SPEED * dt
                player['y'] += dy * PLAYER_SPEED * dt
                # Clamp position to canvas bounds
                player['x'] = max(PLAYER_SIZE / 2, min(player['x'], CANVAS_WIDTH - PLAYER_SIZE / 2))
                player['y'] = max(PLAYER_SIZE / 2, min(player['y'], CANVAS_HEIGHT - PLAYER_SIZE / 2))

            # --- Collision Detection and Scoring ---
            collected_dots_ids = set()
            for pid, player in self.game_state["players"].items():
                for dot in self.game_state["dots"]:
                    if dot['id'] in collected_dots_ids: continue
                    dist = sqrt((player['x'] - dot['x'])**2 + (player['y'] - dot['y'])**2)
                    if dist < (PLAYER_SIZE / 2) + (DOT_SIZE / 2):
                        collected_dots_ids.add(dot['id'])
                        # Apply score based on dot type
                        if dot['type'] == DOT_TYPE_STANDARD:
                            player['score'] += 1
                        elif dot['type'] == DOT_TYPE_SHARED_POSITIVE:
                            self.game_state['shared_score'] += 1
                        elif dot['type'] == DOT_TYPE_SHARED_NEGATIVE:
                            self.game_state['shared_score'] = max(0, self.game_state['shared_score'] - 1)
                        
                        # --- START: MODIFICATION 2 of 2 ---
                        # MODIFICATION: Win condition now checks combined player score and shared score.
                        total_score = player['score'] + self.game_state['shared_score']
                        if total_score >= WINNING_SCORE:
                            self.game_state['winner'] = {"id": pid, "username": player['username']}
                            print(f"GAME OVER! Winner: {player['username']} with a combined score of {total_score}")
                            # Break out of loops as game has ended
                            break
                        # --- END: MODIFICATION 2 of 2 ---
                if self.game_state['winner']: break
            
            # Remove collected dots
            if collected_dots_ids:
                self.game_state["dots"] = [d for d in self.game_state["dots"] if d['id'] not in collected_dots_ids]

            # --- Replenish Dots ---
            while len(self.game_state["dots"]) < MAX_DOTS:
                self.game_state["dots"].append(self._generate_dot())
            
            # --- Handle Player Timeouts ---
            now = time.time()
            timed_out_players = [
                pid for pid, last_seen in self.player_last_seen.items()
                if now - last_seen > PLAYER_TIMEOUT_SECONDS
            ]
            for pid in timed_out_players:
                username = self.game_state['players'].get(pid, {}).get('username', 'A player')
                print(f"{username} timed out. Removing from game.")
                self.game_state["players"].pop(pid, None)
                self.player_velocities.pop(pid, None)
                self.player_last_seen.pop(pid, None)

    def broadcast_state(self):
        """Serializes and sends the current game state to Kafka."""
        try:
            with self.state_lock:
                payload = json.dumps(self.game_state).encode('utf-8')
            self.producer.produce(GAME_STATE_TOPIC, value=payload)
            self.producer.poll(0)
        except Exception as e:
            print(f"Server error broadcasting state: {e}")

    def run(self):
        """Main server loop."""
        consumer_thread = threading.Thread(target=self.consume_actions, daemon=True)
        consumer_thread.start()

        while self.running:
            start_time = time.time()
            self._update_game_state(GAME_TICK_RATE)
            self.broadcast_state()
            
            time_spent = time.time() - start_time
            sleep_time = max(0, GAME_TICK_RATE - time_spent)
            time.sleep(sleep_time)

    def shutdown(self):
        print("Shutting down server...")
        self.running = False
        if hasattr(self, 'producer'): self.producer.flush(timeout=3)
        print("Server shutdown complete.")

if __name__ == "__main__":
    server = GameServer()
    try:
        server.run()
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt received.")
    finally:
        server.shutdown()