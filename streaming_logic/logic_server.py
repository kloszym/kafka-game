"""
streaming_logic/logic_server.py (v2 - Poprawiony)

Zmiany:
- Usunięto konsumenta stanu graczy. Serwer jest teraz jedynym autorytatywnym
  źródłem stanu graczy w swojej pamięci, co eliminuje race condition (rubber-banding).
- Serwer nadal konsumuje stan kropek, aby wiedzieć, gdzie są i móc wykrywać kolizje.
"""
import json
import time
from math import sqrt
import threading
from confluent_kafka import Consumer, Producer

import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from common.config import *

class LogicServer:
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        self.running = True

        self.players = {}
        self.dots = {}
        self.player_velocities = {}
        self.shared_score = 0
        self.winner_info = None
        self.state_lock = threading.Lock()

    def _consume_dots_state(self):
        """Konsumuje stan kropek z tabeli ksqlDB."""
        consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'logic-server-dots-state-consumer',
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(consumer_config)
        consumer.subscribe([DOTS_TABLE_TOPIC])
        print(f"Consumer for {DOTS_TABLE_TOPIC} started.")
        while self.running:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                print(f"Consumer error ({DOTS_TABLE_TOPIC}): {msg.error()}")
                continue
            
            key = msg.key().decode('utf-8')
            with self.state_lock:
                if msg.value() is None:
                    self.dots.pop(key, None)
                else:
                    self.dots[key] = json.loads(msg.value())
        consumer.close()

    def _consume_actions(self):
        """Konsumuje akcje graczy i aktualizuje stan w pamięci serwera."""
        consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'logic-server-actions-consumer',
            'auto.offset.reset': 'latest'
        })
        consumer.subscribe([PLAYER_ACTIONS_TOPIC])
        print("Action consumer started.")
        while self.running:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                print(f"Action consumer error: {msg.error()}")
                continue
            
            action = json.loads(msg.value())
            player_id = action.get("player_id")
            if not player_id: continue

            with self.state_lock:
                self.player_velocities[player_id] = (action.get("dx", 0), action.get("dy", 0))
                
                if player_id not in self.players:
                    print(f"New player detected: {action['username']} ({player_id})")
                    self.players[player_id] = {
                        "PLAYER_ID": player_id,
                        "USERNAME": action.get("username", "Unknown"),
                        "X": float(CANVAS_WIDTH / 2),
                        "Y": float(CANVAS_HEIGHT / 2),
                        "SCORE": 0
                    }
                self.players[player_id]['LAST_SEEN'] = int(time.time())

    def run_game_loop(self):
        print("Game loop started.")
        while self.running:
            start_time = time.time()
            self._update_game_logic()
            self._broadcast_state_updates()
            time_spent = time.time() - start_time
            sleep_time = max(0, GAME_TICK_RATE - time_spent)
            time.sleep(sleep_time)

    def _update_game_logic(self):
        with self.state_lock:
            if self.winner_info: return

            for pid, player in self.players.items():
                dx, dy = self.player_velocities.get(pid, (0, 0))
                player['X'] += dx * PLAYER_SPEED * GAME_TICK_RATE
                player['Y'] += dy * PLAYER_SPEED * GAME_TICK_RATE
                player['X'] = max(PLAYER_SIZE / 2, min(player['X'], CANVAS_WIDTH - PLAYER_SIZE / 2))
                player['Y'] = max(PLAYER_SIZE / 2, min(player['Y'], CANVAS_HEIGHT - PLAYER_SIZE / 2))

            collected_dot_ids = set()
            for pid, player in self.players.items():
                for dot_id, dot in self.dots.items():
                    if dot_id in collected_dot_ids: continue
                    dist = sqrt((player['X'] - dot['X'])**2 + (player['Y'] - dot['Y'])**2)
                    if dist < (PLAYER_SIZE / 2) + (DOT_SIZE / 2):
                        collected_dot_ids.add(dot_id)
                        dot_type = dot.get('TYPE', DOT_TYPE_STANDARD)
                        if dot_type == DOT_TYPE_STANDARD:
                            player['SCORE'] += 1
                        elif dot_type == DOT_TYPE_SHARED_POSITIVE:
                            self.shared_score += 1
                        elif dot_type == DOT_TYPE_SHARED_NEGATIVE:
                            self.shared_score = max(0, self.shared_score - 1)
                        
                        total_score = player['SCORE'] + self.shared_score
                        if total_score >= WINNING_SCORE:
                            self.winner_info = {"id": pid, "username": player['USERNAME']}
                            print(f"GAME OVER! Winner: {player['USERNAME']}")
                            break
                if self.winner_info: break
            
            for dot_id in collected_dot_ids:
                self.producer.produce(DOT_EVENTS_TOPIC, key=dot_id.encode('utf-8'), value=None)
                self.dots.pop(dot_id, None)

            now = int(time.time())
            timed_out_players = [pid for pid, p_data in self.players.items()
                                 if now - p_data.get('LAST_SEEN', now) > PLAYER_TIMEOUT_SECONDS]
            for pid in timed_out_players:
                print(f"Player {self.players[pid]['USERNAME']} timed out. Removing.")
                self.players.pop(pid, None)
                self.player_velocities.pop(pid, None)
                self.producer.produce(PLAYER_STATE_UPDATES_TOPIC, key=pid.encode('utf-8'), value=None)

    def _broadcast_state_updates(self):
        with self.state_lock:
            for pid, player_data in self.players.items():
                payload = {
                    "player_id": pid,
                    "username": player_data["USERNAME"],
                    "x": player_data["X"], "y": player_data["Y"],
                    "score": player_data["SCORE"],
                    "shared_score": self.shared_score,
                    "last_seen": player_data.get("LAST_SEEN", int(time.time())),
                    "winner_id": self.winner_info['id'] if self.winner_info else None,
                    "winner_username": self.winner_info['username'] if self.winner_info else None
                }
                self.producer.produce(PLAYER_STATE_UPDATES_TOPIC, key=pid.encode('utf-8'),
                                      value=json.dumps(payload).encode('utf-8'))
        self.producer.poll(0)

    def start(self):
        threading.Thread(target=self._consume_dots_state, daemon=True).start()
        threading.Thread(target=self._consume_actions, daemon=True).start()
        self.run_game_loop()

    def shutdown(self):
        self.running = False
        print("Shutting down Logic Server...")
        self.producer.flush(3)

if __name__ == "__main__":
    server = LogicServer()
    try:
        server.start()
    except KeyboardInterrupt:
        server.shutdown()