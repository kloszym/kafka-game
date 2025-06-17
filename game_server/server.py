# game_server/server.py
import json
import time
import uuid
import random
import threading
from math import sqrt
from confluent_kafka import Consumer, Producer
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.config import *

WINNING_SCORE = 30 

class GameServer:
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'game-server-actions-group',
            'auto.offset.reset': 'latest'
        })
        self.consumer.subscribe([PLAYER_ACTIONS_TOPIC])
        self.running = True
        self.state = {"players": {}, "dots": [], "shared_score": 0, "winner": None}
        self.player_velocities = {}
        self.state_lock = threading.Lock()
        print("Unified Game Server Initialized.")

    def _produce_event(self, topic, key, value):
        try:
            self.producer.produce(
                topic,
                key=key.encode('utf-8') if key else None,
                value=json.dumps(value).encode('utf-8') if value is not None else None
            )
        except Exception as e:
            print(f"Error producing event: {e}")

    def _generate_dot(self):
        dot = {
            'id': str(uuid.uuid4()),
            'x': random.randint(DOT_SIZE, CANVAS_WIDTH - DOT_SIZE),
            'y': random.randint(DOT_SIZE, CANVAS_HEIGHT - DOT_SIZE),
            'type': random.choice([DOT_TYPE_STANDARD] * 7 + [DOT_TYPE_SHARED_POSITIVE] * 2 + [DOT_TYPE_SHARED_NEGATIVE])
        }
        self._produce_event(DOT_EVENTS_TOPIC, dot['id'], dot)
        self._produce_event(GAME_EVENTS_TOPIC, None, {'type': 'DOT_CREATED', 'dot': dot})
        return dot

    def consume_actions(self):
        # ... (bez zmian) ...
        print("Action consumer started.")
        while self.running:
            msg = self.consumer.poll(0.1)
            if not msg or msg.error():
                continue
            
            try:
                action = json.loads(msg.value())
                pid = action.get("player_id")
                if not pid:
                    continue
                
                action_type = action.get("action")

                with self.state_lock:
                    if action_type == "join":
                        if pid not in self.state["players"]:
                            print(f"New player joining: {action.get('username')} (ID: {pid})")
                            new_player = {
                                "player_id": pid, "username": action.get("username"),
                                "x": CANVAS_WIDTH / 2, "y": CANVAS_HEIGHT / 2,
                                "score": 0, "last_seen": time.time()
                            }
                            self.state["players"][pid] = new_player
                            self._produce_event(GAME_EVENTS_TOPIC, None, {'type': 'PLAYER_JOINED', 'player': new_player})

                        print(f"Received 'join' from {action.get('username')}. Sending state snapshot.")
                        snapshot = {
                            'type': 'GAME_STATE_SNAPSHOT',
                            'recipient_id': pid,
                            'state': {
                                'players': self.state["players"],
                                'dots': self.state["dots"],
                                'shared_score': self.state['shared_score'],
                                'winner': self.state['winner'] # NOWOŚĆ: Przekazujemy info o zwycięzcy
                            }
                        }
                        self._produce_event(GAME_EVENTS_TOPIC, None, snapshot)

                    elif action_type == "move":
                        self.player_velocities[pid] = (action.get("dx", 0), action.get("dy", 0))
                        if pid in self.state["players"]:
                            self.state["players"][pid]['last_seen'] = time.time()
            except json.JSONDecodeError as e:
                print(f"Failed to decode action JSON: {e}")
            except Exception as e:
                print(f"Error processing action: {e}")

    def run_game_loop(self):
        print("Game loop started.")
        while self.running:
            start_time = time.time()
            self._update_game_logic()
            self.producer.flush(0)
            time.sleep(max(0, GAME_TICK_RATE - (time.time() - start_time)))

    # NOWOŚĆ: Funkcja sprawdzająca warunek zwycięstwa
    def _check_for_winner(self):
        if self.state["winner"]:
            return # Gra już się skończyła

        for pid, p in self.state["players"].items():
            total_score = p['score'] + self.state['shared_score']
            if total_score >= WINNING_SCORE:
                self.state["winner"] = {'id': pid, 'username': p['username']}
                print(f"GAME OVER! Winner is {p['username']} with a total score of {total_score}.")
                # Wyślij zdarzenie o zakończeniu gry do wszystkich klientów
                self._produce_event(GAME_EVENTS_TOPIC, None, {'type': 'GAME_OVER', 'winner': self.state['winner']})
                return # Znaleziono zwycięzcę, przerywamy pętlę

    def _update_game_logic(self):
        with self.state_lock:
            # NOWOŚĆ: Jeśli jest zwycięzca, nie aktualizujemy logiki gry
            if self.state["winner"]:
                return

            dt = GAME_TICK_RATE

            moved_players = {}
            for pid, p in self.state["players"].items():
                dx, dy = self.player_velocities.get(pid, (0, 0))
                if dx != 0 or dy != 0:
                    p['x'] += dx * PLAYER_SPEED * dt
                    p['y'] += dy * PLAYER_SPEED * dt
                    p['x'] = max(PLAYER_SIZE / 2, min(p['x'], CANVAS_WIDTH - PLAYER_SIZE / 2))
                    p['y'] = max(PLAYER_SIZE / 2, min(p['y'], CANVAS_HEIGHT - PLAYER_SIZE / 2))
                    moved_players[pid] = {'x': p['x'], 'y': p['y']}

            if moved_players:
                self._produce_event(GAME_EVENTS_TOPIC, None, {'type': 'PLAYERS_MOVED', 'positions': moved_players})

            collected_ids = set()
            score_changed = False
            for pid, p in self.state["players"].items():
                for dot in self.state["dots"]:
                    if dot['id'] in collected_ids:
                        continue
                    if sqrt((p['x'] - dot['x']) ** 2 + (p['y'] - dot['y']) ** 2) < PLAYER_SIZE / 2 + DOT_SIZE / 2:
                        collected_ids.add(dot['id'])
                        score_changed = True
                        if dot['type'] == DOT_TYPE_STANDARD: p['score'] += 1
                        elif dot['type'] == DOT_TYPE_SHARED_POSITIVE: self.state['shared_score'] += 1
                        else: self.state['shared_score'] = max(0, self.state['shared_score'] - 1)
                        
                        self._produce_event(GAME_EVENTS_TOPIC, None, {'type': 'DOT_COLLECTED', 'dot_id': dot['id'], 'player_id': pid})
                        self._produce_event(DOT_EVENTS_TOPIC, dot['id'], None)

            if collected_ids:
                self.state["dots"] = [d for d in self.state["dots"] if d['id'] not in collected_ids]

            if score_changed:
                player_scores = {pid: {'score': p['score']} for pid, p in self.state['players'].items()}
                self._produce_event(GAME_EVENTS_TOPIC, None, {'type': 'SCORE_UPDATE', 'scores': player_scores, 'shared_score': self.state['shared_score']})
                # NOWOŚĆ: Po każdej zmianie punktacji sprawdzamy, czy ktoś wygrał
                self._check_for_winner()

            while len(self.state["dots"]) < MAX_DOTS:
                self.state["dots"].append(self._generate_dot())

            for pid, p_data in self.state["players"].items():
                self._produce_event(PLAYER_STATE_UPDATES_TOPIC, pid, p_data)

            now = time.time()
            for pid in list(self.state["players"].keys()):
                if now - self.state["players"][pid].get('last_seen', now) > PLAYER_TIMEOUT_SECONDS:
                    print(f"Player {self.state['players'][pid]['username']} timed out.")
                    self.state["players"].pop(pid, None)
                    self.player_velocities.pop(pid, None)
                    self._produce_event(PLAYER_STATE_UPDATES_TOPIC, pid, None)
                    self._produce_event(GAME_EVENTS_TOPIC, None, {'type': 'PLAYER_LEFT', 'player_id': pid})

    def start(self):
        threading.Thread(target=self.consume_actions, daemon=True).start()
        self.run_game_loop()

if __name__ == "__main__":
    GameServer().start()