# game_client/client.py
import pygame
import uuid
import json
import threading
import time
import random
from pygame.math import Vector2
import sys
import os
import traceback

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.config import *
from confluent_kafka import Consumer, Producer

# Stałe graficzne
SCORE_PANEL_WIDTH = 220
WINDOW_WIDTH = CANVAS_WIDTH + SCORE_PANEL_WIDTH
WINDOW_HEIGHT = CANVAS_HEIGHT
FPS = 60
ACTION_SEND_INTERVAL = 1.0 / 20.0
COLOR_BACKGROUND_TOP = pygame.Color("#2c3e50")
COLOR_SCORE_PANEL = pygame.Color(44, 62, 80, 200)
COLOR_DIVIDER = pygame.Color("#ecf0f1")
COLOR_PLAYER_SELF = pygame.Color("#3498db")
COLOR_PLAYER_OTHER = pygame.Color("#9b59b6")
COLOR_TEXT = pygame.Color("#ecf0f1")
COLOR_TEXT_HIGHLIGHT = pygame.Color("#f1c40f")
COLOR_GAME_OVER = pygame.Color("#e74c3c") # NOWOŚĆ
COLOR_DOT = pygame.Color("#f1c40f")
COLOR_DOT_SHARED_POSITIVE = pygame.Color("#2ecc71")
COLOR_DOT_SHARED_NEGATIVE = pygame.Color("#e74c3c")

class GameClient:
    def __init__(self):
        self.username = input("Enter username: ")
        if not self.username:
            self.username = f"Anon_{uuid.uuid4().hex[:4]}"
        self.player_id = str(uuid.uuid4())

        self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': f'client-group-{uuid.uuid4().hex}',
            'auto.offset.reset': 'latest'
        })
        self.consumer.subscribe([GAME_EVENTS_TOPIC])

        self.players = {}
        self.dots = {}
        self.rendered_players = {}
        self.shared_score = 0
        self.winner = None  
        self.state_lock = threading.Lock()
        self.running = True
        self.game_state_loaded = False
        self.last_join_request_time = 0
        
        pygame.init()
        self.screen = pygame.display.set_mode((WINDOW_WIDTH, WINDOW_HEIGHT))
        self.clock = pygame.time.Clock()
        self.font = pygame.font.SysFont("Verdana", 18)
        self.font_name = pygame.font.SysFont("Verdana", 12)
        # NOWOŚĆ: Większa czcionka dla komunikatu o końcu gry
        self.font_game_over = pygame.font.SysFont("Verdana", 60)

        self.player_velocity = Vector2(0, 0)
        self.last_action_send_time = 0

        self.dot_colors = {
            DOT_TYPE_STANDARD: COLOR_DOT,
            DOT_TYPE_SHARED_POSITIVE: COLOR_DOT_SHARED_POSITIVE,
            DOT_TYPE_SHARED_NEGATIVE: COLOR_DOT_SHARED_NEGATIVE
        }

        threading.Thread(target=self.consume_loop, daemon=True).start()

    def send_action(self, action_type, dx=0.0, dy=0.0):
        payload = {
            "player_id": self.player_id, "username": self.username,
            "action": action_type, "dx": float(dx), "dy": float(dy)
        }
        self.producer.produce(PLAYER_ACTIONS_TOPIC, key=self.player_id.encode('utf-8'), value=json.dumps(payload).encode('utf-8'))

    def consume_loop(self):
        print("Game event consumer started.")
        while self.running:
            msg = self.consumer.poll(1.0)
            if not msg or msg.error():
                continue

            try:
                event = json.loads(msg.value())
                event_type = event.get('type')

                with self.state_lock:
                    if event_type == 'GAME_STATE_SNAPSHOT' and event.get('recipient_id') == self.player_id:
                        if not self.game_state_loaded:
                            print("Received game state snapshot. Loading game...")
                            state = event['state']
                            self.players = state.get('players', {})
                            self.dots = {dot['id']: dot for dot in state.get('dots', [])}
                            self.shared_score = state.get('shared_score', 0)
                            self.winner = state.get('winner') # NOWOŚĆ: Załaduj zwycięzcę przy dołączaniu
                            self.game_state_loaded = True
                        continue

                    # NOWOŚĆ: Obsługa zdarzenia końca gry
                    if event_type == 'GAME_OVER':
                        self.winner = event['winner']
                        print(f"Game over! Winner: {self.winner['username']}")
                        continue

                    if not self.game_state_loaded:
                        continue

                    if event_type == 'PLAYERS_MOVED':
                        for pid, pos in event['positions'].items():
                            if pid in self.players and pid not in self.rendered_players:
                                self.rendered_players[pid] = {'render_pos': Vector2(pos['x'], pos['y']), 'server_pos': Vector2(pos['x'], pos['y'])}
                            if pid in self.rendered_players:
                                self.rendered_players[pid]['server_pos'] = Vector2(pos['x'], pos['y'])
                    elif event_type == 'DOT_COLLECTED':
                        self.dots.pop(event['dot_id'], None)
                    elif event_type == 'DOT_CREATED':
                        self.dots[event['dot']['id']] = event['dot']
                    elif event_type == 'PLAYER_LEFT':
                        self.players.pop(event['player_id'], None)
                        self.rendered_players.pop(event['player_id'], None)
                    elif event_type == 'PLAYER_JOINED':
                        player_data = event['player']
                        if player_data['player_id'] != self.player_id:
                            self.players[player_data['player_id']] = player_data
                    elif event_type == 'SCORE_UPDATE':
                        for pid, score_data in event['scores'].items():
                            if pid in self.players:
                                self.players[pid]['score'] = score_data['score']
                        self.shared_score = event['shared_score']

            except json.JSONDecodeError as e: print(f"Failed to decode event JSON: {e}")
            except Exception as e: print(f"Error processing event: {e}")

    def handle_input(self):
        for e in pygame.event.get():
            if e.type == pygame.QUIT:
                self.running = False; return

        if not self.game_state_loaded or self.winner:
            self.player_velocity.x = 0; self.player_velocity.y = 0
            return

        keys = pygame.key.get_pressed()
        self.player_velocity.x = (keys[pygame.K_d] or keys[pygame.K_RIGHT]) - (keys[pygame.K_a] or keys[pygame.K_LEFT])
        self.player_velocity.y = (keys[pygame.K_s] or keys[pygame.K_DOWN]) - (keys[pygame.K_w] or keys[pygame.K_UP])
        
        if self.player_velocity.length_squared() > 0: self.player_velocity.normalize_ip()

        if time.time() - self.last_action_send_time > ACTION_SEND_INTERVAL:
            self.send_action("move", self.player_velocity.x, self.player_velocity.y)
            self.last_action_send_time = time.time()

    def update(self, dt):
        if not self.game_state_loaded: return

        with self.state_lock:
            # Aktualizacja pozycji graczy, nawet po końcu gry (dla płynności interpolacji)
            for pid, p_data in self.players.items():
                if pid not in self.rendered_players:
                    self.rendered_players[pid] = {
                        'render_pos': Vector2(p_data.get('x', 0), p_data.get('y', 0)),
                        'server_pos': Vector2(p_data.get('x', 0), p_data.get('y', 0))
                    }
            for pid in list(self.rendered_players.keys()):
                if pid not in self.players: self.rendered_players.pop(pid)
            
            for pid, r_data in self.rendered_players.items():
                # Przewiduj ruch tylko jeśli gra trwa
                if pid == self.player_id and not self.winner:
                    r_data['render_pos'] += self.player_velocity * PLAYER_SPEED * dt
                r_data['render_pos'] = r_data['render_pos'].lerp(r_data['server_pos'], 0.15)

    def draw(self):
        self.screen.fill(COLOR_BACKGROUND_TOP)

        if not self.game_state_loaded:
            loading_text = self.font.render("Connecting to server and loading game state...", True, COLOR_TEXT)
            text_rect = loading_text.get_rect(center=(WINDOW_WIDTH / 2, WINDOW_HEIGHT / 2))
            self.screen.blit(loading_text, text_rect)
            pygame.display.flip()
            return

        with self.state_lock:
            # Rysowanie kropek i graczy
            for d in self.dots.values():
                color = self.dot_colors.get(d.get('type'), COLOR_DOT)
                pygame.draw.circle(self.screen, color, (d.get('x'), d.get('y')), DOT_SIZE)

            for pid, r_data in self.rendered_players.items():
                p_data = self.players.get(pid, {})
                if not p_data: continue
                color = COLOR_PLAYER_SELF if pid == self.player_id else COLOR_PLAYER_OTHER
                pos = r_data['render_pos']
                player_rect = pygame.Rect(pos.x - PLAYER_SIZE / 2, pos.y - PLAYER_SIZE / 2, PLAYER_SIZE, PLAYER_SIZE)
                pygame.draw.rect(self.screen, color, player_rect, border_radius=5)
                name_surf = self.font_name.render(p_data.get('username', '...'), True, COLOR_TEXT)
                name_rect = name_surf.get_rect(center=(pos.x, pos.y - PLAYER_SIZE / 2 - 10))
                self.screen.blit(name_surf, name_rect)

        # Rysowanie panelu wyników
        score_panel = pygame.Surface((SCORE_PANEL_WIDTH, WINDOW_HEIGHT), pygame.SRCALPHA)
        score_panel.fill(COLOR_SCORE_PANEL)
        self.screen.blit(score_panel, (CANVAS_WIDTH, 0))
        pygame.draw.line(self.screen, COLOR_DIVIDER, (CANVAS_WIDTH, 0), (CANVAS_WIDTH, WINDOW_HEIGHT), 2)
        
        self.screen.blit(self.font.render("Scores", True, COLOR_TEXT), (CANVAS_WIDTH + 20, 20))
        y = 50
        
        sorted_players = sorted(self.players.values(), key=lambda x: x.get('score', 0), reverse=True)
        for p in sorted_players:
            score_text = f"{p.get('username', '...'):<12}: {p.get('score', 0)}"
            self.screen.blit(self.font.render(score_text, True, COLOR_TEXT), (CANVAS_WIDTH + 20, y)); y += 30
        
        y += 20
        shared_score_text = f"Shared Score: {self.shared_score}"
        self.screen.blit(self.font.render(shared_score_text, True, COLOR_TEXT_HIGHLIGHT), (CANVAS_WIDTH + 20, y))

        # NOWOŚĆ: Wyświetlanie komunikatu o końcu gry
        if self.winner:
            overlay = pygame.Surface((CANVAS_WIDTH, WINDOW_HEIGHT), pygame.SRCALPHA)
            overlay.fill((0, 0, 0, 180))
            self.screen.blit(overlay, (0, 0))

            game_over_surf = self.font_game_over.render("GAME OVER", True, COLOR_GAME_OVER)
            game_over_rect = game_over_surf.get_rect(center=(CANVAS_WIDTH / 2, WINDOW_HEIGHT / 2 - 40))
            self.screen.blit(game_over_surf, game_over_rect)
            
            winner_text = f"Winner: {self.winner['username']}"
            winner_surf = self.font.render(winner_text, True, COLOR_TEXT_HIGHLIGHT)
            winner_rect = winner_surf.get_rect(center=(CANVAS_WIDTH / 2, WINDOW_HEIGHT / 2 + 30))
            self.screen.blit(winner_surf, winner_rect)

        pygame.display.flip()

    def run(self):
        while self.running:
            if not self.game_state_loaded and time.time() - self.last_join_request_time > 3.0:
                print("Requesting game state...")
                self.send_action("join")
                self.last_join_request_time = time.time()

            self.handle_input()
            dt = self.clock.tick(FPS) / 1000.0
            self.update(dt)
            self.draw()

        print("Shutting down client.")
        self.producer.flush(1)
        self.consumer.close()
        pygame.quit()

if __name__ == "__main__":
    client = None
    try:
        client = GameClient()
        client.run()
    except (KeyboardInterrupt, SystemExit):
        print("Client shutdown requested.")
    except Exception as e:
        print(f"An unhandled exception occurred: {e}")
        traceback.print_exc()
    finally:
        if client and client.running:
            client.running = False
            time.sleep(1)