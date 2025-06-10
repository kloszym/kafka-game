"""
game_client/client.py

The client application for the Kafka Dot Collector game.
- Renders the game state received from the server.
- Captures player input and sends it as actions to the server.
"""

import pygame
import uuid
import json
import threading
import time
import random
from pygame.math import Vector2

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
    PLAYER_SIZE, DOT_SIZE, DOT_TYPE_STANDARD, DOT_TYPE_SHARED_POSITIVE,
    DOT_TYPE_SHARED_NEGATIVE
)
from confluent_kafka import KafkaError

# --- Game & Visual Constants ---
SCORE_PANEL_WIDTH = 220
WINDOW_WIDTH = CANVAS_WIDTH + SCORE_PANEL_WIDTH
WINDOW_HEIGHT = CANVAS_HEIGHT
FPS = 60
ACTION_SEND_INTERVAL = 1.0 / 20.0 # Send updates to server 20 times/sec, matching tick rate

# --- Colors (A more modern palette) ---
COLOR_BACKGROUND_TOP = pygame.Color("#2c3e50")
COLOR_BACKGROUND_BOTTOM = pygame.Color("#34495e")
COLOR_SCORE_PANEL = pygame.Color(44, 62, 80, 200) # Semi-transparent
COLOR_DIVIDER = pygame.Color("#ecf0f1")
COLOR_PLAYER_SELF = pygame.Color("#3498db")
COLOR_PLAYER_OTHER = pygame.Color("#9b59b6")
COLOR_SHADOW = pygame.Color(0, 0, 0, 50)
COLOR_TEXT = pygame.Color("#ecf0f1")
COLOR_TEXT_HIGHLIGHT = pygame.Color("#f1c40f")

# --- Dot Colors ---
COLOR_DOT = pygame.Color("#f1c40f") # Standard
COLOR_DOT_SHARED_POSITIVE = pygame.Color("#2ecc71") # Green
COLOR_DOT_SHARED_NEGATIVE = pygame.Color("#e74c3c") # Red

# --- Particle Class for Effects ---
class Particle:
    def __init__(self, pos, color):
        self.pos = Vector2(pos)
        angle = random.uniform(0, 360)
        speed = random.uniform(50, 150)
        self.vel = Vector2(speed, 0).rotate(angle)
        self.radius = random.uniform(2, 5)
        self.lifespan = random.uniform(0.3, 0.8) # in seconds
        self.color = color

    def update(self, dt):
        self.pos += self.vel * dt
        self.lifespan -= dt
        self.radius -= 2 * dt # Shrink over time
        return self.lifespan > 0 and self.radius > 0

    def draw(self, surface):
        pygame.draw.circle(surface, self.color, self.pos, self.radius)

class GameClient:
    def __init__(self):
        # --- Player and Kafka Setup ---
        self.username = input("Enter your username: ")
        if not self.username:
            self.username = f"Anon_{uuid.uuid4().hex[:4]}"
        self.player_id = str(uuid.uuid4())

        self.kafka_service = KafkaService(self.player_id)
        self.producer = self.kafka_service.get_producer()
        self.consumer = self.kafka_service.get_consumer(
            [GAME_STATE_TOPIC],
            group_id_suffix=f"client-gamestate-{self.player_id}"
        )

        # --- Game State Management ---
        self.game_state = {"players": {}, "dots": [], "shared_score": 0, "winner": None}
        self.rendered_players = {} # For smooth client-side interpolation
        self.previous_dot_ids = set()
        self.particles = []
        self.state_lock = threading.Lock()
        self.running = True

        # --- Pygame Initialization ---
        pygame.init()
        self.screen = pygame.display.set_mode((WINDOW_WIDTH, WINDOW_HEIGHT))
        pygame.display.set_caption(f"Kafka Dot Collector - {self.username}")
        self.clock = pygame.time.Clock()
        self.font_title = pygame.font.SysFont("Verdana", 24, bold=True)
        self.font_score = pygame.font.SysFont("Verdana", 18)
        self.font_player_name = pygame.font.SysFont("Verdana", 12)
        self.font_winner = pygame.font.SysFont("Verdana", 60, bold=True)
        self.background_surface = self.create_gradient_background()

        # --- Timers and Input ---
        self.last_action_send_time = 0
        self.player_velocity = Vector2(0, 0)
        self.player_speed = 300 # pixels per second for local rendering
        # self.is_moving has been removed as it's no longer needed.

        # --- Start Networking ---
        self.consumer_thread = threading.Thread(target=self.consume_game_state, name=f"Client-{self.username}-Consumer")
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        self.send_action("move", 0, 0) # Announce presence

    def create_gradient_background(self):
        bg = pygame.Surface((CANVAS_WIDTH, CANVAS_HEIGHT))
        for y in range(CANVAS_HEIGHT):
            ratio = y / CANVAS_HEIGHT
            color = COLOR_BACKGROUND_TOP.lerp(COLOR_BACKGROUND_BOTTOM, ratio)
            pygame.draw.line(bg, color, (0, y), (CANVAS_WIDTH, y))
        return bg

    def create_particle_effect(self, pos, color):
        for _ in range(random.randint(15, 25)):
            self.particles.append(Particle(pos, color))

    def send_action(self, action_type, dx, dy):
        if not self.running: return
        action_payload = {
            "player_id": self.player_id, "username": self.username, "action": action_type,
            "dx": dx, "dy": dy, "timestamp": int(time.time() * 1000)
        }
        try:
            self.producer.produce(
                PLAYER_ACTIONS_TOPIC, key=self.player_id.encode('utf-8'),
                value=json.dumps(action_payload).encode('utf-8')
            )
            self.producer.poll(0)
        except Exception as e:
            print(f"Client: Error producing action: {e}")

    def consume_game_state(self):
        print(f"Client {self.username} ({self.player_id}): Consumer started...")
        while self.running:
            msg = self.consumer.poll(timeout=0.1)
            if msg is None: continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Client Consumer error: {msg.error()}")
                    if msg.error().fatal(): self.running = False
                continue
            try:
                new_state = json.loads(msg.value().decode('utf-8'))
                with self.state_lock:
                    current_dot_ids = {d['id'] for d in new_state.get("dots", [])}
                    collected_dots = self.previous_dot_ids - current_dot_ids
                    
                    if collected_dots:
                         for dot_id in collected_dots:
                             for p_dot in self.game_state.get("dots",[]):
                                 if p_dot['id'] == dot_id:
                                     dot_type = p_dot.get('type', DOT_TYPE_STANDARD)
                                     color = COLOR_DOT
                                     if dot_type == DOT_TYPE_SHARED_POSITIVE: color = COLOR_DOT_SHARED_POSITIVE
                                     elif dot_type == DOT_TYPE_SHARED_NEGATIVE: color = COLOR_DOT_SHARED_NEGATIVE
                                     self.create_particle_effect((p_dot['x'], p_dot['y']), color)
                                     break
                    self.game_state = new_state
                    self.previous_dot_ids = current_dot_ids
            except Exception as e:
                print(f"Client Error processing game state: {e}")
        self.consumer.close()
        print(f"Client {self.username} ({self.player_id}): Consumer stopped.")

    # --- START: MODIFIED METHOD (CORRECTED AND SIMPLIFIED) ---
    def handle_input(self):
        # Always process the event queue so the window can be closed.
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                self.running = False
                return

        with self.state_lock:
            game_over = self.game_state.get("winner")

        if not game_over:
            # If the game is running, read keys to determine velocity.
            keys = pygame.key.get_pressed()
            self.player_velocity.x = (keys[pygame.K_RIGHT] or keys[pygame.K_d]) - (keys[pygame.K_LEFT] or keys[pygame.K_a])
            self.player_velocity.y = (keys[pygame.K_DOWN] or keys[pygame.K_s]) - (keys[pygame.K_UP] or keys[pygame.K_w])
            
            if self.player_velocity.length_squared() > 0:
                self.player_velocity.normalize_ip()
            
            # Periodically send our current velocity (moving or stopped) to the server.
            now = time.time()
            if now - self.last_action_send_time > ACTION_SEND_INTERVAL:
                self.send_action("move", self.player_velocity.x, self.player_velocity.y)
                self.last_action_send_time = now
        else:
            # If the game is over, force local velocity to zero.
            # This stops the client-side prediction from making the player drift.
            self.player_velocity.x = 0
            self.player_velocity.y = 0
    # --- END: MODIFIED METHOD ---


    def update(self, dt):
        with self.state_lock:
            server_players = self.game_state.get("players", {})
            for pid, server_data in server_players.items():
                target_pos = Vector2(server_data['x'], server_data['y'])
                if pid not in self.rendered_players:
                    self.rendered_players[pid] = {'pos': target_pos, 'data': server_data}
                else:
                    self.rendered_players[pid]['data'] = server_data
                    # Client-side prediction uses self.player_velocity.
                    # handle_input now correctly sets this to 0 when the game is over.
                    if pid == self.player_id:
                        predicted_pos = self.rendered_players[pid]['pos'] + self.player_velocity * self.player_speed * dt
                        predicted_pos.x = max(PLAYER_SIZE / 2, min(predicted_pos.x, CANVAS_WIDTH - PLAYER_SIZE / 2))
                        predicted_pos.y = max(PLAYER_SIZE / 2, min(predicted_pos.y, CANVAS_HEIGHT - PLAYER_SIZE / 2))
                        self.rendered_players[pid]['pos'] = predicted_pos.lerp(target_pos, 0.1)
                    else:
                        current_pos = self.rendered_players[pid]['pos']
                        self.rendered_players[pid]['pos'] = current_pos.lerp(target_pos, 0.2)

            for pid in list(self.rendered_players.keys()):
                if pid not in server_players: del self.rendered_players[pid]
            self.particles = [p for p in self.particles if p.update(dt)]

    def draw(self):
        self.screen.blit(self.background_surface, (0, 0))
        with self.state_lock:
            # Draw dots
            shadow_offset = Vector2(3, 3)
            for dot in self.game_state.get("dots", []):
                pos = Vector2(dot['x'], dot['y'])
                dot_type = dot.get('type', DOT_TYPE_STANDARD)
                color = COLOR_DOT
                if dot_type == DOT_TYPE_SHARED_POSITIVE: color = COLOR_DOT_SHARED_POSITIVE
                elif dot_type == DOT_TYPE_SHARED_NEGATIVE: color = COLOR_DOT_SHARED_NEGATIVE
                pygame.draw.circle(self.screen, COLOR_SHADOW, pos + shadow_offset, DOT_SIZE / 2)
                pygame.draw.circle(self.screen, color, pos, DOT_SIZE / 2)

            # Draw players
            for p in self.rendered_players.values():
                rect = pygame.Rect(p['pos'].x - PLAYER_SIZE/2, p['pos'].y - PLAYER_SIZE/2, PLAYER_SIZE, PLAYER_SIZE)
                shadow_rect = rect.move(shadow_offset); pygame.draw.rect(self.screen, COLOR_SHADOW, shadow_rect, border_radius=5)
            for pid, p in self.rendered_players.items():
                color = COLOR_PLAYER_SELF if pid == self.player_id else COLOR_PLAYER_OTHER
                rect = pygame.Rect(p['pos'].x - PLAYER_SIZE/2, p['pos'].y - PLAYER_SIZE/2, PLAYER_SIZE, PLAYER_SIZE)
                pygame.draw.rect(self.screen, color, rect, border_radius=5)
                name_surface = self.font_player_name.render(p['data']['username'], True, COLOR_TEXT)
                name_rect = name_surface.get_rect(center=(p['pos'].x, p['pos'].y - PLAYER_SIZE / 2 - 8))
                self.screen.blit(name_surface, name_rect)

            for particle in self.particles: particle.draw(self.screen)
        
        # Draw Score Panel
        score_panel = pygame.Surface((SCORE_PANEL_WIDTH, WINDOW_HEIGHT), pygame.SRCALPHA); score_panel.fill(COLOR_SCORE_PANEL)
        self.screen.blit(score_panel, (CANVAS_WIDTH, 0))
        pygame.draw.line(self.screen, COLOR_DIVIDER, (CANVAS_WIDTH, 0), (CANVAS_WIDTH, WINDOW_HEIGHT), 2)
        title_surface = self.font_title.render("Scores", True, COLOR_TEXT); self.screen.blit(title_surface, (CANVAS_WIDTH + 20, 20))
        
        y_offset = 70
        # Draw Shared Score
        shared_score = self.game_state.get("shared_score", 0)
        shared_score_text = f"Team Score: {shared_score}"
        shared_score_surface = self.font_score.render(shared_score_text, True, COLOR_TEXT); self.screen.blit(shared_score_surface, (CANVAS_WIDTH + 20, y_offset))
        y_offset += 40

        # Draw Individual Scores
        sorted_players = sorted(self.game_state.get("players", {}).values(), key=lambda p: p['score'], reverse=True)
        for p_data in sorted_players:
            is_self = p_data['player_id'] == self.player_id
            text_color = COLOR_TEXT_HIGHLIGHT if is_self else COLOR_TEXT
            prefix = "▶ " if is_self else ""
            score_text = f"{prefix}{p_data['username']}: {p_data['score']}"
            score_surface = self.font_score.render(score_text, True, text_color); self.screen.blit(score_surface, (CANVAS_WIDTH + 20, y_offset))
            y_offset += 30

        # Draw Winner Screen
        winner_data = self.game_state.get("winner")
        if winner_data:
            overlay = pygame.Surface((CANVAS_WIDTH, CANVAS_HEIGHT), pygame.SRCALPHA); overlay.fill((0, 0, 0, 150)); self.screen.blit(overlay, (0, 0))
            win_text = f"{winner_data['username']} Wins!"
            if winner_data['id'] == self.player_id: win_text = "You Win!"
            win_surface = self.font_winner.render(win_text, True, COLOR_TEXT_HIGHLIGHT)
            win_rect = win_surface.get_rect(center=(CANVAS_WIDTH / 2, CANVAS_HEIGHT / 2)); self.screen.blit(win_surface, win_rect)

        pygame.display.flip()

    def run(self):
        while self.running:
            dt = self.clock.tick(FPS) / 1000.0
            self.handle_input()
            if not self.running: break
            self.update(dt)
            self.draw()

    def on_close(self):
        if not getattr(self, '_closing', False):
            self._closing = True
            print(f"Client {self.username}: Closing...")
            self.running = False
            if self.consumer_thread.is_alive(): self.consumer_thread.join(timeout=3)
            if hasattr(self, 'producer'): self.producer.flush(timeout=3)
            pygame.quit()
            print(f"Client {self.username}: Shutdown complete.")

if __name__ == "__main__":
    client = None
    try:
        client = GameClient()
        client.run()
    except Exception as e:
        print(f"Unhandled exception in main: {e}")
        import traceback; traceback.print_exc()
    finally:
        if client: client.on_close()