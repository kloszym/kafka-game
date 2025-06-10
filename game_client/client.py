# game_client/client.py
import pygame, uuid, json, threading, time, random
from pygame.math import Vector2
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.config import *
from confluent_kafka import Consumer, Producer, KafkaError

SCORE_PANEL_WIDTH=220; WINDOW_WIDTH=CANVAS_WIDTH+SCORE_PANEL_WIDTH; WINDOW_HEIGHT=CANVAS_HEIGHT; FPS=60
ACTION_SEND_INTERVAL=1.0/20.0; COLOR_BACKGROUND_TOP=pygame.Color("#2c3e50"); COLOR_BACKGROUND_BOTTOM=pygame.Color("#34495e")
COLOR_SCORE_PANEL=pygame.Color(44,62,80,200); COLOR_DIVIDER=pygame.Color("#ecf0f1"); COLOR_PLAYER_SELF=pygame.Color("#3498db")
COLOR_PLAYER_OTHER=pygame.Color("#9b59b6"); COLOR_SHADOW=pygame.Color(0,0,0,50); COLOR_TEXT=pygame.Color("#ecf0f1")
COLOR_TEXT_HIGHLIGHT=pygame.Color("#f1c40f"); COLOR_DOT=pygame.Color("#f1c40f"); COLOR_DOT_SHARED_POSITIVE=pygame.Color("#2ecc71")
COLOR_DOT_SHARED_NEGATIVE=pygame.Color("#e74c3c")

class GameClient:
    def __init__(self):
        self.username = input("Enter username: ")
        if not self.username: self.username = f"Anon_{uuid.uuid4().hex[:4]}"
        self.player_id = str(uuid.uuid4())
        self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        self.consumer = Consumer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, 'group.id': f'client-group-{uuid.uuid4().hex}', 'auto.offset.reset': 'earliest'})
        self.consumer.subscribe([PLAYERS_TABLE_TOPIC, DOTS_TABLE_TOPIC, GAME_EVENTS_TOPIC])
        
        self.players = {}; self.dots = {}; self.rendered_players = {}
        self.state_lock = threading.Lock(); self.running = True
        pygame.init(); self.screen = pygame.display.set_mode((WINDOW_WIDTH, WINDOW_HEIGHT)); self.clock = pygame.time.Clock()
        self.font = pygame.font.SysFont("Verdana", 18); self.font_name = pygame.font.SysFont("Verdana", 12)
        self.player_velocity = Vector2(0,0); self.last_action_send_time = 0
        
        threading.Thread(target=self.consume_loop, daemon=True).start()
        self.send_action("move", 0, 0)

    def send_action(self, a, dx, dy):
        payload = {"player_id": self.player_id, "username": self.username, "action": a, "dx": float(dx), "dy": float(dy)}
        self.producer.produce(PLAYER_ACTIONS_TOPIC, key=self.player_id.encode('utf-8'), value=json.dumps(payload).encode('utf-8'))

    def consume_loop(self):
        print("Hybrid consumer started.")
        while self.running:
            msg = self.consumer.poll(1.0)
            if not msg or msg.error(): continue
            key = msg.key().decode('utf-8') if msg.key() else None
            value = json.loads(msg.value())
            
            with self.state_lock:
                topic = msg.topic()
                if topic == PLAYERS_TABLE_TOPIC:
                    if value: self.players[key] = value
                    else: self.players.pop(key, None)
                elif topic == DOTS_TABLE_TOPIC:
                    if value: self.dots[key] = value
                    else: self.dots.pop(key, None)
                elif topic == GAME_EVENTS_TOPIC:
                    event = value
                    if event['type'] == 'PLAYERS_MOVED':
                        for pid, pos in event['positions'].items():
                            if pid in self.rendered_players: self.rendered_players[pid]['server_pos'] = Vector2(pos['x'], pos['y'])
                    elif event['type'] == 'DOT_COLLECTED':
                        self.dots.pop(event['dot_id'], None)
                    elif event['type'] == 'DOT_CREATED':
                        dot = event['dot']
                        self.dots[dot['id']] = dot # ZDARZENIE MA MAŁE LITERY
                    elif event['type'] == 'PLAYER_LEFT':
                        self.players.pop(event['player_id'], None); self.rendered_players.pop(event['player_id'], None)

    def handle_input(self):
        for e in pygame.event.get():
            if e.type == pygame.QUIT: self.running = False; return
        keys=pygame.key.get_pressed()
        self.player_velocity.x = (keys[pygame.K_d] or keys[pygame.K_RIGHT]) - (keys[pygame.K_a] or keys[pygame.K_LEFT])
        self.player_velocity.y = (keys[pygame.K_s] or keys[pygame.K_DOWN]) - (keys[pygame.K_w] or keys[pygame.K_UP])
        if self.player_velocity.length_squared() > 0: self.player_velocity.normalize_ip()
        if time.time() - self.last_action_send_time > ACTION_SEND_INTERVAL:
            self.send_action("move", self.player_velocity.x, self.player_velocity.y); self.last_action_send_time = time.time()

    def update(self, dt):
        with self.state_lock:
            for pid, p_data in self.players.items(): # Z tabeli (WIELKIE LITERY)
                if pid not in self.rendered_players:
                    self.rendered_players[pid] = {'render_pos': Vector2(p_data.get('X',0), p_data.get('Y',0)),
                                                  'server_pos': Vector2(p_data.get('X',0), p_data.get('Y',0))}
            for pid in list(self.rendered_players.keys()):
                if pid not in self.players: self.rendered_players.pop(pid)
            for pid, r_data in self.rendered_players.items():
                if pid == self.player_id: r_data['render_pos'] += self.player_velocity * PLAYER_SPEED * dt
                r_data['render_pos'] = r_data['render_pos'].lerp(r_data['server_pos'], 0.1)

    def draw(self):
        self.screen.fill(COLOR_BACKGROUND_TOP)
        with self.state_lock:
            for d in self.dots.values(): # Może być z obu źródeł
                pygame.draw.circle(self.screen, COLOR_DOT, (d.get('x', d.get('X')), d.get('y', d.get('Y'))), DOT_SIZE)
            for pid, r_data in self.rendered_players.items():
                p_data = self.players.get(pid, {}) # Z tabeli (WIELKIE LITERY)
                color = COLOR_PLAYER_SELF if pid == self.player_id else COLOR_PLAYER_OTHER
                pos = r_data['render_pos']
                pygame.draw.rect(self.screen, color, (pos.x - PLAYER_SIZE/2, pos.y - PLAYER_SIZE/2, PLAYER_SIZE, PLAYER_SIZE), border_radius=5)
                name_surf = self.font_name.render(p_data.get('USERNAME', '...'), True, COLOR_TEXT)
                self.screen.blit(name_surf, (pos.x - name_surf.get_width()/2, pos.y - PLAYER_SIZE/2 - 15))
            
            score_panel=pygame.Surface((SCORE_PANEL_WIDTH,WINDOW_HEIGHT),pygame.SRCALPHA); score_panel.fill(COLOR_SCORE_PANEL)
            self.screen.blit(score_panel,(CANVAS_WIDTH,0)); pygame.draw.line(self.screen,COLOR_DIVIDER,(CANVAS_WIDTH,0),(CANVAS_WIDTH,WINDOW_HEIGHT),2)
            self.screen.blit(self.font.render("Scores",True,COLOR_TEXT),(CANVAS_WIDTH+20,20)); y=50
            for p in sorted(self.players.values(), key=lambda x: x.get('SCORE', 0), reverse=True): # Z tabeli (WIELKIE LITERY)
                score_text = f"{p.get('USERNAME', '...')}: {p.get('SCORE', 0)}"
                self.screen.blit(self.font.render(score_text, True, COLOR_TEXT), (CANVAS_WIDTH+20, y)); y+=30
        pygame.display.flip()

    def run(self):
        while self.running: self.handle_input(); self.update(self.clock.tick(FPS)/1000.0); self.draw()
        self.producer.flush(1); self.consumer.close(); pygame.quit()

if __name__ == "__main__":
    client=None
    try: client=GameClient()
    except (KeyboardInterrupt, SystemExit): print("Client shutdown requested.")
    except Exception as e: import traceback; print(f"Unhandled exception: {e}"); traceback.print_exc()
    finally:
        if client: client.run()