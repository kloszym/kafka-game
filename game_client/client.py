# game_client/client.py

import tkinter as tk
from tkinter import simpledialog
import uuid
import json
import threading
import time

# --- Python Path Modification ---
import sys
import os
# Get the absolute path of the project root directory (kafka-dot-collector)
# This assumes client.py is in kafka-dot-collector/game_client/
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root) # Add project root to the beginning of sys.path
# --- End of Python Path Modification ---

from common.kafka_service import KafkaService
from common.config import (
    PLAYER_ACTIONS_TOPIC, GAME_STATE_TOPIC, CANVAS_WIDTH, CANVAS_HEIGHT,
    PLAYER_SIZE, DOT_SIZE
)
# Import KafkaError for handling specific consumer errors
from confluent_kafka import KafkaError


class GameClient:
    def __init__(self, root):
        self.root = root
        self.username = simpledialog.askstring("Username", "Enter your username:", parent=root)
        if not self.username:
            self.username = f"Anon_{uuid.uuid4().hex[:4]}"
        self.player_id = str(uuid.uuid4())

        self.root.title(f"Dot Collector - {self.username}")
        self.root.geometry(f"{CANVAS_WIDTH + 150}x{CANVAS_HEIGHT + 50}") # Extra space for scores
        self.root.configure(bg="#f0f0f0") # A light background for the main window

        self.kafka_service = KafkaService(self.player_id) # Suffix for client.id
        self.producer = self.kafka_service.get_producer()
        # Each client needs its own consumer group to get all game state messages
        self.consumer = self.kafka_service.get_consumer(
            [GAME_STATE_TOPIC],
            group_id_suffix=f"client-gamestate-{self.player_id}" # Unique group ID per client instance
        )

        self.game_state = {"players": {}, "dots": []}
        self.player_canvas_objects = {} # For Tkinter canvas objects {player_id: (shape_id, label_id)}
        self.dot_canvas_objects = {}    # {dot_id: shape_id}

        self.setup_ui()

        self.running = True
        self.consumer_thread = threading.Thread(target=self.consume_game_state, name=f"Client-{self.username}-Consumer")
        self.consumer_thread.daemon = True
        self.consumer_thread.start()

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        # Send initial "connect" or first move to register with the server
        self.send_action("move", 0, 0) # A no-op move can signal presence

    def setup_ui(self):
        self.main_frame = tk.Frame(self.root, bg="#f0f0f0")
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.canvas = tk.Canvas(self.main_frame, width=CANVAS_WIDTH, height=CANVAS_HEIGHT, bg="lightblue", highlightthickness=0)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.score_frame = tk.Frame(self.main_frame, width=150, bg="lightgrey", padx=5, pady=5)
        self.score_frame.pack(side=tk.RIGHT, fill=tk.Y, padx=(10, 0))
        self.score_label_title = tk.Label(self.score_frame, text="Scores:", font=("Arial", 14, "bold"), bg="lightgrey")
        self.score_label_title.pack(pady=(0, 10), anchor=tk.NW)
        self.score_labels_map = {} # {player_id: tk.Label for score display}

        # Key bindings for movement
        self.root.bind("<KeyPress-Left>", lambda e: self.send_action("move", -1, 0))
        self.root.bind("<KeyPress-Right>", lambda e: self.send_action("move", 1, 0))
        self.root.bind("<KeyPress-Up>", lambda e: self.send_action("move", 0, -1))
        self.root.bind("<KeyPress-Down>", lambda e: self.send_action("move", 0, 1))
        self.root.focus_set() # Ensure the root window has focus for key events

    def send_action(self, action_type, dx, dy):
        if not self.running: # Don't send if client is closing
            return

        action_payload = {
            "player_id": self.player_id,
            "username": self.username,
            "action": action_type,
            "dx": dx,
            "dy": dy,
            "timestamp": int(time.time() * 1000)
        }
        try:
            self.producer.produce(
                PLAYER_ACTIONS_TOPIC,
                key=self.player_id.encode('utf-8'), # Keying by player_id can be useful for partitioning or compaction
                value=json.dumps(action_payload).encode('utf-8'),
                callback=self.delivery_report # Optional: for ack/nack logging
            )
            self.producer.poll(0) # Non-blocking poll to trigger callbacks and send messages
        except BufferError:
            print("Client: Producer queue full. Message might be dropped or delayed.")
            self.producer.poll(0.1) # Try to flush
        except Exception as e:
            print(f"Client: Error producing action: {e}")

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result. """
        if err is not None:
            print(f'Client: Message delivery failed for {msg.key()}: {err}')
        # else:
        #     print(f'Client: Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')


    def consume_game_state(self):
        print(f"Client {self.username} ({self.player_id}): Game state consumer started...")
        while self.running:
            msg = self.consumer.poll(timeout=0.1) # Poll with a short timeout
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event, not an error
                    continue
                else:
                    print(f"Client {self.username}: Consumer error: {msg.error()}")
                    if msg.error().fatal():
                        print(f"Client {self.username}: Fatal consumer error. Stopping.")
                        self.running = False # Trigger shutdown
                    continue # Or break, depending on desired robustness

            try:
                new_game_state = json.loads(msg.value().decode('utf-8'))
                self.game_state = new_game_state
                # Schedule UI update on the main Tkinter thread
                if self.running: # Check again, as on_close might have set it to False
                    self.root.after(0, self.update_ui_from_state)
            except json.JSONDecodeError:
                print(f"Client {self.username}: Could not decode JSON game state: {msg.value()[:100]}...")
            except tk.TclError as e: # Handles errors if root window is destroyed while after() is pending
                if "application has been destroyed" in str(e).lower():
                    print(f"Client {self.username}: Tkinter TclError (likely window closed), stopping consumer.")
                    self.running = False
                else:
                    raise # Re-raise if it's a different TclError
            except Exception as e:
                print(f"Client {self.username}: Error processing game state: {e}. Data: {msg.value()[:100]}...")
        
        self.consumer.close()
        print(f"Client {self.username} ({self.player_id}): Game state consumer stopped.")

    def update_ui_from_state(self):
        if not self.running or not hasattr(self.canvas, 'winfo_exists') or not self.canvas.winfo_exists():
             # Don't update if client is closing or canvas is destroyed
            return

        # --- Update Players on Canvas ---
        current_player_ids_on_canvas = set(self.player_canvas_objects.keys())
        player_ids_in_current_state = set(self.game_state.get("players", {}).keys())

        # Remove players from canvas that are no longer in the game state
        for pid_to_remove in current_player_ids_on_canvas - player_ids_in_current_state:
            if pid_to_remove in self.player_canvas_objects:
                shape_id, label_id = self.player_canvas_objects.pop(pid_to_remove)
                self.canvas.delete(shape_id)
                self.canvas.delete(label_id)
            if pid_to_remove in self.score_labels_map:
                self.score_labels_map.pop(pid_to_remove).destroy()

        # Add or update players on canvas
        for pid, pdata in self.game_state.get("players", {}).items():
            x, y = pdata.get("x", CANVAS_WIDTH / 2), pdata.get("y", CANVAS_HEIGHT / 2)
            username = pdata.get("username", pid[:6])
            score = pdata.get("score", 0)
            color = "dodgerblue" if pid == self.player_id else "mediumseagreen"
            half_player_size = PLAYER_SIZE / 2

            if pid in self.player_canvas_objects: # Existing player, update position and label
                shape_id, label_id = self.player_canvas_objects[pid]
                self.canvas.coords(shape_id, x - half_player_size, y - half_player_size, x + half_player_size, y + half_player_size)
                self.canvas.itemconfig(shape_id, fill=color)
                self.canvas.coords(label_id, x, y - half_player_size - 7) # Position label above player
                self.canvas.itemconfig(label_id, text=username)
            else: # New player, create shape and label
                shape_id = self.canvas.create_rectangle(
                    x - half_player_size, y - half_player_size, x + half_player_size, y + half_player_size,
                    fill=color, outline="black", width=1
                )
                label_id = self.canvas.create_text(x, y - half_player_size - 7, text=username, anchor=tk.S, font=("Arial", 8))
                self.player_canvas_objects[pid] = (shape_id, label_id)

            # Update score display
            if pid in self.score_labels_map:
                self.score_labels_map[pid].config(text=f"{username}: {score}")
            else:
                score_text_label = tk.Label(self.score_frame, text=f"{username}: {score}", bg="lightgrey", font=("Arial", 9))
                score_text_label.pack(anchor=tk.NW)
                self.score_labels_map[pid] = score_text_label

        # --- Update Dots on Canvas ---
        current_dot_ids_on_canvas = set(self.dot_canvas_objects.keys())
        dot_ids_in_current_state = set(d.get("id") for d in self.game_state.get("dots", []))

        # Remove dots no longer in state
        for dot_id_to_remove in current_dot_ids_on_canvas - dot_ids_in_current_state:
            if dot_id_to_remove in self.dot_canvas_objects:
                self.canvas.delete(self.dot_canvas_objects.pop(dot_id_to_remove))

        # Add or update dots
        for d_data in self.game_state.get("dots", []):
            dot_id = d_data.get("id")
            if not dot_id: continue # Skip if dot has no ID

            x, y = d_data.get("x", CANVAS_WIDTH / 2), d_data.get("y", CANVAS_HEIGHT / 2)
            half_dot_size = DOT_SIZE / 2

            if dot_id in self.dot_canvas_objects: # Existing dot, update position
                self.canvas.coords(self.dot_canvas_objects[dot_id], x - half_dot_size, y - half_dot_size, x + half_dot_size, y + half_dot_size)
            else: # New dot, create shape
                shape_id = self.canvas.create_oval(
                    x - half_dot_size, y - half_dot_size, x + half_dot_size, y + half_dot_size,
                    fill="red", outline="darkred", width=1
                )
                self.dot_canvas_objects[dot_id] = shape_id

    def on_close(self):
        print(f"Client {self.username} ({self.player_id}): Closing procedure initiated...")
        if not self.running: # Already closing
            return
        self.running = False # Signal threads to stop

        # Optionally send a disconnect message to the server
        # self.send_action("disconnect_request", 0, 0)

        # Wait for the consumer thread to finish
        if hasattr(self, 'consumer_thread') and self.consumer_thread.is_alive():
            print(f"Client {self.username}: Waiting for consumer thread to join...")
            self.consumer_thread.join(timeout=3) # Increased timeout slightly
            if self.consumer_thread.is_alive():
                print(f"Client {self.username}: Consumer thread did not join in time.")

        # Flush any pending messages from the producer
        if hasattr(self, 'producer'):
            print(f"Client {self.username}: Flushing producer...")
            try:
                self.producer.flush(timeout=3)
            except Exception as e:
                print(f"Client {self.username}: Error flushing producer: {e}")
        
        # Destroy the Tkinter window
        if hasattr(self.root, 'winfo_exists') and self.root.winfo_exists():
            try:
                self.root.destroy()
                print(f"Client {self.username}: Tkinter root destroyed.")
            except tk.TclError as e:
                print(f"Client {self.username}: Error destroying Tkinter root (possibly already destroyed): {e}")
        
        print(f"Client {self.username} ({self.player_id}): Shutdown complete.")


if __name__ == "__main__":
    try:
        main_root = tk.Tk()
        app = GameClient(main_root)
        main_root.mainloop()
    except Exception as e:
        print(f"Unhandled exception in main: {e}")
        import traceback
        traceback.print_exc()