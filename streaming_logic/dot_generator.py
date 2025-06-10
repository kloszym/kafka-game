"""
streaming_logic/dot_generator.py (v2 - Poprawiony)

Zmiany:
- Całkowicie zmieniono logikę. Zamiast zawodnego liczenia komunikatów,
  serwis teraz utrzymuje w pamięci pełny stan kropek (słownik `self.dots`),
  konsumując tabelę `DOTS_TABLE` w tle.
- Główna pętla po prostu sprawdza rozmiar tego słownika, co jest niezawodne.
"""
import json
import time
import uuid
import random
import threading
from confluent_kafka import Consumer, Producer

import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    
from common.config import *

class DotGenerator:
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        self.dots = {}
        self.running = True
        self.state_lock = threading.Lock()

    def _consume_dots_state(self):
        """Utrzymuje lokalną, aktualną kopię stanu kropek z tabeli ksqlDB."""
        consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'dot-generator-state-consumer',
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe([DOTS_TABLE_TOPIC])
        print("Dot state consumer started.")
        while self.running:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                print(f"Dot consumer error: {msg.error()}")
                continue
            
            key = msg.key().decode('utf-8')
            with self.state_lock:
                if msg.value() is None: # Usunięcie kropki
                    self.dots.pop(key, None)
                else:
                    self.dots[key] = json.loads(msg.value())

    def run_generation_loop(self):
        print("Dot generation loop started.")
        while self.running:
            time.sleep(5) # Sprawdzaj co 5 sekund
            
            with self.state_lock:
                current_dot_count = len(self.dots)
            
            if current_dot_count < MAX_DOTS:
                dots_to_add = MAX_DOTS - current_dot_count
                print(f"Dot count is {current_dot_count}. Generating {dots_to_add} new dots...")
                for _ in range(dots_to_add):
                    dot_id = str(uuid.uuid4())
                    dot_type = random.choices(
                        [DOT_TYPE_STANDARD, DOT_TYPE_SHARED_POSITIVE, DOT_TYPE_SHARED_NEGATIVE],
                        weights=[0.6, 0.2, 0.2], k=1
                    )[0]
                    dot_payload = {
                        'id': dot_id,
                        'x': random.randint(DOT_SIZE, CANVAS_WIDTH - DOT_SIZE),
                        'y': random.randint(DOT_SIZE, CANVAS_HEIGHT - DOT_SIZE),
                        'type': dot_type
                    }
                    self.producer.produce(
                        DOT_EVENTS_TOPIC,
                        key=dot_id.encode('utf-8'),
                        value=json.dumps(dot_payload).encode('utf-8')
                    )
                self.producer.flush()

    def start(self):
        threading.Thread(target=self._consume_dots_state, daemon=True).start()
        self.run_generation_loop()

    def shutdown(self):
        self.running = False
        print("Shutting down Dot Generator...")

if __name__ == "__main__":
    generator = DotGenerator()
    try:
        generator.start()
    except KeyboardInterrupt:
        generator.shutdown()