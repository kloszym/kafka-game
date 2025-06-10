# Kafka Game: Real-Time Multiplayer Dot Collector 🎮

**Experience dynamic multiplayer action fueled by the power of event-driven architecture!** This project demonstrates a sophisticated real-time game where players navigate a shared world, collect dots, and compete for the highest score, all orchestrated through Apache Kafka and ksqlDB.

Watch the gameplay:
*(Ensure video files are in the `img/` directory and named as below)*
*   [Gameplay Demo 1 (Single Screen)](img/video_gameplay_example_1.mp4)
*   [Gameplay Demo 2 (Two Laptops)](img/video_gameplay_example_2_two_laptops.mp4)
*   [Gameplay Demo 3 (Different Winner)](img/video_gameplay_example_3_different_winner.mp4)

![Multiplayer Kafka Game in action across two laptops](img/12.jpg)

## 🚀 Overview

The Kafka Dot Collector Game is a compelling showcase of building responsive, scalable, and resilient multiplayer experiences using an event-driven paradigm. Players control squares, aiming to collect dots that appear randomly. Each collected dot contributes to an individual score, while special dots can affect a shared team score. The game leverages Apache Kafka as its messaging backbone, ensuring reliable communication between clients and the server. ksqlDB is employed for stateful stream processing, providing an authoritative and queryable view of the game state. The entire backend infrastructure is containerized with Docker for easy deployment and management.

## ✨ Key Features

*   **Real-Time Multiplayer:** Engage multiple players simultaneously in a dynamic, shared game environment.
*   **Event-Driven Core:** Utilizes Apache Kafka for robust, decoupled communication, forming the heart of the game's architecture.
*   **Stateful Stream Processing with ksqlDB:** Leverages ksqlDB to transform raw game event streams into durable, queryable tables (`PLAYERS_TABLE`, `DOTS_TABLE`), maintaining an authoritative game state.
*   **Hybrid Communication Strategy:**
    *   **Fast Path:** Low-latency `GAME_EVENTS_TOPIC` delivers immediate UI updates to clients (player movements, dot spawns).
    *   **Authoritative Path:** Server events are fed into ksqlDB (`PLAYER_STATE_UPDATES_TOPIC`, `DOT_EVENTS_TOPIC`) to build a consistent, canonical game state.
*   **Dockerized Ecosystem:** Kafka, Zookeeper, ksqlDB Server, and ksqlDB CLI are managed via `docker-compose.yml` for effortless setup and teardown.
*   **Interactive Pygame Client:** A rich graphical client (`game_client/client.py`) built with Pygame offers an engaging player experience.
*   **Dynamic Scoring Mechanics:** Features individual scores and a shared team score, with different dot types offering varied scoring effects.
*   **Clear "Game Over" Condition:** The game concludes decisively when a player's total score (individual + shared) reaches the `WINNING_SCORE`.

## 🏗️ Architecture Deep Dive

The Kafka Game system is a symphony of interconnected components, communicating seamlessly via Kafka topics:

**1. Game Clients (`game_client/client.py`):**
    *   The player's window into the game world, crafted with **Pygame**.
    *   Handles graphical rendering, user input (W,A,S,D or arrow keys), and local prediction for smooth avatar movement.
    *   Publishes player actions (e.g., "join", "move") to the `PLAYER_ACTIONS_TOPIC`.
    *   Subscribes to `GAME_EVENTS_TOPIC` for real-time state changes (other players' positions, dot states, score updates, and game over signals).

**2. Game Server (`game_server/server.py`):**
    *   The central nervous system of the game, orchestrating all game logic and rules.
    *   Consumes player inputs from `PLAYER_ACTIONS_TOPIC`.
    *   Manages player states (positions, scores, activity), dot lifecycles, collision detection, and win condition evaluation.
    *   Produces events to multiple Kafka topics, ensuring data flows correctly to both clients and ksqlDB:
        *   `GAME_EVENTS_TOPIC`: For broadcasting fast-path updates to clients.
        *   `PLAYER_STATE_UPDATES_TOPIC`: For feeding detailed player state into ksqlDB to build the `PLAYERS_TABLE`.
        *   `DOT_EVENTS_TOPIC`: For publishing dot creation and collection events (using tombstones for deletion) to ksqlDB for the `DOTS_TABLE`.

**3. Apache Kafka (Managed by `docker-compose.yml`):**
    *   The distributed, fault-tolerant messaging backbone that enables asynchronous communication and decouples all system components.
    *   Broker IP and port are configured in `common/config.py`.

**4. ksqlDB (Managed by `docker-compose.yml`):**
    *   A powerful stream processing engine allowing SQL-like queries on real-time Kafka data.
    *   `ksqldb-server`: Executes the stream processing logic.
    *   `ksqldb-cli`: Provides an interactive shell for defining and querying ksqlDB objects.
    *   Consumes from `PLAYER_STATE_UPDATES_TOPIC` and `DOT_EVENTS_TOPIC`.
    *   The `ksql/game_logic.ksql` script defines:
        *   **Streams:** `PLAYER_STATE_UPDATES_STREAM` and `DOT_EVENTS_STREAM` directly mapping to Kafka topics.
        *   **Tables:** `PLAYERS_TABLE` and `DOTS_TABLE` as continuously updated materialized views, representing the canonical, queryable game state.

**5. Docker & Docker Compose (`docker-compose.yml`):**
    *   Simplifies the deployment and management of the entire backend infrastructure (Zookeeper, Kafka, ksqlDB Server, ksqlDB CLI), ensuring a consistent environment.

**Data Flow & Kafka Topics (defined in `common/config.py`):**

*   `PLAYER_ACTIONS_TOPIC`: Client → Server (Player inputs: "join", "move").
*   `GAME_EVENTS_TOPIC`: Server → Client (Fast-path updates for responsive UI).
*   `PLAYER_STATE_UPDATES_TOPIC`: Server → ksqlDB (Data for `PLAYERS_TABLE`).
    *   *ksqlDB Stream:* `PLAYER_STATE_UPDATES_STREAM`
*   `DOT_EVENTS_TOPIC`: Server → ksqlDB (Data for `DOTS_TABLE`).
    *   *ksqlDB Stream:* `DOT_EVENTS_STREAM`
*   `PLAYERS_TABLE_TOPIC` & `DOTS_TABLE_TOPIC`: Internal ksqlDB topics backing the materialized tables.

## 💻 Technology Stack

-   **Backend & Game Logic:** Python 3
-   **Client GUI:** Pygame
-   **Messaging Backbone:** Apache Kafka
-   **Stream Processing:** ksqlDB
-   **Containerization:** Docker, Docker Compose
-   **Kafka Python Client:** `confluent-kafka`
-   **Configuration:** Python scripts (`common/config.py`, `setup_environments.py`)

## 🛠️ Setup and Installation Guide

**Prerequisites:**
-   Python 3.8+ and `pip`
-   Docker Engine & Docker Compose (Docker Desktop is recommended)
    ![Docker Desktop Starting Engine](img/18.png)

**Steps:**

1.  **Clone the Repository:**
    ```bash
    git clone <your-repository-url>
    cd kafka-game
    ```

2.  **❗CRUCIAL: Configure Kafka Broker IP❗**
    Open `common/config.py`. You **MUST** update `KAFKA_BROKER_IP` to your machine's Local Area Network (LAN) IP address. This IP must be reachable by Docker containers and other clients on your network for multiplayer functionality.
    ```python
    # common/config.py
    KAFKA_BROKER_IP = "YOUR_MACHINE_LAN_IP_ADDRESS" # Example: "192.168.1.105"
    KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_BROKER_IP}:9092"
    ```

3.  **Install Python Dependencies:**
    (Specified in `requirements.txt`)
    ```bash
    pip install -r requirements.txt
    ```

4.  **Launch Dockerized Infrastructure:**
    From the project root:
    ```bash
    docker-compose up -d
    ```
    ![Docker Compose Up Command Output in terminal](img/2.png)

    Verify containers are running via Docker Desktop:
    ![Docker Desktop list of running containers](img/3.png)

    Optionally, check ksqlDB server logs for successful startup:
    ![Docker Desktop logs for ksqlDB server container](img/4.png)

5.  **Initialize Kafka Topics:**
    This script (`setup_environments.py`) creates the necessary Kafka topics.
    ```bash
    python setup_environments.py
    ```

6.  **Bootstrap ksqlDB Streams & Tables:**
    a.  Access the ksqlDB CLI:
        ```bash
        docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
        ```
        ![Terminal command to enter ksqlDB CLI](img/5.png)

        You'll be greeted by the ksqlDB CLI:
        ![ksqlDB CLI welcome message and status screen](img/6.png)

    b.  Execute the ksqlDB Logic:
        Open `ksql/game_logic.ksql`.
        ![VS Code showing contents of game_logic.ksql file](img/7.png)

        Copy the entire content of `ksql/game_logic.ksql` and paste it into the ksqlDB CLI. Press Enter.
        ![VS Code integrated terminal showing ksqlDB commands being pasted/executed. A Pygame client window showing "GAME OVER" is visible in the background within VS Code.](img/8.png)

        This script defines how ksqlDB processes game data, for example:
        ```ksql
        -- ksql/game_logic.ksql (snippet)
        SET 'auto.offset.reset' = 'earliest';

        CREATE STREAM IF NOT EXISTS PLAYER_STATE_UPDATES_STREAM (
            player_id VARCHAR KEY, username VARCHAR, x DOUBLE, y DOUBLE, score INT,
            shared_score INT, last_seen BIGINT, winner_id VARCHAR, winner_username VARCHAR
        ) WITH (KAFKA_TOPIC = 'player_state_updates', VALUE_FORMAT = 'JSON');

        CREATE TABLE IF NOT EXISTS PLAYERS_TABLE AS
            SELECT player_id, LATEST_BY_OFFSET(username) AS username, LATEST_BY_OFFSET(x) AS x, /* ... more fields */
            FROM PLAYER_STATE_UPDATES_STREAM GROUP BY player_id EMIT CHANGES;
        ```

    c.  Verify Table Creation:
        In the ksqlDB CLI:
        ```ksql
        SHOW TABLES;
        ```
        You should see `PLAYERS_TABLE` and `DOTS_TABLE`.
        ![ksqlDB CLI output for SHOW TABLES command](img/13.png)

## ▶️ How to Run the Game

1.  **Ensure Docker services are active** (`docker-compose up -d`).
2.  **Confirm Kafka topics & ksqlDB setup is complete.**

3.  **Start the Game Server (`game_server/server.py`):**
    In a new terminal:
    ```bash
    python game_server/server.py
    ```
    ![Game server initial output in terminal](img/9.png)
    Watch for logs indicating player joins:
    ![Game server terminal output showing players joining](img/14.png)

4.  **Launch Game Clients (`game_client/client.py`):**
    For each player, open a new terminal (on the same or different machines on the network) and run:
    ```bash
    python game_client/client.py
    ```
    Enter a username when prompted:
    ![Game client terminal output showing username prompt](img/10.png)

5.  **Enjoy the Game!**
    Use `W, A, S, D` or `Arrow Keys` to move. Collect dots, watch the scores, and aim for victory!
    ![Pygame client window showing single player gameplay](img/11.png)

    The game concludes with a "GAME OVER" screen (visible in the background of `img/8.png`).

## 🔍 Inspecting Live Game State with ksqlDB

Peek into the authoritative game state directly via ksqlDB:
1.  Connect to ksqlDB CLI: `docker exec -it ksqldb-cli ksql http://ksqldb-server:8088`
2.  Run live queries:
    View dot states:
    ```ksql
    SELECT * FROM DOTS_TABLE EMIT CHANGES;
    ```
    ![ksqlDB CLI output for SELECT query on DOTS_TABLE](img/15.png)

    View player states:
    ```ksql
    SELECT * FROM PLAYERS_TABLE EMIT CHANGES;
    ```
    ![ksqlDB CLI output for SELECT query on PLAYERS_TABLE (header)](img/16.png)
    ![ksqlDB CLI output for SELECT query on PLAYERS_TABLE (data)](img/17.png)

## 📂 Project Structure Overview
A look at the project's organization:
![VS Code file explorer view of the project structure](img/19.png)
```
kafka-game/
├── common/
│   ├── config.py           # Central config: Kafka IPs, topics, game rules
│   └── kafka_service.py    # Kafka producer/consumer helpers
├── game_client/
│   └── client.py           # Pygame client: UI, input, Kafka communication
├── game_server/
│   └── server.py           # Core game logic, state management, event production
├── ksql/
│   ├── cleanup.ksql        # (Optional) For resetting ksqlDB objects
│   └── game_logic.ksql     # ksqlDB stream & table definitions
├── img/                    # Images and videos for this README
│   ├── 1.png
│   ├── 2.png
│   ├── ... (and so on for all 19 images and 3 videos)
│   └── 19.png
├── .gitignore
├── docker-compose.yml      # Defines Docker services (Kafka, Zookeeper, ksqlDB)
├── README.md               # This masterpiece!
├── requirements.txt        # Python dependencies (confluent-kafka, pygame)
└── setup_environments.py   # Script for Kafka topic creation
```

## 🛑 Stopping the Environment

To gracefully shut down and remove all Docker containers, networks, and volumes:
```bash
docker-compose down -v --remove-orphans
```
![Terminal output of docker-compose down command](img/1.png)

---

We hope you enjoy playing, exploring, and learning from the Kafka Dot Collector Game!
