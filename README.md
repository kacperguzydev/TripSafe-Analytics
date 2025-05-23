# 🚗 TripSafe Analytics

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)  
[![Python](https://img.shields.io/badge/python-3.8%2B-green.svg)]()  
[![Docker](https://img.shields.io/badge/docker-enabled-blue.svg)]()  
[![Airflow](https://img.shields.io/badge/airflow-2.x-orange.svg)]()

A real-time data pipeline for monitoring ride-share events, computing trip metrics, and detecting potential fraud—built with Kafka, Spark, PostgreSQL, and Airflow. Includes a Streamlit dashboard to visualize events and alerts.

---

## 📐 Architecture

```text
┌─────────┐          ┌─────────┐          ┌───────────┐
│Producer │──Kafka──>│ Spark   │──JDBC───>│ PostgreSQL│
│ (Python)│          │Streaming│          │  trip_events
└─────────┘          └─────────┘          └────┬──────┘
                                                │
                                                │ batch
                                                │ run
                                                ▼
                                         ┌───────────────┐
                                         │ fraud_detector│──┐
                                         │ & metrics     │  │ writes
                                         └───────────────┘  ▼
                                               │      ┌──────────────┐
                                               └─────>│ trip_alerts  │
                                                      └──────────────┘
                                                      ┌──────────────┐
                                                      │ trip_metrics │
                                                      └──────────────┘
                                                      ┌──────────────┐
                                                      │ Streamlit    │
                                                      │ dashboard    │
                                                      └──────────────┘
🚀 Quick Start
 1. Clone and configure
git clone https://github.com/kacperguzydev/TripSafe-Analytics.git
cd TripSafe-Analytics
cp .env.example .env
 Edit .env with your local paths / credentials
 2. Prepare your database
python database.py
 3. Run the producer
python kafka/producer.py
 4. Launch Spark streaming
python streaming.py
 5. (Optional) Send test fraud events
python fraud_test.py
 6. Fire up your DAGs
Copy dags/tripsafe_pipeline.py into your Airflow dags/ folder.
 7. View the dashboard
streamlit run dashboard.py
⚙️ Components
producer.py & fraud_test.py
Simulate ride events and inject controlled “fraud” scenarios.

streaming.py
Reads from Kafka, parses JSON, writes raw events into trip_events.

fraud_detector.py
Batch job that truncates trip_alerts, applies static‐GPS, short-trip, and location-jump rules, dedupes, then writes alerts.

trip_metrics.py
Batch job that computes daily city-level total trips, average duration, and cancellations into trip_metrics.

monitoring.py
Exposes Prometheus metrics (e.g. event counts, processing rates) on port 800X.

dashboard.py
Streamlit app to browse events, metrics, and alerts in real time.

database.py
Helpers to create the tripsafe database and schemas on PostgreSQL.

dags/tripsafe_pipeline.py
Orchestrates fraud & metrics jobs via Airflow every 5 minutes.

🔧 Configuration
All settings live in config.py and overridable via environment variables in .env.

DB_CONFIG: PostgreSQL host, port, user, password.

KAFKA_CONFIG: bootstrap servers & topic name.

CITIES, EVENT_TYPES: fixtures for producer.
