# TripSafe-Analytics# 🚀 TripSafe Analytics

**TripSafe Analytics** is an end-to-end real-time ride-hailing analytics platform.  
It ingests streaming trip events from Kafka, processes them in Spark for both fraud detection and rolling metrics, writes results to PostgreSQL, exposes Prometheus metrics, and visualizes alerts & KPIs in Streamlit. Orchestrated with Apache Airflow and containerized via Docker.

---

## 🗂️ Repository Structure

TripSafe-Analytics/
├── airflow/ # Docker-Compose & Airflow config
│ ├── dags/ # Airflow DAGs (tripsafe_pipeline.py)
│ ├── logs/
│ └── plugins/
├── config.py # central DB & Kafka settings
├── database.py # create DB + tables
├── producer.py # Kafka trip event simulator
├── streaming.py # Spark streaming → trip_events table
├── fraud_detector.py # batch Spark fraud detector (via Airflow DAG)
├── trip_metrics.py # batch Spark metrics job (via Airflow DAG)
├── monitoring.py # Prometheus instrumentation
├── dashboard.py # Streamlit dashboard (alerts & metrics)
└── README.md # you are here

markdown
Copy
Edit

---

## ⚙️ Tech Stack

- **Streaming & Batch:** Apache Spark Structured Streaming & batch  
- **Messaging:** Apache Kafka  
- **Storage:** PostgreSQL  
- **Orchestration:** Apache Airflow (CeleryExecutor + Redis broker)  
- **Monitoring:** Prometheus + Grafana (optional)  
- **Dashboard:** Streamlit  
- **Containerization:** Docker & Docker Compose  
- **Language:** Python 3.8+

---

## 🔧 Prerequisites

1. **Java 8+**  
2. **Python 3.8+** (with virtualenv)  
3. **Docker & Docker Compose**  
4. **Kafka**, **Zookeeper**, **PostgreSQL** (or use bundled Docker services)  

---

## 🚀 Quickstart

1. **Clone & set up**  
   ```bash
   git clone https://github.com/kacperguzydev/TripSafe-Analytics.git
   cd TripSafe-Analytics
   python3 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
Initialize database & tables

bash
Copy
Edit
python database.py
Start Kafka & Zookeeper

bash
Copy
Edit
docker-compose up -d zookeeper kafka
Fire up Spark jobs

Streaming ingestion

bash
Copy
Edit
python streaming.py
Monitoring endpoint

bash
Copy
Edit
python monitoring.py
In another shell:
Fraud detector (batch)

bash
Copy
Edit
python fraud_detector.py
Metrics roll-up (batch)

bash
Copy
Edit
python trip_metrics.py
Run the producer simulator

bash
Copy
Edit
python producer.py
Launch the dashboard

bash
Copy
Edit
streamlit run dashboard.py
(Optional) Orchestrate with Airflow

bash
Copy
Edit
cd airflow
docker-compose up -d
# Place tripsafe_pipeline.py in airflow/dags/
# Visit http://localhost:8080 to trigger DAG
📈 What’s Inside
producer.py – simulates realistic trips + injects 2–5% fraud

streaming.py – Spark Structured Streaming → trip_events table

fraud_detector.py – Spark batch job, applies rules (static GPS, short trip, jump) → trip_alerts

trip_metrics.py – Spark batch job, calculates daily city KPIs → trip_metrics

monitoring.py – exposes Prometheus metrics (/metrics on port 8002)

dashboard.py – Streamlit UI for alerts & metrics

📊 Fraud Rules
Static GPS – start and end at same coordinates

Short Trip – duration < 60 sec

Sudden Jump – lat/lon delta > 1°

📝 Roadmap
✅ Core streaming & persistence

✅ Batch fraud + metrics jobs

✅ Airflow orchestration

✅ Prometheus monitoring

✅ Streamlit dashboard
