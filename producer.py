import logging
import time
import uuid
import random
import json
from datetime import datetime, timedelta

from kafka import KafkaProducer
from config import KAFKA_CONFIG, CITIES
from monitoring import start_metrics_server, messages_sent, send_latency

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("producer")

start_metrics_server(8001)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
    value_serializer=lambda v: json.dumps(v).encode()
)

def send_trip(trip_id, rider_id, driver_id, city, start_ts, end_ts, start_loc, end_loc, fraud=None):
    tag = f" [{fraud}]" if fraud else ""
    for etype, ts, loc in [
        ("trip_started", start_ts, start_loc),
        ("trip_ended", end_ts, end_loc)
    ]:
        payload = {
            "event_type": etype,
            "trip_id": trip_id,
            "rider_id": rider_id,
            "driver_id": driver_id,
            "city": city,
            "timestamp": ts.isoformat(),
            "location": loc
        }
        with send_latency.time():
            producer.send(KAFKA_CONFIG["topic"], value=payload)
            producer.flush()
        messages_sent.inc()
        logger.info(f"Sent {etype} for {trip_id}{tag}")

def run():
    logger.info("Starting producer (5% fraud)…")
    while True:
        trip_id    = str(uuid.uuid4())
        rider_id   = str(uuid.uuid4())
        driver_id  = str(uuid.uuid4())
        city       = random.choice(CITIES)
        start_ts   = datetime.utcnow()
        end_ts     = start_ts + timedelta(minutes=random.randint(1, 15))
        lat        = round(random.uniform(-90, 90), 6)
        lon        = round(random.uniform(-180, 180), 6)

        p = random.random()
        if p < 0.025:
            send_trip(trip_id, rider_id, driver_id, city, start_ts, end_ts,
                      {"lat": lat, "lon": lon}, {"lat": lat, "lon": lon},
                      fraud="Static GPS")
        elif p < 0.05:
            send_trip(trip_id, rider_id, driver_id, city, start_ts, end_ts,
                      {"lat": lat, "lon": lon}, {"lat": lat+50, "lon": lon+100},
                      fraud="Unrealistic Jump")
        else:
            send_trip(trip_id, rider_id, driver_id, city, start_ts, end_ts,
                      {"lat": lat, "lon": lon},
                      {"lat": lat + round(random.uniform(-0.01,0.01),6),
                       "lon": lon + round(random.uniform(-0.01,0.01),6)})
        time.sleep(1)

if __name__ == "__main__":
    run()
