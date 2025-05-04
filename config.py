DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "tripsafe",
    "user": "postgres",
    "password": "1234"
}

KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    "topic": "ride-events"
}

CITIES = ["New York", "San Francisco", "Chicago", "Berlin", "London"]
EVENT_TYPES = ["trip_started", "trip_ended"]
