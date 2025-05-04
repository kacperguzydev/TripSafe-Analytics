import logging
from prometheus_client import start_http_server, Counter, Gauge, Histogram

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("monitoring")

def start_metrics_server(port: int):
    start_http_server(port)
    logger.info(f"Prometheus metrics on :{port}")

# Producer
messages_sent = Counter("producer_messages_total", "Total messages sent")
send_latency = Histogram("producer_send_latency_seconds", "Time to send one message")

# Streaming
batches_processed = Counter("stream_batches_total", "Micro-batches processed")
batch_duration  = Histogram("stream_batch_duration_seconds", "Time per micro-batch")
rows_in_batch   = Gauge("stream_batch_rows", "Rows per micro-batch")

# Fraud detector
fraud_static = Counter("fraud_static_total", "Static-GPS frauds")
fraud_short  = Counter("fraud_short_total", "Short-trip frauds")
fraud_jump   = Counter("fraud_jump_total", "Sudden-jump frauds")

# Metrics job
metrics_runs     = Counter("metrics_job_runs_total", "Metrics jobs run")
metrics_duration = Histogram("metrics_job_duration_seconds", "Metrics job time")
