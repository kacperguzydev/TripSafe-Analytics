import logging
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType
from monitoring import (
    start_metrics_server,
    batches_processed,
    batch_duration,
    rows_in_batch
)
from config import KAFKA_CONFIG, DB_CONFIG

os.environ["HADOOP_HOME"] = "C:\\hadoop"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("streaming")
start_metrics_server(8002)
logger.info("Streaming metrics on :8002")

schema = StructType() \
    .add("event_type", StringType()) \
    .add("trip_id", StringType()) \
    .add("rider_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("city", StringType()) \
    .add("timestamp", StringType()) \
    .add("location", StructType()
         .add("lat", DoubleType())
         .add("lon", DoubleType())
    )

spark = (
    SparkSession.builder
    .appName("TripSafeKafkaToPostgres")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
        "org.postgresql:postgresql:42.2.18"
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap_servers"]) \
    .option("subscribe", KAFKA_CONFIG["topic"]) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select(
        col("data.event_type"),
        col("data.trip_id"),
        col("data.rider_id"),
        col("data.driver_id"),
        col("data.city"),
        to_timestamp(col("data.timestamp")).alias("timestamp"),
        col("data.location.lat").alias("lat"),
        col("data.location.lon").alias("lon")
    )

def write_batch(batch_df, batch_id):
    start = time.time()
    rows = batch_df.count()
    rows_in_batch.set(rows)

    (
        batch_df.write
        .format("jdbc")
        .option(
            "url",
            f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/"
            f"{DB_CONFIG['database']}"
        )
        .option("dbtable", "trip_events")
        .option("user", DB_CONFIG["user"])
        .option("password", DB_CONFIG["password"])
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    batches_processed.inc()
    duration = time.time() - start
    batch_duration.observe(duration)
    logger.info(f"Batch {batch_id}: wrote {rows} rows in {duration:.2f}s")

(
    df_parsed.writeStream
    .foreachBatch(write_batch)
    .outputMode("append")
    .start()
    .awaitTermination()
)
