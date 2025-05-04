import logging, os, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, count, expr
from config import DB_CONFIG
from monitoring import start_metrics_server, metrics_runs, metrics_duration

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("metrics")

os.environ["HADOOP_HOME"] = "C:\\hadoop"
start_metrics_server(8004)
metrics_runs.inc()
t0 = time.time()

spark = (
    SparkSession.builder
    .appName("TripMetricsJob")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}") \
    .option("dbtable", "trip_events") \
    .option("user", DB_CONFIG["user"]) \
    .option("password", DB_CONFIG["password"]) \
    .option("driver", "org.postgresql.Driver") \
    .load()

started = df.filter(col("event_type") == "trip_started") \
    .selectExpr("trip_id", "city", "timestamp as start_ts")
ended   = df.filter(col("event_type") == "trip_ended") \
    .selectExpr("trip_id", "timestamp as end_ts")

joined = (started.join(ended, on="trip_id", how="left")
          .withColumn("trip_duration", (col("end_ts").cast("long") - col("start_ts").cast("long"))/60)
          .withColumn("event_date", to_date("start_ts")))

metrics = joined.groupBy("city", "event_date").agg(
    count("trip_id").alias("total_trips"),
    avg("trip_duration").alias("avg_trip_duration"),
    count(expr("CASE WHEN end_ts IS NULL THEN 1 END")).alias("cancellations")
)

(metrics.write
    .format("jdbc")
    .option("url", f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    .option("dbtable", "trip_metrics")
    .option("user", DB_CONFIG["user"])
    .option("password", DB_CONFIG["password"])
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save())

dt = time.time() - t0
metrics_duration.observe(dt)
logger.info(f"Metrics job finished in {dt:.1f}s")
