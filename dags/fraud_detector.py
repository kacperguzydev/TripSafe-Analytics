import logging, os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, abs as abs_, unix_timestamp
from config import DB_CONFIG
from monitoring import start_metrics_server, fraud_static, fraud_short, fraud_jump

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fraud")

os.environ["HADOOP_HOME"] = "C:\\hadoop"
start_metrics_server(8003)

# reset alerts
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("TRUNCATE trip_alerts RESTART IDENTITY CASCADE")
    conn.commit()
    cur.close()
    conn.close()
    logger.info("trip_alerts cleared.")
except Exception as e:
    logger.error("Could not clear alerts: %s", e)

spark = (
    SparkSession.builder
    .appName("FraudDetector")
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

s = df.filter(col("event_type") == "trip_started") \
      .selectExpr("trip_id AS s_id", "driver_id AS drv", "timestamp AS s_ts", "lat AS s_lat", "lon AS s_lon")
e = df.filter(col("event_type") == "trip_ended") \
      .selectExpr("trip_id AS e_id", "timestamp AS e_ts", "lat AS e_lat", "lon AS e_lon")

joined = s.join(e, s.s_id == e.e_id)

alerts = []

# static
stat = joined.filter((col("s_lat")==col("e_lat")) & (col("s_lon")==col("e_lon")))
cnt = stat.count()
if cnt:
    fraud_static.inc(cnt)
    alerts.append(
        stat.select(
            col("s_id").alias("trip_id"),
            col("drv").alias("driver_id"),
            lit("Static GPS").alias("alert_type"),
            lit("Started & ended same place").alias("description"),
            current_timestamp().alias("timestamp")
        )
    )
    logger.info(f"{cnt} static-GPS frauds")

# short
short = joined.filter(unix_timestamp("e_ts")-unix_timestamp("s_ts") < 60)
cnt = short.count()
if cnt:
    fraud_short.inc(cnt)
    alerts.append(
        short.select(
            col("s_id").alias("trip_id"),
            col("drv").alias("driver_id"),
            lit("Short Trip").alias("alert_type"),
            lit("<1 min duration").alias("description"),
            current_timestamp().alias("timestamp")
        )
    )
    logger.info(f"{cnt} short-trip frauds")

# jump
jump = joined.filter(
    (abs_(col("s_lat")-col("e_lat")) > 1) |
    (abs_(col("s_lon")-col("e_lon")) > 1)
)
cnt = jump.count()
if cnt:
    fraud_jump.inc(cnt)
    alerts.append(
        jump.select(
            col("s_id").alias("trip_id"),
            col("drv").alias("driver_id"),
            lit("Sudden Jump").alias("alert_type"),
            lit("Unrealistic jump").alias("description"),
            current_timestamp().alias("timestamp")
        )
    )
    logger.info(f"{cnt} jump frauds")

if alerts:
    df_all = alerts[0]
    for dfb in alerts[1:]:
        df_all = df_all.union(dfb)
    final = df_all.dropDuplicates(["trip_id","alert_type"])
    if final.count():
        (final.write
              .format("jdbc")
              .option("url", f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
              .option("dbtable", "trip_alerts")
              .option("user", DB_CONFIG["user"])
              .option("password", DB_CONFIG["password"])
              .option("driver", "org.postgresql.Driver")
              .mode("append")
              .save())
        logger.info("Alerts saved.")
else:
    logger.info("No fraud found.")
