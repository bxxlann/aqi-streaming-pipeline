"""
AQI Stream Processor

Hot path:  Kafka → AQI calculation → ClickHouse → Slack alert (if threshold exceeded)
Cold path: Kafka → buffer → Parquet file (hourly flush, partitioned by date)

AQI is calculated using the US EPA linear interpolation formula from PM2.5 concentration.
"""

import json
import logging
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from clickhouse_driver import Client
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [processor] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BROKER        = os.environ["KAFKA_BROKER"]
CLICKHOUSE_HOST     = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT     = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_DB       = os.environ.get("CLICKHOUSE_DB", "aqi")
SLACK_WEBHOOK_URL   = os.environ.get("SLACK_WEBHOOK_URL", "")
ALERT_THRESHOLD     = int(os.environ.get("AQI_ALERT_THRESHOLD", "150"))
COLD_PATH_DIR       = Path(os.environ.get("COLD_PATH_DIR", "/data/cold-path"))
TOPIC               = "aqi-raw"
CONSUMER_GROUP      = "aqi-processor-v1"
COLD_FLUSH_INTERVAL = 300  # write Parquet every 5 minutes

# EPA PM2.5 → AQI breakpoints: (C_low, C_high, AQI_low, AQI_high)
PM25_BREAKPOINTS = [
    (0.0,   12.0,   0,   50),
    (12.1,  35.4,  51,  100),
    (35.5,  55.4, 101,  150),
    (55.5, 150.4, 151,  200),
    (150.5, 250.4, 201, 300),
    (250.5, 350.4, 301, 400),
    (350.5, 500.4, 401, 500),
]

AQI_CATEGORIES = [
    (50,  "Good"),
    (100, "Moderate"),
    (150, "Unhealthy for Sensitive Groups"),
    (200, "Unhealthy"),
    (300, "Very Unhealthy"),
    (500, "Hazardous"),
]


def pm25_to_aqi(concentration: float) -> int:
    """Convert PM2.5 µg/m³ to AQI using EPA linear interpolation."""
    c = round(concentration, 1)
    for c_lo, c_hi, aqi_lo, aqi_hi in PM25_BREAKPOINTS:
        if c_lo <= c <= c_hi:
            return round((aqi_hi - aqi_lo) / (c_hi - c_lo) * (c - c_lo) + aqi_lo)
    return 500  # beyond hazardous


def aqi_category(aqi: int) -> str:
    for threshold, label in AQI_CATEGORIES:
        if aqi <= threshold:
            return label
    return "Hazardous"


def enrich(reading: dict) -> dict:
    """Add AQI fields to a reading. Only PM2.5 → AQI conversion is standardized."""
    param = reading.get("parameter", "").lower()
    value = reading.get("value", 0.0)

    if param == "pm25":
        aqi = pm25_to_aqi(value)
    else:
        aqi = 0  # AQI not calculated for other parameters in this pipeline

    return {
        **reading,
        "aqi": aqi,
        "aqi_category": aqi_category(aqi) if aqi > 0 else "N/A",
    }


# ── ClickHouse ─────────────────────────────────────────────────────────────────
def make_ch_client() -> Client:
    # Connect without specifying DB first, ensure schema exists, then reconnect
    for attempt in range(30):
        try:
            client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
            client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
            client.execute(f"""
                CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.readings (
                    location_id   UInt32,
                    location_name String,
                    city          String,
                    country       String,
                    parameter     String,
                    value         Float32,
                    unit          String,
                    aqi           UInt16,
                    aqi_category  String,
                    latitude      Float64,
                    longitude     Float64,
                    measured_at   DateTime,
                    ingested_at   DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(measured_at)
                ORDER BY (location_id, measured_at)
                TTL measured_at + INTERVAL 1 YEAR
            """)
            log.info("ClickHouse ready")
            return Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)
        except Exception as exc:
            log.warning("ClickHouse not ready (attempt %d/30): %s", attempt + 1, exc)
            time.sleep(3)
    raise RuntimeError("Could not connect to ClickHouse after 30 attempts")


def insert_to_clickhouse(client: Client, rows: list[dict]):
    if not rows:
        return
    data = [
        {
            "location_id":   r["location_id"],
            "location_name": r["location_name"],
            "city":          r["city"],
            "country":       r["country"],
            "parameter":     r["parameter"],
            "value":         r["value"],
            "unit":          r["unit"],
            "aqi":           r["aqi"],
            "aqi_category":  r["aqi_category"],
            "latitude":      r["latitude"],
            "longitude":     r["longitude"],
            "measured_at":   datetime.fromisoformat(r["measured_at"].replace("Z", "+00:00")),
        }
        for r in rows
    ]
    client.execute(
        """INSERT INTO aqi.readings
           (location_id, location_name, city, country, parameter,
            value, unit, aqi, aqi_category, latitude, longitude, measured_at)
           VALUES""",
        data,
    )
    log.info("Inserted %d rows into ClickHouse", len(rows))


# ── Slack alerts ───────────────────────────────────────────────────────────────
# Track last alert time per location to avoid spam (1 alert per location per hour)
_last_alert: dict[int, datetime] = {}


def maybe_send_slack_alert(reading: dict):
    if not SLACK_WEBHOOK_URL:
        return
    if reading["aqi"] < ALERT_THRESHOLD:
        return

    loc_id = reading["location_id"]
    now = datetime.now(timezone.utc)
    last = _last_alert.get(loc_id)
    if last and (now - last).seconds < 3600:
        return

    _last_alert[loc_id] = now

    emoji = "🔴" if reading["aqi"] > 200 else "🟠"
    text = (
        f"{emoji} *AQI Alert — {reading['location_name']}* ({reading['city']})\n"
        f"PM2.5: *{reading['value']} {reading['unit']}*  →  "
        f"AQI *{reading['aqi']}* ({reading['aqi_category']})\n"
        f"Threshold: {ALERT_THRESHOLD}  |  Time: {reading['measured_at']}"
    )
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=5)
        resp.raise_for_status()
        log.info("Slack alert sent for location %d (AQI=%d)", loc_id, reading["aqi"])
    except Exception as exc:
        log.warning("Slack alert failed: %s", exc)


# ── Cold path (Parquet) ────────────────────────────────────────────────────────
PARQUET_SCHEMA = pa.schema([
    pa.field("location_id",   pa.int32()),
    pa.field("location_name", pa.string()),
    pa.field("city",          pa.string()),
    pa.field("country",       pa.string()),
    pa.field("parameter",     pa.string()),
    pa.field("value",         pa.float32()),
    pa.field("unit",          pa.string()),
    pa.field("aqi",           pa.int16()),
    pa.field("aqi_category",  pa.string()),
    pa.field("latitude",      pa.float64()),
    pa.field("longitude",     pa.float64()),
    pa.field("measured_at",   pa.string()),
])


def flush_cold_path(buffer: list[dict]):
    """Write buffered readings to a date-partitioned Parquet file."""
    if not buffer:
        return

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")

    out_dir = COLD_PATH_DIR / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"hour={hour_str}.parquet"

    table = pa.table(
        {field.name: [r.get(field.name, None) for r in buffer] for field in PARQUET_SCHEMA},
        schema=PARQUET_SCHEMA,
    )

    # Append to existing file if it already exists for this hour
    if out_file.exists():
        existing = pq.read_table(out_file)
        table = pa.concat_tables([existing, table])

    pq.write_table(table, out_file, compression="snappy")
    log.info("Cold path: wrote %d rows → %s", len(buffer), out_file)


# ── Main consumer loop ─────────────────────────────────────────────────────────
def run():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "session.timeout.ms": 30000,
    })
    consumer.subscribe([TOPIC])

    ch_client = make_ch_client()
    cold_buffer: list[dict] = []
    last_cold_flush = datetime.now(timezone.utc)

    log.info(
        "Processor started. Broker=%s  Topic=%s  AQI threshold=%d",
        KAFKA_BROKER, TOPIC, ALERT_THRESHOLD,
    )

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Kafka error: %s", msg.error())
            else:
                try:
                    raw = json.loads(msg.value().decode())
                    reading = enrich(raw)

                    # Hot path: ClickHouse + optional Slack alert
                    insert_to_clickhouse(ch_client, [reading])
                    if reading["parameter"] == "pm25":
                        maybe_send_slack_alert(reading)

                    # Buffer for cold path
                    cold_buffer.append(reading)

                except Exception as exc:
                    log.error("Failed to process message: %s — %s", msg.value(), exc)

            # Flush cold path on interval
            now = datetime.now(timezone.utc)
            if (now - last_cold_flush).seconds >= COLD_FLUSH_INTERVAL:
                flush_cold_path(cold_buffer)
                cold_buffer.clear()
                last_cold_flush = now

    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        flush_cold_path(cold_buffer)
        consumer.close()


if __name__ == "__main__":
    run()
