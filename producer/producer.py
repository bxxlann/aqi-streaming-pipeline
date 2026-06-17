"""
AQI Producer — fetches air quality data from OpenAQ API and publishes to Redpanda.

Flow: OpenAQ REST API → JSON message → Kafka topic "aqi-raw"

Each message is one sensor reading: location, parameter (pm25/pm10/o3...), value, timestamp.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
OPENAQ_API_KEY = os.environ.get("OPENAQ_API_KEY", "")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))
TOPIC = "aqi-raw"

# Almaty, Kazakhstan — location IDs from OpenAQ
# We fetch the list dynamically but keep a fallback for offline resilience
ALMATY_SEARCH_PARAMS = {
    "country_id": "KZ",
    "city": "Almaty",
    "limit": 20,
}

HEADERS = {"X-API-Key": OPENAQ_API_KEY} if OPENAQ_API_KEY else {}


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 1000,
    })


def delivery_callback(err, msg):
    if err:
        log.error("Delivery failed for key %s: %s", msg.key(), err)
    else:
        log.debug("Delivered → %s [%d]", msg.topic(), msg.partition())


def fetch_almaty_location_ids() -> list[int]:
    """Discover Almaty station IDs from OpenAQ."""
    url = "https://api.openaq.org/v3/locations"
    try:
        resp = requests.get(url, params=ALMATY_SEARCH_PARAMS, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        ids = [loc["id"] for loc in data.get("results", [])]
        log.info("Found %d Almaty locations", len(ids))
        return ids
    except Exception as exc:
        log.warning("Could not fetch location list: %s", exc)
        return []


def fetch_latest_measurements(location_ids: list[int]) -> list[dict]:
    """Fetch the most recent measurement for each location."""
    if not location_ids:
        return []

    readings = []
    for loc_id in location_ids:
        url = f"https://api.openaq.org/v3/locations/{loc_id}/latest"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            location_meta = data.get("results", [{}])[0] if data.get("results") else {}
            loc_name = location_meta.get("name", f"location-{loc_id}")
            loc_city = location_meta.get("locality", "Almaty")
            loc_country = location_meta.get("country", {}).get("code", "KZ")
            coords = location_meta.get("coordinates", {})

            for sensor in location_meta.get("sensors", []):
                latest = sensor.get("latest", {})
                if not latest.get("value"):
                    continue

                readings.append({
                    "location_id": loc_id,
                    "location_name": loc_name,
                    "city": loc_city,
                    "country": loc_country,
                    "parameter": sensor.get("parameter", {}).get("name", "unknown"),
                    "value": float(latest["value"]),
                    "unit": sensor.get("parameter", {}).get("units", "µg/m³"),
                    "latitude": coords.get("latitude", 43.238),
                    "longitude": coords.get("longitude", 76.945),
                    "measured_at": latest.get("datetime", datetime.now(timezone.utc).isoformat()),
                    "producer_ts": datetime.now(timezone.utc).isoformat(),
                })
        except Exception as exc:
            log.warning("Failed to fetch location %d: %s", loc_id, exc)

    return readings


def simulate_almaty_readings() -> list[dict]:
    """
    Fallback: generate realistic synthetic readings when the API is unavailable
    or no API key is configured. Values mimic a typical Almaty winter day.
    """
    import math
    import random

    now = datetime.now(timezone.utc)
    # Winter smog is worse at night — simple sinusoidal pattern
    hour = now.hour
    base_pm25 = 45 + 30 * math.sin(math.pi * (hour - 6) / 12)

    stations = [
        (1001, "Almaty-Center",     "Almaty", 43.2565, 76.9285),
        (1002, "Almaty-Alatau",     "Almaty", 43.2117, 76.8489),
        (1003, "Almaty-Bostandyk",  "Almaty", 43.2456, 76.9003),
    ]

    readings = []
    for loc_id, loc_name, city, lat, lon in stations:
        pm25 = max(0.0, base_pm25 + random.gauss(0, 8))
        pm10 = pm25 * 1.6 + random.gauss(0, 5)

        for param, value, unit in [("pm25", pm25, "µg/m³"), ("pm10", pm10, "µg/m³")]:
            readings.append({
                "location_id": loc_id,
                "location_name": loc_name,
                "city": city,
                "country": "KZ",
                "parameter": param,
                "value": round(value, 2),
                "unit": unit,
                "latitude": lat,
                "longitude": lon,
                "measured_at": now.isoformat(),
                "producer_ts": now.isoformat(),
                "simulated": True,
            })
    return readings


def run():
    producer = make_producer()
    location_ids: list[int] = []
    location_refresh_counter = 0

    log.info("Producer started. Broker=%s  Topic=%s  Interval=%ds", KAFKA_BROKER, TOPIC, POLL_INTERVAL)

    while True:
        # Refresh location list every 10 polls (~10 minutes)
        if location_refresh_counter == 0:
            location_ids = fetch_almaty_location_ids()

        location_refresh_counter = (location_refresh_counter + 1) % 10

        if location_ids and OPENAQ_API_KEY:
            readings = fetch_latest_measurements(location_ids)
        else:
            if not OPENAQ_API_KEY:
                log.info("No API key — using simulated data (set OPENAQ_API_KEY for real data)")
            readings = simulate_almaty_readings()

        for reading in readings:
            key = f"{reading['location_id']}:{reading['parameter']}"
            producer.produce(
                topic=TOPIC,
                key=key.encode(),
                value=json.dumps(reading).encode(),
                callback=delivery_callback,
            )

        producer.flush()
        log.info("Published %d readings", len(readings))
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
