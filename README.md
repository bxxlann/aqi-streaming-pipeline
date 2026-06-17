# AQI Streaming Pipeline — Almaty Air Quality Monitor

Real-time air quality monitoring pipeline for Almaty, Kazakhstan.  
Demonstrates a production-grade **Lambda architecture** with hot and cold data paths.

```
OpenAQ API (PM2.5 sensors)
        │
        ▼
   [ Producer ]  ──────────────────────────────────────────────────────────┐
        │  publishes JSON events                                             │
        ▼                                                                    │
  Redpanda (Kafka-compatible)  ←  topic: aqi-raw                            │
        │                                                                    │
        ▼                                                                    │
  [ Processor ]                                                              │
    ├── HOT PATH  ──→  AQI calculation  ──→  ClickHouse  ──→  Grafana dashboard
    │                                    └──→  Slack alert (AQI > 150)      │
    └── COLD PATH ──→  Parquet files (date-partitioned)  ──────────────────┘
                       (historical analytics / data lake)
```

## Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Message broker | [Redpanda](https://redpanda.com/) | Kafka-compatible, simpler ops |
| Stream processor | Python + confluent-kafka | Consume, enrich, route |
| Hot storage | [ClickHouse](https://clickhouse.com/) | OLAP, sub-second queries |
| Visualization | [Grafana](https://grafana.com/) | Real-time dashboard |
| Cold storage | Parquet (Snappy) | Partitioned data lake |
| Alerting | Slack Incoming Webhooks | AQI threshold notifications |
| Data source | [OpenAQ API v3](https://openaq.org/) | Real sensor data |

## Quick Start

### 1. Prerequisites
- Docker + Docker Compose
- (Optional) Free OpenAQ API key from [explore.openaq.org](https://explore.openaq.org/)
- (Optional) Slack Incoming Webhook URL

### 2. Configure
```bash
cp .env.example .env
# Edit .env with your OpenAQ API key and Slack webhook
```

### 3. Run
```bash
docker compose up --build
```

Without an API key the pipeline runs with **realistic simulated data** — useful for local development.

### 4. Access
| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana dashboard | http://localhost:3000 | admin / admin |
| Redpanda console | http://localhost:9644 | — |
| ClickHouse HTTP | http://localhost:8123 | — |

## Key Design Decisions

**Why Redpanda over Kafka?**  
Redpanda is Kafka-API compatible but runs as a single binary — no Zookeeper, no JVM. For local development and small-to-medium workloads it's simpler to operate while being fully compatible with Kafka clients.

**Why ClickHouse for hot storage?**  
ClickHouse is a columnar OLAP database optimised for time-series aggregations. A query like "average AQI per station over the last 6 hours" runs in milliseconds even on millions of rows, making it ideal for Grafana dashboards.

**Hot vs Cold path**  
The hot path (ClickHouse) stores 1 year of data for dashboards and alerts. The cold path (Parquet on disk/S3) stores everything indefinitely for historical analysis and ML training — at a fraction of the storage cost.

**AQI calculation**  
PM2.5 concentration (µg/m³) is converted to the US EPA AQI scale using the standard linear interpolation formula across 7 breakpoint ranges. AQI > 150 triggers a Slack alert.

## Cold Path: Querying Parquet with DuckDB
```bash
pip install duckdb
python -c "
import duckdb
result = duckdb.query(\"SELECT date, avg(aqi) FROM read_parquet('data/cold-path/**/*.parquet', hive_partitioning=true) GROUP BY date ORDER BY date\").df()
print(result)
"
```

## Project Structure
```
├── producer/        # Fetches OpenAQ API → publishes to Redpanda
├── processor/       # Consumes Redpanda → ClickHouse + Parquet + Slack
├── clickhouse/      # DB schema (MergeTree table + materialized view)
├── grafana/         # Auto-provisioned datasource and dashboard
├── data/cold-path/  # Parquet files (gitignored)
└── docker-compose.yml
```
