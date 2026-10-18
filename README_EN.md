# 🇲🇦 Moroccan Air Traffic Streaming Platform

[![CI Quality & Data Tests](https://github.com/elhidaniassmaa/moroccan-air-traffic/actions/workflows/ci.yml/badge.svg)](https://github.com/elhidaniassmaa/moroccan-air-traffic/actions)
![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)
![Kafka](https://img.shields.io/badge/streaming-Apache%20Kafka%203.6%20%2B%20ZooKeeper-orange.svg)
![Spark](https://img.shields.io/badge/processing-Spark%203.5.1%20%2B%20Delta%203.1.0-red.svg)
![PostgreSQL DW](https://img.shields.io/badge/warehouse-PostgreSQL%2015%20DW-blue.svg)
![dbt Multi-Target](https://img.shields.io/badge/modeling-dbt--postgres%20%7C%20dbt--snowflake-yellow.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

[🇫🇷 Passer à la version française](README.md)

An end-to-end real-time and analytical data engineering platform for monitoring Moroccan airspace (Casablanca CMN, Marrakech RAK, Rabat RBA, Agadir AGA, Tangier TNG).  
The system ingests live flight trajectories, normalizes them in a Delta Lake Lakehouse, loads them into a Kimball-modeled PostgreSQL Data Warehouse using dbt, and visualizes insights on an interactive 3D Streamlit dashboard.

---

## 📸 Platform Overview

| Streamlit 3D Dashboard (Live & Warehouse Data) | Kafka UI Cluster Overview |
| :---: | :---: |
| ![Streamlit Dashboard](docs/images/dashboard.png) | ![Kafka UI](docs/images/kafka_ui.png) |

| Apache Airflow Orchestration |
| :---: |
| ![Apache Airflow](docs/images/airflow_dag.png) |

---

## 🎯 Business Context & Data Scope

The platform is **designed to analyze** how air traffic over Morocco interacts with:
- **Severe weather conditions** (high winds, gusts > 50 km/h, visibility < 2 km, precipitation) via the Open-Meteo API.
- **Calendar & religious cycles** (Ramadan, Eid al-Fitr, seasonal vacation shifts).
- **Major sports mega-events** (FIFA World Cup 2030).

> ℹ️ **Timeline Scope & Methodological Rigor:**  
> - **Active flight data:** Live collection started in **October 2026**. Available operational history reflects this observation window.
> - **Calendar indicators:** The date dimension (`dim_date`) is pre-computed with markers for key historical and upcoming events (Ramadan, Eid, World Cup 2030) to support trend analyses as data accumulates over time.
> - **Operational metrics:** Because the OpenSky Network API does not provide scheduled commercial timetables, the platform does not fabricate official delays. Instead, it computes **rigorous operational proxies**: hourly distinct aircraft density, geodesic flight phase heuristic classification (climb, cruise, descent, approach), and activity variance against historical medians.

---

## 🏛️ System Architecture

```mermaid
flowchart TD
    subgraph Ingestion ["1. Real-Time & Batch Ingestion"]
        A[OpenSky REST API / Synthetic] -->|Poll 15s - JSON Schema Draft 2020-12| B[Python Producer confluent-kafka]
        B -->|Validation OK| C[(Topic flights.raw<br/>3 partitions - key=icao24)]
        B -->|Validation KO| D[(Topic flights.dlq<br/>Dead Letter Queue)]
        W[Open-Meteo API] -->|Hourly Airport Weather| WH[ingestion.batch.weather]
    end

    subgraph Streaming ["2. Lakehouse Streaming (Spark + Delta)"]
        C -->|Structured Streaming - Watermark 10min| E[(Delta Lake Bronze<br/>data/lake/bronze/flights)]
        E -->|Haversine Enrichment + Phase Detection| F[(Delta Lake Silver<br/>data/lake/silver/flights)]
    end

    subgraph Warehouse ["3. Relational Data Warehouse"]
        F -->|PyArrow / PySpark Loader| G[(PostgreSQL Warehouse<br/>flights_dw.silver_flights)]
        WH -->|Direct Sync| H[(PostgreSQL Warehouse<br/>flights_dw.raw_weather)]
    end

    subgraph Transformation ["4. Dimensional Modeling (dbt)"]
        G & H --> I[dbt Staging Models<br/>stg_flights, stg_weather, stg_airports]
        I --> J[dbt Intermediate Models<br/>int_flights_near_airport, int_weather_hourly]
        J --> K[dbt Kimball Marts - Star Schema<br/>fct_airport_movements_hourly<br/>mart_traffic_weather_impact<br/>dim_date, dim_airport, dim_airline]
    end

    subgraph Analytics ["5. Visualization & Orchestration"]
        K --> L[Streamlit 3D Dashboard<br/>Direct query on flights_dw]
        M[Apache Airflow 2.8.4] -->|DAG dbt_hourly| G
        M -->|Trigger dbt seed, run, test| I
    end
```

---

## 🛠️ Technology Stack

| Layer | Component | Operational Role |
| :--- | :--- | :--- |
| **Message Broker** | Apache Kafka 3.6 (Confluent 7.6.1) | Distributed persistent message stream, keyed by `icao24`. |
| **Cluster Coordination** | Apache ZooKeeper 3.8 | Consensus, controller election, and partition state. |
| **Stream Processing** | Apache Spark 3.5.1 (PySpark) | Structured Streaming with fault-tolerant temporal deduplication (`watermark 10 min`). |
| **Lakehouse Storage** | Delta Lake 3.1.0 | ACID table format for Bronze and Silver tiers on disk. |
| **Data Warehouse** | PostgreSQL 15 | Dedicated relational warehouse (`flights_dw`) hosting the analytical Gold tier. |
| **Data Transformation** | dbt (Data Build Tool) | Kimball star schema modeling, data contracts, and automated schema tests. |
| **Orchestration** | Apache Airflow 2.8.4 | Scheduling batch ingestion, warehouse synchronization, and dbt execution. |
| **BI & Visualization** | Streamlit + pydeck | Interactive 3D spatial flight tracking connected directly to `flights_dw`. |
| **Testing & Quality** | Pytest & Ruff | Unit/integration test suite, JSON Schema contract enforcement, PEP-8 compliance. |

---

## 🌐 Services & Web Endpoints

| Service | Local URL / Port | Credentials | Purpose |
| :--- | :--- | :--- | :--- |
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) | *No auth* | 3D Pydeck flight map, hourly throughput, and weather correlation |
| **Kafka UI** | [http://localhost:8080](http://localhost:8080) | *No auth* | Monitoring topics (`flights.raw`, `flights.dlq`) and consumer lag |
| **Airflow Webserver** | [http://localhost:8085](http://localhost:8085) | `admin` / `admin` | DAG management (`dbt_hourly`, `weather_daily`) |
| **PostgreSQL DW** | `localhost:5433` | `flights_user` / `flights_password_dev` | Analytical database `flights_dw` (DBeaver / pgAdmin) |
| **Kafka Direct Broker** | `localhost:9092` / `localhost:19092` | *No auth (dev)* | Direct streaming access |

---

## 🚀 Quickstart Guide

### Prerequisites
- Docker and Docker Compose
- Python 3.11+

### 1. Environment Setup
```bash
cp .env.example .env
```
*(By default, `PRODUCER_SYNTHETIC_MODE=true` allows running the full pipeline without external credentials. Add your OpenSky credentials to `.env` and set `PRODUCER_SYNTHETIC_MODE=false` for live data).*

### 2. Start Infrastructure
```bash
docker compose up -d
```
Verify container health:
```bash
docker compose ps
```

### 3. Run Warehouse Sync & dbt Marts
```bash
# Sync Delta Lake Silver into PostgreSQL DW
docker compose exec airflow-webserver python -m ingestion.batch.silver_to_postgres

# Seed reference data, run models, and execute dbt data tests
docker compose exec airflow-webserver dbt seed --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
docker compose exec airflow-webserver dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
docker compose exec airflow-webserver dbt test --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
```

To verify dbt directly from Windows PowerShell (from the parent `Projet_2` directory), point it to the repository's project and profile. This avoids using an unrelated global `~/.dbt/profiles.yml`:
```powershell
$env:WAREHOUSE_HOST = "localhost"
$env:WAREHOUSE_PORT = "5433"
dbt debug --project-dir .\moroccan-air-traffic\dbt_project --profiles-dir .\moroccan-air-traffic\dbt_project
```

### 4. Run Test Suite
```bash
python -m pytest tests/
```

---

## 📐 Dimensional Modeling (dbt Kimball Star Schema)

The warehouse database `flights_dw` is divided into structured schemas:

```text
flights_dw
├── public (Raw tables loaded from Delta Lake & Open-Meteo)
│   ├── silver_flights
│   └── raw_weather
│
├── public_staging (Cleaned, strictly-typed numeric/timestamps, deduplicated)
│   ├── stg_flights
│   ├── stg_weather
│   ├── stg_airports (ICAO/IATA seed of Moroccan international hubs)
│   └── stg_airlines (Airline seed)
│
├── public_intermediate (Geodesic math and hourly aggregations)
│   ├── int_flights_near_airport (Haversine distance < 50 km)
│   ├── int_flight_phases (Climb, cruise, descent, approach heuristic classification)
│   └── int_weather_hourly (Normalized hourly weather metrics)
│
└── public_marts (Kimball Star Schema Fact & Dimension Tables)
    ├── dim_airport (Coordinates, ICAO/IATA codes, primary runways)
    ├── dim_date (Date dimension enriched with holiday & mega-event flags)
    ├── dim_airline (Airline dimension with SCD Type 2 snapshot)
    ├── fct_airport_movements_hourly (Hourly distinct movements by airport)
    ├── fct_traffic_density (Aircraft density by altitude bracket)
    └── mart_traffic_weather_impact (Final traffic vs weather severity mart)
```

### Multi-Target Portability (PostgreSQL & Snowflake)
> ℹ️ **Status:** Models are **designed to be portable, tested on PostgreSQL**.  
> A Snowflake profile is pre-configured in [`profiles.yml`](dbt_project/profiles.yml). Cross-dialect portability is achieved through ANSI-compliant date functions (`EXTRACT(DOW FROM ...)`, `TO_CHAR(...)`) and explicit `numeric` casting for floating-point calculations.

---

## 🔮 Roadmap & Future Enhancements

1. **Predictive Machine Learning**: Train an XGBoost model on `mart_traffic_weather_impact` to forecast airport congestion given weather forecasts.
2. **Prometheus & Grafana Observability**: Instrument Kafka consumer lag and Spark micro-batch latency.
3. **Apache Iceberg Table Format**: Evaluate Apache Iceberg integration alongside Delta Lake.

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
