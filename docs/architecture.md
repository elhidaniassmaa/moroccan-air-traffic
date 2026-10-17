# Architecture Globale — Moroccan Air Traffic Platform

## 1. Vue d'ensemble du Système

La plateforme combine une ingestion continue d'événements aéronautiques (télémétrie ADS-B OpenSky Network) avec des enrichissements contextuels météorologiques et calendaires pour analyser la dynamique du trafic aérien marocain.

```mermaid
flowchart TD
    subgraph Sources ["Sources Externes"]
        OS["API OpenSky Network\n(OAuth2 Bearer /states/all)"]
        OM["API Open-Meteo\n(Prévisions & Archives)"]
    end

    subgraph Ingestion ["Ingestion Temps Réel & Batch"]
        Prod["OpenSky Producer\n(Python / confluent-kafka)"]
        Batch["Weather Ingestion\n(PostgreSQL raw_weather + Parquet)"]
    end

    subgraph Streaming ["Streaming & Buffer (Apache Kafka)"]
        TopicRaw[("Topic flights.raw\n3 partitions / clé=icao24")]
        TopicDLQ[("Topic flights.dlq\n1 partition / compact")]
        KUI["Kafka UI\n(Port 8080)"]
    end

    subgraph Lakehouse ["Stockage Médaillon (Delta Lake)"]
        Bronze[("Bronze Delta Lake\nWatermark 10 min + dédup")]
        Silver[("Silver Delta Lake\nEnrichi Nearest Airport & Phases")]
    end

    subgraph Warehouse ["Data Warehouse Analytique (PostgreSQL)"]
        DW_Silver[("silver_flights\nSynchronisation idempotente")]
        DW_Weather[("raw_weather\nMétéo horaire aéroports")]
        DW_Marts[("Marts Kimball (public_marts)\nfct_airport_movements_hourly\nmart_traffic_weather_impact\ndim_airport, dim_date")]
    end

    subgraph Transformations ["Transformations Analytiques (dbt-postgres)"]
        Staging["Staging (stg_flights, stg_weather, seeds)"]
        Intermediate["Intermediate (int_flights_near_airport, phases, weather)"]
        Marts["Marts Kimball (Marts & Facts)"]
    end

    subgraph Orchestration ["Orchestration Batch & Qualité (Airflow 2.8.4)"]
        DAG1["DAG weather_daily\nIngestion météo + dbt weather"]
        DAG2["DAG dbt_hourly\nSync Silver -> PG + dbt seed/run/test"]
    end

    subgraph Consumption ["Restitution & Dashboard"]
        Streamlit["Dashboard Streamlit\n(SQLAlchemy -> PostgreSQL flights_dw)"]
    end

    OS -->|Poll 15s bbox Maroc| Prod
    Prod -->|Validé JSON Schema| TopicRaw
    Prod -->|Rejeté schéma| TopicDLQ

    TopicRaw -->|Structured Streaming| Bronze
    Bronze -->|Enrichissement haversine & UDFs| Silver

    Silver -->|Spark JDBC / PyArrow| DW_Silver
    OM -->|Fetch journalier| Batch
    Batch --> DW_Weather

    DW_Silver --> Staging
    DW_Weather --> Staging
    Staging --> Intermediate --> Marts
    Marts --> DW_Marts

    DAG2 -.->|Orchestre| Silver
    DAG2 -.->|Déclenche| Transformations
    DAG1 -.->|Trigger| Batch

    DW_Marts --> Streamlit
    DW_Silver --> Streamlit
```

## 2. Choix Technologiques Industriels
1. **Apache Kafka (Confluent Platform 7.6.1)** : Standard d'entreprise pour le découplage et la haute disponibilité des flux de données.
2. **Spark Structured Streaming 3.5.1 + Delta Lake 3.1.0** : Traitement streaming exact-once avec transactions ACID et gestion du watermark.
3. **Data Warehouse PostgreSQL 15 (`flights_dw`)** : Entrepôt analytique relationnel dédié, isolé de la base de métadonnées d'Airflow, permettant des requêtes SQL performantes et standardisées.
4. **dbt (dbt-postgres / dbt-snowflake)** : Modélisation dimensionnelle Kimball avec tests automatisés de qualité (not_null, unique, accepted_values, relationships).
5. **Apache Airflow 2.8.4** : Orchestration robuste des pipelines de chargement et des cycles de transformation dbt.
6. **Streamlit** : Visualisation interactive connectée en direct à PostgreSQL via SQLAlchemy.
