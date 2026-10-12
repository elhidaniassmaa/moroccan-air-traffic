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
        Batch["Weather Ingestion\n(Parquet partitioned)"]
    end

    subgraph Streaming ["Streaming & Buffer (Redpanda)"]
        TopicRaw[("Topic flights.raw\n3 partitions / clé=icao24")]
        TopicDLQ[("Topic flights.dlq\n1 partition / compact")]
    end

    subgraph Lakehouse ["Stockage Médaillon (Delta Lake)"]
        Bronze[("Bronze Delta Lake\nWatermark 10 min + dédup")]
        Silver[("Silver Delta Lake\nEnrichi Nearest Airport & Phases")]
        RawWeather[("Raw Weather Parquet\ndata/lake/raw/weather")]
    end

    subgraph Analytics ["Transformation & Modélisation (dbt + DuckDB)"]
        Staging["Staging (stg_flights, stg_weather, seeds)"]
        Intermediate["Intermediate (int_flights_near_airport, phases, weather)"]
        Marts["Marts Kimball\n(fct_movements_hourly, dim_airport, dim_date)"]
    end

    subgraph Orchestration ["Orchestration Batch (Airflow)"]
        DAG1["DAG weather_daily\nIngestion + dbt weather"]
        DAG2["DAG dbt_hourly\nBuild incrémental marts"]
    end

    subgraph Consumption ["Restitution & Dashboard"]
        Streamlit["Dashboard Streamlit\n(pydeck 3D + Plotly)"]
    end

    OS -->|Poll 15s bbox Maroc| Prod
    Prod -->|Validé JSON Schema| TopicRaw
    Prod -->|Rejeté schéma| TopicDLQ

    TopicRaw -->|Structured Streaming| Bronze
    Bronze -->|Enrichissement haversine & UDFs| Silver

    OM -->|Fetch journalier| Batch
    Batch --> RawWeather

    Silver --> Staging
    RawWeather --> Staging

    Staging --> Intermediate --> Marts

    DAG1 -.->|Trigger| Batch
    DAG2 -.->|Trigger| Marts

    Marts --> Streamlit
```

## 2. Principes & Décisions Techniques Clés

1. **Partitionnement Kafka par `icao24`** : Garantit que tous les états successifs d'un même aéronef arrivent dans la même partition, permettant la détection séquentielle d'états de vol sans reshuffle inter-partitions.
2. **Architecture Médaillon Delta Lake** :
   - **Bronze** : Stockage append-only des événements bruts validés, partitionné par `event_date`, avec déduplication sur `(icao24, time_position)` encadrée par un watermark de 10 minutes.
   - **Silver** : Calcul de proximité aux aéroports clés (CMN, RAK, RBA, AGA, TNG) via distance Haversine, classification heuristique des phases de vol (au sol, décollage, montée, croisière, approche, atterrissage).
3. **Modélisation Kimball avec dbt** :
   - Modèle incrémental `fct_airport_movements_hourly` avec stratégie `delete+insert` sur clé primaire surrogate MD5 `movement_hour_id`.
   - Snapshot SCD2 `snap_dim_airline` pour le suivi des évolutions de flotte ou compagnies.
4. **Découplage Strict Streaming vs Airflow** :
   - Airflow orchestre uniquement les jobs périodiques finis (météo, dbt hourly incrémental).
   - Les moteurs de streaming tournent en démons autonomes résilients.
