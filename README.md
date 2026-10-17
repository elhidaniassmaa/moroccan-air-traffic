# 🇲🇦 Moroccan Air Traffic Streaming Platform

[![CI Quality & Data Tests](https://github.com/elhidaniassmaa/moroccan-air-traffic/actions/workflows/ci.yml/badge.svg)](https://github.com/elhidaniassmaa/moroccan-air-traffic/actions)
![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)
![Kafka](https://img.shields.io/badge/streaming-Apache%20Kafka%203.6%20%2B%20ZooKeeper-orange.svg)
![Spark](https://img.shields.io/badge/processing-Spark%203.5.1%20%2B%20Delta%203.1.0-red.svg)
![PostgreSQL DW](https://img.shields.io/badge/warehouse-PostgreSQL%2015%20DW-blue.svg)
![dbt Multi-Target](https://img.shields.io/badge/modeling-dbt--postgres%20%7C%20dbt--snowflake-yellow.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

[🇬🇧 Switch to English Version](README_EN.md)

Plateforme de données de bout en bout pour la surveillance temps réel et l'analyse historique de l'espace aérien marocain (Casablanca CMN, Marrakech RAK, Rabat RBA, Agadir AGA, Tanger TNG).  
Le système ingère les trajectoires de vol, les normalise dans un Lakehouse Delta Lake, les charge dans un Data Warehouse PostgreSQL modélisé selon Kimball via dbt, et les visualise sur un tableau de bord interactif Streamlit en 3D.

---

## 📸 Aperçu de la Plateforme

| Tableau de bord Streamlit (Carte 3D & Données Réelles) | Supervision Kafka UI |
| :---: | :---: |
| ![Streamlit Dashboard](docs/images/dashboard.png) | ![Kafka UI](docs/images/kafka_ui.png) |

| Orchestration Apache Airflow |
| :---: |
| ![Apache Airflow](docs/images/airflow_dag.png) |

---

## 🎯 Problématique & Périmètre des Données

La plateforme est **conçue pour analyser** comment le trafic aérien au-dessus du Maroc interagit avec :
- **Les conditions météorologiques défavorables** (vents violents, rafales > 50 km/h, visibilité < 2 km, précipitations) via l'API Open-Meteo.
- **Les cycles calendaires et religieux** (Ramadan, Aïd al-Fitr, départs estivaux des MRE).
- **Les méga-événements sportifs majeurs** (Coupe du Monde FIFA 2030).

> ℹ️ **Périmètre temporel et transparence des données :**  
> - **Données de vol actives :** La collecte en direct a débuté en **octobre 2026**. Les historiques récents reflètent cette fenêtre d'observation.
> - **Indicateurs calendaires :** La dimension temporelle (`dim_date`) est pré-calculée avec les marqueurs historiques et prévisionnels (Ramadan, Aïd, Mondial 2030) afin de permettre les analyses comparatives au fur et à mesure de l'accumulation des données.
> - **Métriques d'exploitation :** L'API OpenSky Network ne fournissant pas les plans de vol commerciaux théoriques, la plateforme ne génère aucun retard artificiel. Elle calcule des **proxies opérationnels rigoureux** : densité horaire d'aéronefs distincts par secteur, détection heuristique des phases de vol (montée, croisière, descente, approche) et variations relatives d'activité.

---

## 🏛️ Architecture du Système

```mermaid
flowchart TD
    subgraph Ingestion ["1. Ingestion Temps Réel & Batch"]
        A[OpenSky REST API / Synthétique] -->|Poll 15s - Schema JSON Draft 2020-12| B[Python Producer confluent-kafka]
        B -->|Validation OK| C[(Topic flights.raw<br/>3 partitions - key=icao24)]
        B -->|Validation KO| D[(Topic flights.dlq<br/>Dead Letter Queue)]
        W[Open-Meteo API] -->|Relevés horaires aéroports| WH[ingestion.batch.weather]
    end

    subgraph Streaming ["2. Lakehouse Streaming (Spark + Delta)"]
        C -->|Structured Streaming - Watermark 10min| E[(Delta Lake Bronze<br/>data/lake/bronze/flights)]
        E -->|Calculs Haversine + Classification Phases| F[(Delta Lake Silver<br/>data/lake/silver/flights)]
    end

    subgraph Warehouse ["3. Data Warehouse Relationnel"]
        F -->|Loader PyArrow / PySpark| G[(PostgreSQL Warehouse<br/>flights_dw.silver_flights)]
        WH -->|Insertion directe| H[(PostgreSQL Warehouse<br/>flights_dw.raw_weather)]
    end

    subgraph Transformation ["4. Modélisation Dimensionnelle (dbt)"]
        G & H --> I[dbt Staging Models<br/>stg_flights, stg_weather, stg_airports]
        I --> J[dbt Intermediate Models<br/>int_flights_near_airport, int_weather_hourly]
        J --> K[dbt Marts Kimball - Schéma en Étoile<br/>fct_airport_movements_hourly<br/>mart_traffic_weather_impact<br/>dim_date, dim_airport, dim_airline]
    end

    subgraph Analytics ["5. Visualisation & Orchestration"]
        K --> L[Streamlit Dashboard 3D<br/>Lecture directe flights_dw]
        M[Apache Airflow 2.8.4] -->|DAG dbt_hourly| G
        M -->|Trigger dbt seed, run, test| I
    end
```

---

## 🛠️ Stack Technologique

| Composant | Technologie | Rôle Opérationnel |
| :--- | :--- | :--- |
| **Message Broker** | Apache Kafka 3.6 (Confluent 7.6.1) | Ingestion distribuée persistante, partitionnement par clé `icao24`. |
| **Coordination Cluster** | Apache ZooKeeper 3.8 | Gestion des métadonnées du cluster Kafka et consensus. |
| **Traitement Streaming** | Apache Spark 3.5.1 (PySpark) | Structured Streaming avec déduplication temporelle (`watermark 10 min`). |
| **Format Lakehouse** | Delta Lake 3.1.0 | Stockage ACID sur disque (couches Bronze et Silver). |
| **Data Warehouse** | PostgreSQL 15 | Entrepôt dédié (`flights_dw`) pour la couche analytique Gold. |
| **Transformations BI** | dbt (Data Build Tool) | Modélisation dimensionnelle Kimball, tests de schéma et portabilité multi-cibles. |
| **Orchestration** | Apache Airflow 2.8.4 | Ordonnancement automatique des batchs de synchronisation et d'exécution dbt. |
| **Dashboard BI** | Streamlit + pydeck | Interface de visualisation spatiale 3D connectée directement à `flights_dw`. |
| **Assurance Qualité** | Pytest & Ruff | Suite de tests unitaires/intégration et conformité PEP-8. |

---

## 🌐 Cartographie des Services & Interfaces Web

| Interface / Service | URL / Port Local | Identifiants | Rôle |
| :--- | :--- | :--- | :--- |
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) | *Accès libre* | Carte 3D pydeck, mouvements par aéroport et corrélation météo |
| **Kafka UI** | [http://localhost:8080](http://localhost:8080) | *Accès libre* | Monitoring des topics (`flights.raw`, `flights.dlq`) et partitions |
| **Airflow Webserver** | [http://localhost:8085](http://localhost:8085) | `admin` / `admin` | Supervision des DAGs (`dbt_hourly`, `weather_daily`) |
| **PostgreSQL Data Warehouse** | `localhost:5433` | `flights_user` / `flights_password_dev` | Base analytique `flights_dw` (DBeaver / pgAdmin) |
| **Kafka Broker** | `localhost:9092` / `localhost:19092` | *Sans auth (dev)* | Endpoint de streaming externe |

---

## 🚀 Démarrage Rapide

### Prérequis
- Docker et Docker Compose
- Python 3.11+

### 1. Variables d'Environnement
```bash
cp .env.example .env
```
*(Par défaut, `PRODUCER_SYNTHETIC_MODE=true` permet de démarrer sans clé d'API. Renseignez vos identifiants OpenSky dans `.env` et passez `PRODUCER_SYNTHETIC_MODE=false` pour consommer les vols en direct).*

### 2. Démarrage de l'Infrastructure
```bash
docker compose up -d
```
Vérifiez l'état des conteneurs :
```bash
docker compose ps
```

### 3. Synchronisation et Calculs dbt
```bash
# Synchronisation Delta Lake Silver -> PostgreSQL DW
docker compose exec airflow-webserver python -m ingestion.batch.silver_to_postgres

# Exécution des seeds, modèles et tests dbt
docker compose exec airflow-webserver dbt seed --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
docker compose exec airflow-webserver dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
docker compose exec airflow-webserver dbt test --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
```

### 4. Exécution de la Suite de Tests
```bash
python -m pytest tests/
```

---

## 📐 Modélisation Dimensionnelle (dbt Kimball)

L'entrepôt `flights_dw` est structuré en plusieurs schémas :

```text
flights_dw
├── public (Tables brutes ingérées depuis Delta Lake & Open-Meteo)
│   ├── silver_flights
│   └── raw_weather
│
├── public_staging (Nettoyage, casting strict numeric/timestamps, déduplication)
│   ├── stg_flights
│   ├── stg_weather
│   ├── stg_airports (Seed ICAO/IATA des aéroports cibles marocains)
│   └── stg_airlines (Seed des compagnies aériennes)
│
├── public_intermediate (Calculs géodésiques et agrégats horaires)
│   ├── int_flights_near_airport (Distances Haversine < 50 km)
│   ├── int_flight_phases (Classification montée, croisière, descente, approche)
│   └── int_weather_hourly (Météo horaire normalisée)
│
└── public_marts (Tables de faits et dimensions - Schéma en Étoile)
    ├── dim_airport (Coordonnées GPS, codes ICAO/IATA, piste principale)
    ├── dim_date (Calendrier enrichi avec marqueurs de fêtes et événements)
    ├── dim_airline (Dimension compagnie aérienne avec snapshot SCD Type 2)
    ├── fct_airport_movements_hourly (Mouvements horaires et taux d'activité par aéroport)
    ├── fct_traffic_density (Densité horaire d'aéronefs par tranche d'altitude)
    └── mart_traffic_weather_impact (Corrélation finale trafic vs sévérité météo)
```

### Portabilité Multi-Cibles (PostgreSQL & Snowflake)
> ℹ️ **Statut :** Les modèles SQL sont **conçus pour être portables et ont été testés sur PostgreSQL**.  
> Le profil Snowflake est pré-configuré dans [`profiles.yml`](dbt_project/profiles.yml). La portabilité est garantie par l'usage exclusif de fonctions SQL ANSI communes aux deux moteurs (`EXTRACT(DOW FROM ...)`, `TO_CHAR(...)`, et casts explicites `numeric`).

---

## 🔮 Pistes d'Évolution

1. **Modèle de Machine Learning Prédictif** : Entraîner un modèle sur `mart_traffic_weather_impact` pour anticiper les congestions d'aéroports en fonction des prévisions météo.
2. **Monitoring & Alerting Prometheus / Grafana** : Collecter les métriques de latence Spark et de lag Kafka pour émettre des alertes.
3. **Partitionnement Iceberg** : Évaluer Apache Iceberg en complément ou alternative à Delta Lake.

---

## 📄 Licence

Ce projet est distribué sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus d'informations.
