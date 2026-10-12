# ADR 002: Découplage de la frontière Streaming (Kafka + Spark) et Batch (Airflow + dbt)

## Contexte
Le projet combine l'ingestion d'événements à haute fréquence (positions ADS-B toutes les 15s via OpenSky / Redpanda) et des enrichissements analytiques par batch (météo toutes les 6 heures, transformations dbt incrémentales chaque heure).

## Décision
1. **Périmètre en Streaming continu (Hors Airflow)** :
   - Le producteur OpenSky (`opensky_producer.py`) tourne en processus démon continu (avec redémarrage automatique dans Docker Compose).
   - Les streams Spark (`flights_bronze_stream.py` et `flights_silver_stream.py`) tournent en Structured Streaming continu 24/7 avec checkpoints résilients.
   - Raison : Airflow est un ordonnanceur de graphes de tâches périodiques (DAGs finis), il n'est pas conçu pour piloter des boucles infinies de streaming ni surveiller des watermarks sous la minute.

2. **Périmètre Orchestré par Airflow** :
   - `DAG weather_daily` : Ingestion batch via `ingestion.batch.weather` à intervalle régulier, suivie du déclenchement du modèle dbt `stg_weather` et `int_weather_hourly`.
   - `DAG dbt_hourly` : Construction incrémentale des marts Kimball (`fct_airport_movements_hourly`, `mart_traffic_weather_impact`) et exécution des tests de qualité de données.
   - Retries (3 tentatives), backoff exponentiel et alertes d'échec configurées.

## Conséquences
- Séparation nette des responsabilités : les pannes de stream n'engorgent pas la queue de tâches Airflow.
- Idempotence garantie : la table incrémentale dbt repose sur des identifiants MD5 stables (`movement_hour_id`), permettant à Airflow de relancer les tranches horaires sans duplication.
