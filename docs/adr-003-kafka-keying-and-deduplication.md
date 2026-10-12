# ADR 003: Stratégie de Partitionnement Kafka et Déduplication At-Least-Once

## Contexte
La télémétrie OpenSky / ADS-B est émise périodiquement par les aéronefs. Le polling API ou les reprises de flux génèrent inévitablement des doublons d'état pour un même aéronef au même horodatage.

## Décision
1. **Clé de message Kafka : `icao24`** :
   - L'adresse hexadécimale du transpondeur de l'aéronef est choisie comme clé de partitionnement Kafka.
   - Raison : cela garantit le partition-level ordering. Toutes les positions d'un même aéronef sont consommées dans l'ordre chronologique par la même tâche de traitement, évitant les incohérences de calcul de vitesse ou de trajectoire.

2. **Garantie At-Least-Once avec Déduplication Idempotente** :
   - Côté producteur : `enable.idempotence=true`, `acks=all`, retries infinis pour garantir zéro perte vers Redpanda.
   - Côté Spark Structured Streaming (Bronze) : application d'un `withWatermark("event_timestamp", "10 minutes")` associé à un `dropDuplicates(["icao24", "time_position"])`.
   - Côté dbt (Staging) : fenêtrage `row_number() over (partition by icao24, position_ts order by ingestion_ts desc)` pour absorber les éventuels doublons tardifs hors fenêtre de streaming.

## Conséquences
- Zéro écriture dupliquée dans les tables analytiques finales.
- Consommation mémoire maîtrisée dans Spark grâce au compactage de l'état du watermark (10 min).
