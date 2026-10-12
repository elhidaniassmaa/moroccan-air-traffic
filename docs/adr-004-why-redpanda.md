# ADR 004: Choix de Redpanda comme Moteur de Streaming Kafka-Compatible

## Contexte
Le projet nécessite un broker compatible avec l'API Apache Kafka pour accueillir le débit d'événements de vol et permettre la consommation par Spark Structured Streaming.

## Décision
Utiliser **Redpanda** plutôt qu'un cluster Apache Kafka standard (avec Zookeeper ou KRaft Java).

### Justifications :
1. **Légèreté et Démarrage Instantané** : Développé en C++, binaire unique sans JVM, consomme moins de 250 Mo de RAM au repos contre 1 à 2 Go pour Kafka/JVM.
2. **Compatibilité 100% avec l'écosystème Kafka** : Compatible avec `confluent-kafka` en Python et le connecteur `spark-sql-kafka-0-10`.
3. **Console d'Administration Intégrée** : Redpanda Console permet d'inspecter visuellement les topics, les messages et les groupes de consommateurs immédiatement via port 8080.
4. **Idéal pour l'environnement Local et CI** : Démarrage en moins de 3 secondes dans `docker compose`.
