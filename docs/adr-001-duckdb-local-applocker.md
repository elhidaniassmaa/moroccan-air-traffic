# ADR 001: Migration de DuckDB vers PostgreSQL Data Warehouse

## Contexte
Initialement, le projet utilisait DuckDB comme moteur SQL local pour dbt.
Cependant :
1. Dans un contexte de valorisation d'un profil Data Engineer en entreprise, DuckDB (moteur embarqué mono-utilisateur) est souvent perçu comme un outil de prototypage local plutôt qu'un Data Warehouse d'entreprise partagé et distribué.
2. Sur les postes d'entreprise Windows avec politique stricte de sécurité (AppLocker / WDAC), le chargement de DLLs natives non signées (`_duckdb.pyd`) peut être restreint.

## Décision
Remplacer intégralement DuckDB par **PostgreSQL 15** comme Data Warehouse analytique dédié (`flights_dw`) :
1. **Service dédié `warehouse`** : Totalement distinct de la base de métadonnées d'Airflow, configuré sur le port `5433` avec volume persistant `warehouse_data`.
2. **Pipeline de chargement idempotent** : Job Spark JDBC et synchronisation PyArrow pour transférer les données enrichies Delta Lake Silver vers la table `silver_flights`.
3. **dbt-postgres** : Modèles dbt réécrits pour PostgreSQL avec schémas Kimball (`public_staging`, `public_intermediate`, `public_marts`).
4. **Portabilité Snowflake (Phase 2)** : Modèles rendus 100% compatibles cross-database entre PostgreSQL et Snowflake.
5. **Dashboard Streamlit** : Connecté via SQLAlchemy et psycopg2 avec cache `@st.cache_resource` / `@st.cache_data`.

## Conséquences
- Architecture conforme aux standards de l'industrie (séparation Lakehouse / Warehouse).
- Élimination des erreurs de dépendances et de verrouillage de fichiers mono-processus propres à DuckDB.
- Modèles dbt testés automatiquement (not_null, unique, accepted_values, relationships) avec 100% de succès.
