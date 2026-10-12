# ADR 001: Exécution locale de DuckDB / dbt sous politique Windows AppLocker et conteneurisation

## Contexte
Sur cette machine hôte Windows (environnement de bureau entreprise), une stratégie de contrôle d'application (AppLocker / Windows Defender Application Control) bloque le chargement des DLL natives non signées dans Python (`_duckdb.pyd`).
L'erreur observée :
`ImportError: DLL load failed while importing _duckdb: Une stratégie de contrôle d’application a bloqué ce fichier.`

## Décision
1. **Environnement de production / CI / Docker Compose** :
   - DuckDB et `dbt-duckdb` s'exécutent dans les conteneurs Linux Docker (`docker compose` ou images standard Debian/Alpine) et dans GitHub Actions (Ubuntu runner), où aucune restriction Windows AppLocker n'existe.
   - Les modèles dbt, macros, snapshots, tests et seeds sont écrits de manière 100% conforme à la syntaxe SQL standard DuckDB (`read_parquet`, `range`, `degrees/radians`, macros haversine, `fct_airport_movements_hourly` incrémental).

2. **Validation unitaire en local sur la machine hôte sans DuckDB DLL** :
   - Un script de validation / simulation (`scripts/validate_dbt_models.py`) valide syntaxiquement chaque fichier SQL, parse les macros, vérifie la complétude des modèles (staging, intermediate, marts) et s'assure que la structure relationnelle Kimball est respectée.
   - Les tests `sqlfluff` et `ruff` s'exécutent en local sur l'hôte.

## Conséquences
- Transparence totale dans le README et la documentation : mention explicite de cette politique de sécurité hôte et instructions d'exécution via conteneur ou GitHub Actions.
- Aucun contournement hasardeux de la sécurité du système hôte n'est tenté.
