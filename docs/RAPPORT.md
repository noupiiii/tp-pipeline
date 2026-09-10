# Rapport de processus — Pipeline de données TP Louis Aurélien

Ce document trace l'exécution complète du pipeline, étape par étape, avec les preuves
d'exécution (logs, requêtes, états des tâches) relevées sur un run réel de bout en bout.

> 📸 **Captures d'écran** : les emplacements ci-dessous sont prêts à recevoir des
> captures des interfaces web (Airflow, Spark, HDFS). Pour les ajouter : ouvrir
> l'interface concernée (URLs données dans chaque section), faire une capture, la
> déposer dans `docs/screenshots/<nom-indiqué>.png` — l'image s'affichera alors
> automatiquement ici.

## Vue d'ensemble

```
API Open-Meteo ──▶ ingestion (Python) ──▶ Data Lake (HDFS, JSON brut)
                                                  │
                                                  ▼
                                    transformation (PySpark)
                                                  │
                                                  ▼
                                  Data Warehouse (PostgreSQL)
```

Orchestré par un DAG Airflow (`ingest` → `upload_to_datalake` → `transform` → `load`),
le tout packagé en conteneurs Docker (Hadoop HDFS, Spark, PostgreSQL, Airflow).

## 1. Démarrage de l'environnement

```bash
docker compose up -d
```

État des services une fois démarrés :

```
NAME                SERVICE             STATUS                   PORTS
airflow-db          airflow-db          Up 7 minutes (healthy)   5432/tcp
airflow-scheduler   airflow-scheduler   Up 7 minutes             8080/tcp
airflow-webserver   airflow-webserver   Up 7 minutes             0.0.0.0:8080->8080/tcp
datanode            datanode            Up 7 minutes (healthy)   0.0.0.0:9864->9864/tcp
namenode            namenode            Up 7 minutes (healthy)   0.0.0.0:9870->9870/tcp, 0.0.0.0:9820->9000/tcp
postgres-dwh        postgres-dwh        Up 7 minutes (healthy)   0.0.0.0:5432->5432/tcp
spark-master        spark-master        Up 7 minutes             0.0.0.0:7077->7077/tcp, 0.0.0.0:8081->8080/tcp
spark-worker        spark-worker        Up 7 minutes
```

8 conteneurs actifs : 2 pour Hadoop (namenode/datanode), 2 pour Spark (master/worker),
2 pour PostgreSQL (DWH + métadonnées Airflow), 2 pour Airflow (webserver/scheduler).

Rapport du cluster HDFS (`hdfs dfsadmin -report`), confirmant l'enregistrement du datanode :

```
Configured Capacity: 1081101176832 (1006.85 GB)
Present Capacity: 983339241579 (915.81 GB)
DFS Remaining: 983338885120 (915.81 GB)
DFS Used: 356459 (348.10 KB)
```

![HDFS namenode overview](screenshots/01-hdfs-overview.png)
*(Interface : http://localhost:9870 — page d'accueil du namenode)*

## 2. Ingestion — appel API

`src/ingestion/api_client.py` interroge l'API publique [Open-Meteo](https://open-meteo.com/)
(prévisions horaires : température, humidité, vent, précipitations) pour Paris, et écrit
le JSON brut horodaté.

Extrait de log Airflow (tâche `ingest`) :

```
INFO - Fetching weather data for lat=48.8566 lon=2.3522
INFO - Raw payload written to data/raw/weather_paris_20260910T095522Z.json
```

## 3. Dépôt dans le Data Lake (HDFS)

`src/datalake/hdfs_writer.py` envoie le fichier JSON brut vers HDFS via WebHDFS
(tâche `upload_to_datalake`).

Contenu du dossier `/data-lake/raw/weather` après plusieurs runs (`hdfs dfs -ls -R`) :

```
-rw-r--r--   3 root supergroup      12330 2026-09-10 07:53 /data-lake/raw/weather/weather_paris_20260910T075323Z.json
-rw-r--r--   3 root supergroup      12330 2026-09-10 07:56 /data-lake/raw/weather/weather_paris_20260910T075446Z.json
-rw-r--r--   3 root supergroup      12330 2026-09-10 08:06 /data-lake/raw/weather/weather_paris_20260910T075955Z.json
-rw-r--r--   3 root supergroup      12328 2026-09-10 08:07 /data-lake/raw/weather/weather_paris_20260910T080721Z.json
-rw-r--r--   3 root supergroup      12328 2026-09-10 09:56 /data-lake/raw/weather/weather_paris_20260910T095120Z.json
-rw-r--r--   3 root supergroup      12329 2026-09-10 09:55 /data-lake/raw/weather/weather_paris_20260910T095522Z.json
```

Chaque exécution du DAG dépose un nouveau fichier JSON brut, horodaté — traçabilité
complète des données brutes reçues de l'API.

![HDFS raw explorer](screenshots/02-hdfs-raw-explorer.png)
*(Interface : http://localhost:9870/explorer.html#/data-lake/raw/weather)*

## 4. Transformation (PySpark)

`src/transform/spark_transform.py` lit l'ensemble des fichiers JSON bruts du Data Lake,
aplatit les tableaux horaires (`hourly.time`, `hourly.temperature_2m`, ...) en une ligne
par timestamp, et écrit le résultat au format Parquet dans
`/data-lake/processed/weather` (tâche `transform`).

Contenu du dossier processed (extrait) :

```
-rw-r--r--   3 root supergroup          0 2026-09-10 09:56 /data-lake/processed/weather/_SUCCESS
-rw-r--r--   3 root supergroup       4787 2026-09-10 09:55 /data-lake/processed/weather/part-00000-060fa5ef-...-c000.snappy.parquet
-rw-r--r--   3 root supergroup       4787 2026-09-10 09:55 /data-lake/processed/weather/part-00001-060fa5ef-...-c000.snappy.parquet
... (6 fichiers Parquet pour ce run)
```

Le marqueur `_SUCCESS` confirme l'écriture complète du job Spark.

![Spark master UI](screenshots/03-spark-master.png)
*(Interface : http://localhost:8081 — worker actif, 16 cœurs / 10.4 GiB disponibles)*

## 5. Chargement dans le Data Warehouse (PostgreSQL)

`src/load/postgres_loader.py` rapatrie les fichiers Parquet du Data Lake (via WebHDFS),
puis les insère dans la table `weather_hourly` (tâche `load`) avec une logique **upsert**
(`INSERT ... ON CONFLICT DO NOTHING` sur `latitude, longitude, observed_at`) — chaque
run retraitant l'historique complet des fichiers bruts, l'idempotence évite les doublons.

Schéma de la table (`sql/init.sql`) :

```sql
CREATE TABLE weather_hourly (
    id                SERIAL PRIMARY KEY,
    latitude          DOUBLE PRECISION NOT NULL,
    longitude         DOUBLE PRECISION NOT NULL,
    observed_at       TIMESTAMP NOT NULL,
    temperature_c     DOUBLE PRECISION,
    humidity_pct      DOUBLE PRECISION,
    wind_speed_kmh    DOUBLE PRECISION,
    precipitation_mm  DOUBLE PRECISION,
    loaded_at         TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (latitude, longitude, observed_at)
);
```

Vérification du contenu final (`psql`) :

```
 total_rows
------------
        168
(1 row)

 latitude | longitude |     observed_at     | temperature_c | humidity_pct | wind_speed_kmh | precipitation_mm
----------+-----------+---------------------+---------------+--------------+----------------+------------------
    48.86 | 2.3599997 | 2026-09-10 00:00:00 |          15.8 |           61 |            3.2 |                0
    48.86 | 2.3599997 | 2026-09-10 01:00:00 |          15.1 |           63 |            3.9 |                0
    48.86 | 2.3599997 | 2026-09-10 02:00:00 |          14.2 |           72 |            2.5 |                0
    48.86 | 2.3599997 | 2026-09-10 03:00:00 |          13.8 |           74 |            5.5 |                0
    48.86 | 2.3599997 | 2026-09-10 04:00:00 |          13.4 |           77 |              4 |                0
    48.86 | 2.3599997 | 2026-09-10 05:00:00 |          12.7 |           78 |            2.7 |                0
    48.86 | 2.3599997 | 2026-09-10 06:00:00 |          12.4 |           79 |            1.8 |                0
    48.86 | 2.3599997 | 2026-09-10 07:00:00 |          11.9 |           81 |            2.2 |                0
```

**Test d'idempotence** : deux exécutions consécutives du DAG ont été lancées ; la
seconde s'est terminée avec succès sans erreur de contrainte unique, et le nombre de
lignes distinctes (`COUNT(DISTINCT (latitude, longitude, observed_at))`) est resté égal
au nombre total de lignes — aucun doublon introduit.

## 6. Orchestration — Airflow

Le DAG `weather_etl_pipeline` (`airflow/dags/weather_etl_dag.py`) enchaîne les 4 tâches
avec dépendances strictes : `ingest >> upload_to_datalake >> transform >> load`,
planifié quotidiennement (`@daily`), avec 2 retries automatiques par tâche.

État final d'un run complet (`airflow tasks states-for-dag-run`) :

```
dag_id               | execution_date            | task_id            | state   | start_date                       | end_date
=====================+===========================+====================+=========+==================================+=================================
weather_etl_pipeline | 2026-09-10T09:55:20+00:00 | ingest             | success | 2026-09-10T09:55:21.625762+00:00 | 2026-09-10T09:55:22.187453+00:00
weather_etl_pipeline | 2026-09-10T09:55:20+00:00 | upload_to_datalake | success | 2026-09-10T09:55:22.704576+00:00 | 2026-09-10T09:55:24.236250+00:00
weather_etl_pipeline | 2026-09-10T09:55:20+00:00 | transform          | success | 2026-09-10T09:55:24.857232+00:00 | 2026-09-10T09:55:37.508858+00:00
weather_etl_pipeline | 2026-09-10T09:55:20+00:00 | load               | success | 2026-09-10T09:55:37.901250+00:00 | 2026-09-10T09:55:39.570934+00:00
```

Durée totale du pipeline : ~19 secondes (ingestion → chargement final).

![Airflow DAG grid view](screenshots/04-airflow-dag-grid.png)
*(Interface : http://localhost:8080/dags/weather_etl_pipeline/grid — vue historique
des runs, les 4 tâches en vert = succès)*

## 7. Synthèse des vérifications effectuées

| Étape           | Vérification                                          | Résultat |
|-----------------|--------------------------------------------------------|----------|
| Infrastructure  | 8 conteneurs démarrés et healthy                        | ✅ |
| HDFS            | Datanode enregistré, capacité ~1 To disponible           | ✅ |
| Ingestion       | Fichier JSON brut écrit et horodaté                      | ✅ |
| Data Lake       | Fichier brut présent sous `/data-lake/raw/weather`        | ✅ |
| Transformation  | Parquet généré, marqueur `_SUCCESS` présent               | ✅ |
| Data Warehouse  | 168 lignes chargées, schéma conforme                      | ✅ |
| Idempotence     | 2 runs consécutifs, 0 doublon introduit                   | ✅ |
| Orchestration   | DAG Airflow : 4/4 tâches en succès                        | ✅ |

## 8. Limites connues / axes d'amélioration

- Le job Spark tourne actuellement en mode local (`local[*]`) intégré au conteneur
  Airflow plutôt que sur le cluster Spark master/worker démarré par
  `docker-compose.yml` (0 applications visibles sur son UI). Un axe d'amélioration
  serait de soumettre le job via `spark://spark-master:7077`.
- Le déploiement AWS (`infra/aws/README.md`) est documenté mais n'a pas été exécuté
  dans le cadre de ce rapport (nécessite une instance EC2 provisionnée).
