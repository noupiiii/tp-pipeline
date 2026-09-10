# TP Louis Aurélien — Pipeline de données Cloud & Big Data

Pipeline de données de bout en bout : ingestion API → Data Lake (HDFS) → transformation (Spark) →
Data Warehouse (PostgreSQL) → orchestration (Airflow) → conteneurisation (Docker) → CI/CD (GitHub Actions) →
déploiement cloud.

## Architecture

```
API (Open-Meteo) ──▶ Ingestion (Python) ──▶ Data Lake (HDFS, JSON brut)
                                                     │
                                                     ▼
                                        Transformation (Spark / Python)
                                                     │
                                                     ▼
                                     Data Warehouse (PostgreSQL)
                                                     │
                                                     ▼
                                          Analyse / Dashboard
```

Le tout est orchestré par **Airflow** (DAG `ingest → upload_to_datalake → transform → load`, planifié
`@daily`), packagé avec **Docker Compose** (Hadoop HDFS, Spark, PostgreSQL, Airflow), testé et vérifié
en CI via **GitHub Actions**, et déployable sur n'importe quel hôte Linux (VM cloud, VPS, EC2...).

## Source de données

[Open-Meteo](https://open-meteo.com/) : API météo publique, sans clé d'API requise. Le client
d'ingestion récupère les prévisions horaires (température, humidité, vent, précipitations) pour une
ville configurable (Paris par défaut).

## Structure du repo

```
.
├── src/
│   ├── config.py              # configuration centralisée (variables d'environnement)
│   ├── ingestion/              # appel API + écriture des données brutes (JSON)
│   ├── datalake/                # écriture/lecture HDFS (WebHDFS)
│   ├── transform/               # aplatissement + agrégation (PySpark)
│   └── load/                    # chargement idempotent dans PostgreSQL (upsert)
├── airflow/dags/                # DAG d'orchestration (weather_etl_pipeline)
├── sql/                          # schéma du Data Warehouse
├── tests/                        # tests unitaires (pytest)
├── docker/airflow/Dockerfile     # image Airflow + dépendances du projet
├── docker-compose.yml            # Hadoop, Spark, PostgreSQL, Airflow
├── .github/workflows/ci.yml      # lint (ruff) + tests automatisés
├── infra/aws/                    # notes de déploiement sur une instance cloud
└── docs/RAPPORT.md               # rapport d'exécution (captures d'écran du pipeline)
```

## Prérequis

- [Docker](https://docs.docker.com/get-docker/) et le plugin Docker Compose v2 (`docker compose version`)
- Git
- ~4 Go de RAM disponibles pour les conteneurs (Hadoop + Spark + Postgres + Airflow)
- Ports libres sur la machine hôte : `8080` (Airflow), `8081` (Spark UI), `7077` (Spark RPC),
  `9870`/`9820`/`9864` (HDFS), `5432` (PostgreSQL, lié en local uniquement) — tous overridables,
  voir [Configuration](#configuration)

## Installation

```bash
git clone https://github.com/noupiiii/tp-pipeline.git
cd tp-pipeline
cp .env.example .env
```

## Configuration

Toute la configuration passe par `.env` (voir [.env.example](.env.example)) :

| Variable | Rôle | Défaut |
|---|---|---|
| `WEATHER_LATITUDE` / `WEATHER_LONGITUDE` / `WEATHER_CITY` | Ville pour laquelle ingérer la météo | Paris |
| `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | Identifiants du Data Warehouse | `dwh` / `dwh_user` / `change_me` |
| `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` | Identifiants de l'UI Airflow | `admin` / `admin` |
| `AIRFLOW_WEBSERVER_PORT`, `NAMENODE_UI_PORT`, `SPARK_MASTER_UI_PORT`, ... | Ports exposés sur l'hôte | valeurs "standard" (8080, 9870, 8081, ...) |
| `DWH_POSTGRES_BIND` | Interface d'écoute de PostgreSQL | `127.0.0.1` (jamais exposé publiquement par défaut) |

⚠️ **Avant tout déploiement autre que local** : changer `POSTGRES_PASSWORD` et
`_AIRFLOW_WWW_USER_PASSWORD` (des valeurs par défaut faibles sur une UI/DB exposée sur Internet sont une
vraie faille). Sur un hôte partagé où les ports par défaut sont déjà pris, surcharger les variables de
port dans `.env` (ex. `AIRFLOW_WEBSERVER_PORT=8089`).

## Exécution

```bash
docker compose up -d --build
```

Attendre que les services soient `healthy` (`docker compose ps`), puis déclencher le DAG :

```bash
# via l'UI (http://localhost:8080, identifiants dans .env), bouton ▶, ou en CLI :
docker compose exec airflow-scheduler airflow dags unpause weather_etl_pipeline
docker compose exec airflow-scheduler airflow dags trigger weather_etl_pipeline
```

Suivre l'exécution :
- Airflow UI : http://localhost:8080 — grille des tâches `ingest → upload_to_datalake → transform → load`
- HDFS namenode UI : http://localhost:9870 — explorer `/data-lake/raw` et `/data-lake/processed`
- Spark master UI : http://localhost:8081
- PostgreSQL (DWH) : `localhost:5432` (depuis l'hôte uniquement, cf. `DWH_POSTGRES_BIND`)

## Résultats

Une exécution réussie du DAG produit :
- un fichier JSON brut horodaté par run dans `/data-lake/raw/weather` (HDFS)
- des fichiers Parquet dans `/data-lake/processed/weather` (HDFS), marqués par un fichier `_SUCCESS`
- des lignes dans la table `weather_hourly` du Data Warehouse PostgreSQL (chargement **idempotent** :
  relancer le DAG plusieurs fois ne crée pas de doublons, grâce à un upsert `ON CONFLICT DO NOTHING`
  sur `(latitude, longitude, observed_at)`)

Voir [docs/RAPPORT.md](docs/RAPPORT.md) pour les captures d'écran d'une exécution réelle (grille Airflow,
explorateur HDFS, UI Spark) et le détail des vérifications effectuées.

## Développement local (sans Docker)

```bash
python -m venv .venv
source .venv/bin/activate   # ou .venv\Scripts\activate sous Windows
pip install -r requirements.txt -r requirements-dev.txt
pytest
python -m src.ingestion.api_client
```

## Tests & CI/CD

Chaque push/PR déclenche (`.github/workflows/ci.yml`) :
1. Installation des dépendances (Python + Java, requis par PySpark)
2. Lint (`ruff check src tests`)
3. Tests unitaires (`pytest`)

## Déploiement cloud

Le pipeline est packagé en Docker Compose et ne dépend d'aucun service managé — il se déploie donc
sur n'importe quel hôte Linux disposant de Docker (VM cloud AWS/GCP/Azure, VPS, bare metal).

Voir [infra/aws/README.md](infra/aws/README.md) pour la procédure détaillée (provisioning EC2, install
Docker, `docker compose up -d`). Le projet a été validé en conditions réelles sur un serveur Linux
distant (voir [docs/RAPPORT.md](docs/RAPPORT.md)) : pipeline complet exécuté avec succès, données
vérifiées en base.

**Note pour un déploiement sur un hôte partagé** (plusieurs projets/étudiants sur la même machine) :
- ne pas fixer `container_name` dans `docker-compose.yml` (laisser Compose préfixer par le nom du projet
  `name:`, pour éviter tout conflit avec les conteneurs d'un autre projet)
- vérifier les ports déjà utilisés (`ss -tlnp`) et surcharger les variables `*_PORT` en conséquence

## Roadmap

- [x] Squelette du repo
- [x] Client d'ingestion API opérationnel
- [x] Écriture Data Lake (HDFS)
- [x] Job de transformation Spark
- [x] Chargement PostgreSQL + schéma DWH (idempotent)
- [x] DAG Airflow bout-en-bout
- [x] CI GitHub Actions
- [x] Déploiement sur un serveur cloud (Docker Compose, hôte Linux distant)
