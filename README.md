# TP Louis Aurélien — Pipeline de données Cloud & Big Data

Pipeline de données de bout en bout : ingestion API → Data Lake (HDFS) → transformation (Spark/Python) →
Data Warehouse (PostgreSQL) → orchestration (Airflow) → conteneurisation (Docker) → CI/CD (GitHub Actions) →
déploiement cloud (AWS EC2).

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

Le tout est orchestré par **Airflow** (un DAG quotidien : ingest → transform → load), packagé avec
**Docker Compose**, testé et vérifié en CI via **GitHub Actions**, et déployable sur une instance **AWS EC2**.

## Source de données

[Open-Meteo](https://open-meteo.com/) : API météo publique, sans clé d'API requise. Le client
d'ingestion récupère les prévisions horaires (température, vent, humidité) pour une ville configurable
(Paris par défaut).

## Structure du repo

```
.
├── src/
│   ├── config.py              # configuration centralisée (env vars)
│   ├── ingestion/              # appel API + écriture des données brutes
│   ├── datalake/                # écriture/lecture HDFS
│   ├── transform/               # nettoyage / agrégation (Spark ou pandas)
│   └── load/                    # chargement dans PostgreSQL
├── airflow/dags/                # DAG d'orchestration
├── sql/                          # schéma du Data Warehouse
├── tests/                        # tests unitaires (pytest)
├── docker-compose.yml            # Hadoop, Spark, Postgres, Airflow
├── .github/workflows/ci.yml      # lint + tests automatisés
└── infra/aws/                    # notes de déploiement EC2
```

## Démarrage rapide

```bash
cp .env.example .env
docker compose up -d
```

Services exposés :
- Airflow UI : http://localhost:8080 (admin / admin)
- Spark master UI : http://localhost:8081
- HDFS namenode UI : http://localhost:9870
- PostgreSQL (DWH) : localhost:5432

## Développement local (sans Docker)

```bash
python -m venv .venv
source .venv/bin/activate   # ou .venv\Scripts\activate sous Windows
pip install -r requirements.txt -r requirements-dev.txt
pytest
python -m src.ingestion.api_client
```

## CI/CD

Chaque push/PR déclenche (`.github/workflows/ci.yml`) :
1. Installation des dépendances
2. Lint (`ruff`)
3. Tests unitaires (`pytest`)

## Déploiement AWS

Voir [infra/aws/README.md](infra/aws/README.md) pour le déploiement sur une instance EC2 via Docker Compose.

## Rapport d'exécution

Voir [docs/RAPPORT.md](docs/RAPPORT.md) pour un compte-rendu détaillé, étape par étape,
d'une exécution complète du pipeline (logs, requêtes SQL, contenu HDFS, états Airflow).

## Roadmap

- [x] Squelette du repo
- [ ] Client d'ingestion API opérationnel
- [ ] Écriture Data Lake (HDFS)
- [ ] Job de transformation Spark
- [ ] Chargement PostgreSQL + schéma DWH
- [ ] DAG Airflow bout-en-bout
- [ ] CI GitHub Actions
- [ ] Déploiement EC2
