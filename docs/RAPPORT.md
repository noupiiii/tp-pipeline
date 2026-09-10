# Rapport d'analyse — Pipeline de données TP Louis Aurélien

Captures d'écran d'une exécution réelle et complète du pipeline (ingestion → Data Lake HDFS →
transformation Spark → Data Warehouse PostgreSQL), déployé via Docker Compose sur un serveur Linux
distant. Le DAG `weather_etl_pipeline` a été exécuté avec succès à plusieurs reprises.

## 1. Airflow — vue d'ensemble du DAG

3 exécutions complètes, toutes en succès, des 4 tâches `ingest → upload_to_datalake → transform → load`.

![Vue grille du DAG Airflow](screenshots/01-airflow-dag-grid.png)

## 2. Airflow — graphe des dépendances

![Graphe du DAG Airflow](screenshots/02-airflow-dag-graph.png)

## 3. Airflow — liste des DAGs

![Liste des DAGs Airflow](screenshots/03-airflow-dags-list.png)

## 4. Data Lake HDFS — vue d'ensemble du cluster

![Vue d'ensemble du namenode HDFS](screenshots/04-hdfs-overview.png)

## 5. Data Lake HDFS — données brutes ingérées

Un fichier JSON par exécution du DAG, horodaté.

![Explorateur HDFS - données brutes](screenshots/05-hdfs-raw-explorer.png)

## 6. Data Lake HDFS — données transformées (Parquet)

Fichiers Parquet produits par le job Spark, avec marqueur `_SUCCESS`.

![Explorateur HDFS - données transformées](screenshots/06-hdfs-processed-explorer.png)

## 7. Cluster Spark

![UI du master Spark](screenshots/07-spark-master.png)
