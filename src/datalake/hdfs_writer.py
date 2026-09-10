"""Data Lake : dépose les fichiers JSON bruts dans HDFS via WebHDFS.

Utilise le client `hdfs` (WebHDFS REST API) pour rester indépendant de
l'installation locale de binaires Hadoop — seul le namenode doit être
accessible (cf. docker-compose.yml, service `namenode`).
"""
from __future__ import annotations

import logging
from pathlib import Path

from hdfs import InsecureClient

from src.config import HDFS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_client() -> InsecureClient:
    return InsecureClient(HDFS.webhdfs_url, user="root")


def upload_raw_file(local_path: Path, hdfs_dir: str = HDFS.raw_path) -> str:
    """Envoie un fichier local vers le Data Lake HDFS et retourne le chemin distant."""
    client = get_client()
    client.makedirs(hdfs_dir)
    remote_path = f"{hdfs_dir}/{local_path.name}"
    client.upload(remote_path, str(local_path), overwrite=True)
    logger.info("Uploaded %s -> hdfs://%s", local_path, remote_path)
    return remote_path


def list_raw_files(hdfs_dir: str = HDFS.raw_path) -> list[str]:
    client = get_client()
    if not client.status(hdfs_dir, strict=False):
        return []
    return client.list(hdfs_dir)


def download_dir(hdfs_path: str, local_dir: str) -> str:
    """Télécharge un fichier/dossier HDFS en local via WebHDFS.

    Utilisé par le loader Postgres pour lire du Parquet sans dépendre de
    `libhdfs` (natif, absent des images Airflow/Python standards) : on
    rapatrie les fichiers localement puis on les lit avec pandas/pyarrow.
    """
    client = get_client()
    client.download(hdfs_path, local_dir, overwrite=True)
    logger.info("Downloaded hdfs://%s -> %s", hdfs_path, local_dir)
    return local_dir


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m src.datalake.hdfs_writer <local_json_path>")
    upload_raw_file(Path(sys.argv[1]))
