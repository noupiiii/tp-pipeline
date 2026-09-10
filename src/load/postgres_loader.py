"""Chargement : écrit les données transformées (Parquet ou DataFrame pandas)
dans le Data Warehouse PostgreSQL.
"""
from __future__ import annotations

import logging
import re
import tempfile
import uuid
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config import POSTGRES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_HDFS_URI_RE = re.compile(r"^hdfs://[^/]+(?P<path>/.*)$")

# Matches the UNIQUE constraint in sql/init.sql. Each Airflow run re-transforms
# every raw file ever ingested (Spark has no notion of "only the new one"), so
# the same hourly forecast rows are re-produced run after run — the load step
# must be idempotent or a second run fails on a duplicate-key error.
CONFLICT_KEY_COLUMNS = ("latitude", "longitude", "observed_at")


def get_engine() -> Engine:
    return create_engine(POSTGRES.sqlalchemy_url)


def load_parquet_to_postgres(
    parquet_path: str,
    table: str = POSTGRES.table,
) -> int:
    """Lit un dossier/fichier Parquet (local ou HDFS) et l'insère dans PostgreSQL.

    Un chemin `hdfs://...` est d'abord rapatrié localement via WebHDFS (voir
    `src.datalake.hdfs_writer.download_dir`) : lire du Parquet directement sur
    une URI hdfs:// avec pandas/pyarrow nécessite `libhdfs`, une lib native
    absente des images Python/Airflow standards.
    """
    hdfs_match = _HDFS_URI_RE.match(parquet_path)
    if hdfs_match:
        from src.datalake.hdfs_writer import download_dir

        with tempfile.TemporaryDirectory() as tmp_dir:
            local_dir = download_dir(hdfs_match.group("path"), str(Path(tmp_dir) / "parquet"))
            df = pd.read_parquet(local_dir)
    else:
        df = pd.read_parquet(parquet_path)
    return load_dataframe(df, table=table)


def load_dataframe(
    df: pd.DataFrame,
    table: str = POSTGRES.table,
    conflict_key_columns: tuple[str, ...] = CONFLICT_KEY_COLUMNS,
) -> int:
    """Upsert un DataFrame dans PostgreSQL (idempotent sur `conflict_key_columns`).

    Passe par une table de staging temporaire puis `INSERT ... ON CONFLICT DO
    NOTHING`, car chaque run Airflow retraite l'ensemble des fichiers bruts et
    régénère donc des lignes déjà chargées lors d'un run précédent.
    """
    engine = get_engine()
    staging_table = f"_stg_{table}_{uuid.uuid4().hex[:8]}"
    columns = list(df.columns)
    conflict_clause = ", ".join(conflict_key_columns)
    column_list = ", ".join(columns)

    with engine.begin() as conn:
        df.to_sql(staging_table, conn, if_exists="replace", index=False)
        result = conn.execute(
            text(
                # The WHERE clause is required for SQLite (used by the test suite)
                # to disambiguate an INSERT...SELECT from an upsert-clause; harmless
                # no-op on PostgreSQL.
                f'INSERT INTO "{table}" ({column_list}) '
                f'SELECT {column_list} FROM "{staging_table}" WHERE 1=1 '
                f"ON CONFLICT ({conflict_clause}) DO NOTHING"
            )
        )
        conn.execute(text(f'DROP TABLE "{staging_table}"'))

    inserted = result.rowcount
    logger.info(
        "Loaded %d/%d row(s) into %s.%s (%d duplicate(s) skipped)",
        inserted,
        len(df),
        POSTGRES.db,
        table,
        len(df) - inserted,
    )
    return inserted


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m src.load.postgres_loader <parquet_path>")
    load_parquet_to_postgres(sys.argv[1])
