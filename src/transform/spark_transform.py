"""Transformation : aplati le JSON météo brut (Open-Meteo) en table tabulaire.

Le job tourne avec PySpark (master `local[*]` par défaut, ou pointé sur
`spark://spark-master:7077` en conteneur) et peut lire aussi bien un fichier
local qu'un chemin HDFS (`hdfs://namenode:9000/...`).
"""
from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_SCHEMA = StructType(
    [
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField(
            "hourly",
            StructType(
                [
                    StructField("time", ArrayType(StringType())),
                    StructField("temperature_2m", ArrayType(DoubleType())),
                    StructField("relative_humidity_2m", ArrayType(DoubleType())),
                    StructField("wind_speed_10m", ArrayType(DoubleType())),
                    StructField("precipitation", ArrayType(DoubleType())),
                ]
            ),
        ),
    ]
)


def get_spark(app_name: str = "weather-etl", master: str = "local[*]") -> SparkSession:
    return SparkSession.builder.appName(app_name).master(master).getOrCreate()


def load_raw(spark: SparkSession, path: str) -> DataFrame:
    """Charge un (ou plusieurs) fichier(s) JSON brut Open-Meteo."""
    return spark.read.schema(RAW_SCHEMA).json(path, multiLine=True)


def flatten_hourly(df: DataFrame) -> DataFrame:
    """Explose les tableaux `hourly.*` en une ligne par timestamp horaire."""
    zipped = df.select(
        "latitude",
        "longitude",
        F.arrays_zip(
            F.col("hourly.time").alias("time"),
            F.col("hourly.temperature_2m").alias("temperature_2m"),
            F.col("hourly.relative_humidity_2m").alias("relative_humidity_2m"),
            F.col("hourly.wind_speed_10m").alias("wind_speed_10m"),
            F.col("hourly.precipitation").alias("precipitation"),
        ).alias("hourly_zipped"),
    )
    exploded = zipped.withColumn("hourly_row", F.explode("hourly_zipped"))
    return exploded.select(
        "latitude",
        "longitude",
        F.to_timestamp(F.col("hourly_row.time")).alias("observed_at"),
        F.col("hourly_row.temperature_2m").alias("temperature_c"),
        F.col("hourly_row.relative_humidity_2m").alias("humidity_pct"),
        F.col("hourly_row.wind_speed_10m").alias("wind_speed_kmh"),
        F.col("hourly_row.precipitation").alias("precipitation_mm"),
    )


def transform(spark: SparkSession, input_path: str) -> DataFrame:
    raw_df = load_raw(spark, input_path)
    return flatten_hourly(raw_df)


def write_parquet(df: DataFrame, output_path: str) -> None:
    df.write.mode("append").parquet(output_path)
    logger.info("Written transformed data to %s", output_path)


def run(input_path: str, output_path: str) -> dict[str, Any]:
    spark = get_spark()
    try:
        df = transform(spark, input_path)
        write_parquet(df, output_path)
        return {"rows_written": df.count(), "output_path": output_path}
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        raise SystemExit("Usage: spark-submit spark_transform.py <input_path> <output_path>")
    run(sys.argv[1], sys.argv[2])
