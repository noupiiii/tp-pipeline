"""Configuration centralisée du pipeline, lue depuis les variables d'environnement.

Toutes les valeurs par défaut correspondent à `.env.example` pour permettre une
exécution locale sans configuration supplémentaire (hors Data Lake/DWH qui
nécessitent les conteneurs Docker).
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class WeatherApiConfig:
    base_url: str = os.getenv("WEATHER_API_BASE_URL", "https://api.open-meteo.com/v1/forecast")
    latitude: float = float(os.getenv("WEATHER_LATITUDE", "48.8566"))
    longitude: float = float(os.getenv("WEATHER_LONGITUDE", "2.3522"))
    city: str = os.getenv("WEATHER_CITY", "Paris")


@dataclass(frozen=True)
class HdfsConfig:
    namenode_host: str = os.getenv("HDFS_NAMENODE_HOST", "namenode")
    namenode_port: int = int(os.getenv("HDFS_NAMENODE_PORT", "9870"))
    raw_path: str = os.getenv("HDFS_RAW_PATH", "/data-lake/raw/weather")

    @property
    def webhdfs_url(self) -> str:
        return f"http://{self.namenode_host}:{self.namenode_port}"


@dataclass(frozen=True)
class PostgresConfig:
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    db: str = os.getenv("POSTGRES_DB", "dwh")
    user: str = os.getenv("POSTGRES_USER", "dwh_user")
    password: str = os.getenv("POSTGRES_PASSWORD", "change_me")
    table: str = os.getenv("POSTGRES_TABLE", "weather_hourly")

    @property
    def sqlalchemy_url(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


WEATHER_API = WeatherApiConfig()
HDFS = HdfsConfig()
POSTGRES = PostgresConfig()
