"""Ingestion : récupère les prévisions météo brutes depuis l'API Open-Meteo.

Ce module ne fait qu'une chose : appeler l'API et renvoyer le JSON brut,
horodaté, prêt à être déposé dans le Data Lake par `src.datalake.hdfs_writer`.
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from src.config import WEATHER_API

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_HOURLY_FIELDS = "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation"


def fetch_weather(
    latitude: float = WEATHER_API.latitude,
    longitude: float = WEATHER_API.longitude,
    hourly_fields: str = DEFAULT_HOURLY_FIELDS,
    timeout: int = 15,
) -> dict[str, Any]:
    """Appelle l'API Open-Meteo et retourne la réponse JSON brute."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": hourly_fields,
        "timezone": "auto",
    }
    logger.info("Fetching weather data for lat=%s lon=%s", latitude, longitude)
    response = requests.get(WEATHER_API.base_url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def save_raw_locally(payload: dict[str, Any], output_dir: str = "data/raw") -> Path:
    """Sauvegarde le payload brut en JSON, horodaté (pré-étape avant dépôt HDFS)."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    file_path = out_dir / f"weather_{WEATHER_API.city.lower()}_{timestamp}.json"
    file_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Raw payload written to %s", file_path)
    return file_path


def run() -> Path:
    payload = fetch_weather()
    return save_raw_locally(payload)


if __name__ == "__main__":
    run()
