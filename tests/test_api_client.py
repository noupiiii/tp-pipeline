import json

import pytest
import responses

from src.config import WEATHER_API
from src.ingestion.api_client import fetch_weather, save_raw_locally

SAMPLE_RESPONSE = {
    "latitude": 48.86,
    "longitude": 2.35,
    "hourly": {
        "time": ["2026-01-01T00:00", "2026-01-01T01:00"],
        "temperature_2m": [4.2, 3.9],
        "relative_humidity_2m": [88, 90],
        "wind_speed_10m": [10.1, 9.8],
        "precipitation": [0.0, 0.1],
    },
}


@responses.activate
def test_fetch_weather_returns_json_payload():
    responses.add(
        responses.GET,
        WEATHER_API.base_url,
        json=SAMPLE_RESPONSE,
        status=200,
    )

    payload = fetch_weather()

    assert payload == SAMPLE_RESPONSE


@responses.activate
def test_fetch_weather_raises_on_http_error():
    responses.add(responses.GET, WEATHER_API.base_url, status=500)

    with pytest.raises(Exception, match="500"):
        fetch_weather()


def test_save_raw_locally_writes_json_file(tmp_path):
    output_dir = tmp_path / "raw"

    file_path = save_raw_locally(SAMPLE_RESPONSE, output_dir=str(output_dir))

    assert file_path.exists()
    assert json.loads(file_path.read_text(encoding="utf-8")) == SAMPLE_RESPONSE
