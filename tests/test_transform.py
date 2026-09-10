import json

import pytest

from src.transform.spark_transform import get_spark, transform

SAMPLE_RAW = {
    "latitude": 48.86,
    "longitude": 2.35,
    "hourly": {
        "time": ["2026-01-01T00:00", "2026-01-01T01:00"],
        "temperature_2m": [4.2, 3.9],
        "relative_humidity_2m": [88.0, 90.0],
        "wind_speed_10m": [10.1, 9.8],
        "precipitation": [0.0, 0.1],
    },
}


@pytest.fixture(scope="module")
def spark():
    session = get_spark(app_name="test-weather-etl")
    yield session
    session.stop()


def test_transform_flattens_hourly_arrays_into_rows(spark, tmp_path):
    raw_file = tmp_path / "raw.json"
    raw_file.write_text(json.dumps(SAMPLE_RAW), encoding="utf-8")

    df = transform(spark, str(raw_file))
    rows = df.orderBy("observed_at").collect()

    assert len(rows) == 2
    assert rows[0]["temperature_c"] == pytest.approx(4.2)
    assert rows[1]["temperature_c"] == pytest.approx(3.9)
    assert {"latitude", "longitude", "observed_at", "temperature_c", "humidity_pct",
            "wind_speed_kmh", "precipitation_mm"} == set(df.columns)
