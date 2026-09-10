import pandas as pd
from sqlalchemy import create_engine, text

from src.load.postgres_loader import load_dataframe


def _make_sqlite_engine_with_schema():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE weather_hourly (
                    latitude DOUBLE, longitude DOUBLE, observed_at TEXT,
                    temperature_c DOUBLE,
                    UNIQUE (latitude, longitude, observed_at)
                )
                """
            )
        )
    return engine


def test_load_dataframe_inserts_all_new_rows(mocker):
    engine = _make_sqlite_engine_with_schema()
    mocker.patch("src.load.postgres_loader.get_engine", return_value=engine)
    df = pd.DataFrame(
        {
            "latitude": [48.86, 48.86],
            "longitude": [2.35, 2.35],
            "observed_at": ["2026-01-01T00:00:00", "2026-01-01T01:00:00"],
            "temperature_c": [4.2, 3.9],
        }
    )

    inserted = load_dataframe(df, table="weather_hourly")

    assert inserted == 2
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM weather_hourly")).scalar() == 2


def test_load_dataframe_skips_rows_already_loaded(mocker):
    """A DAG re-run reprocesses every raw file, so it re-produces rows a previous
    run already loaded — load_dataframe must skip those instead of erroring."""
    engine = _make_sqlite_engine_with_schema()
    mocker.patch("src.load.postgres_loader.get_engine", return_value=engine)
    df = pd.DataFrame(
        {
            "latitude": [48.86],
            "longitude": [2.35],
            "observed_at": ["2026-01-01T00:00:00"],
            "temperature_c": [4.2],
        }
    )
    load_dataframe(df, table="weather_hourly")

    second_run_df = pd.concat(
        [df, pd.DataFrame({"latitude": [48.86], "longitude": [2.35], "observed_at": ["2026-01-01T01:00:00"], "temperature_c": [3.9]})],
        ignore_index=True,
    )
    inserted = load_dataframe(second_run_df, table="weather_hourly")

    assert inserted == 1
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM weather_hourly")).scalar() == 2
