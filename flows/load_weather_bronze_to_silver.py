from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# -----------------------
# Settings
# -----------------------

@dataclass(frozen=True)
class Settings:
    gcs_bucket: str
    gcs_prefix: str  # bronze/weather
    bq_project: str
    bq_dataset: str  # sentinel_silver
    bq_table: str    # weather_observations

def load_settings() -> Settings:
    bucket = os.getenv("GCS_BRONZE_BUCKET", "sentinel-bronze").strip()
    prefix = os.getenv("GCS_BRONZE_PREFIX", "bronze/weather").strip()

    # Fallback to simple gcloud query or hardcoded if env var missing, but user provided env vars usually
    project = os.getenv("BQ_PROJECT", os.getenv("GCLOUD_PROJECT", "spherical-booth-474518-n6")).strip()
    dataset = os.getenv("BQ_SILVER_DATASET", "sentinel_silver").strip()
    table = os.getenv("BQ_SILVER_TABLE", "weather_observations").strip()

    if not bucket:
        raise ValueError("Missing GCS_BRONZE_BUCKET")
    if not prefix:
        raise ValueError("Missing GCS_BRONZE_PREFIX")
    if not project:
        raise ValueError("Missing BQ_PROJECT (ex: my-gcp-project-id)")

    return Settings(
        gcs_bucket=bucket,
        gcs_prefix=prefix,
        bq_project=project,
        bq_dataset=dataset,
        bq_table=table,
    )


# -----------------------
# 1) Read latest Bronze JSON from GCS
# -----------------------

def read_latest_bronze_from_gcs(settings: Settings) -> Tuple[Dict[str, Any], str]:
    """
    Finds and downloads the latest Bronze file from:
      gs://{bucket}/{prefix}/YYYY/MM/DD/data.json

    Strategy: list blobs under prefix and take the lexicographically last name.
    Returns: (record_dict, gcs_uri)
    """
    client = storage.Client()
    bucket = client.bucket(settings.gcs_bucket)

    blobs = list(client.list_blobs(settings.gcs_bucket, prefix=f"{settings.gcs_prefix}/"))
    if not blobs:
        raise FileNotFoundError(f"No blobs found under gs://{settings.gcs_bucket}/{settings.gcs_prefix}/")

    # Lexicographic order works because path contains YYYY/MM/DD
    latest_blob = sorted(blobs, key=lambda b: b.name)[-1]

    # Guard: only accept data.json objects (optional but keeps it clean)
    if not latest_blob.name.endswith("/data.json"):
        # Try to find the latest data.json specifically
        candidates = [b for b in blobs if b.name.endswith("/data.json")]
        if not candidates:
            raise FileNotFoundError("No data.json found under prefix")
        latest_blob = sorted(candidates, key=lambda b: b.name)[-1]

    raw = latest_blob.download_as_bytes()
    record = json.loads(raw.decode("utf-8"))

    gcs_uri = f"gs://{settings.gcs_bucket}/{latest_blob.name}"
    return record, gcs_uri


# -----------------------
# 2) Transform to Silver schema (minimal cleaning/typing)
# -----------------------

def transform_to_silver(record: Dict[str, Any]) -> pd.DataFrame:
    """
    Converts the Bronze record into a 1-row Silver dataframe.
    Minimal rules:
      - types
      - observed_at_utc from OpenWeather dt (unix seconds)
      - nulls instead of missing
      - keep raw_main/raw_temp etc as columns
    """
    data = record.get("data", {}) or {}
    meta = record.get("_meta", {}) or {}

    # OpenWeather typical fields
    city = data.get("name")
    country = (data.get("sys") or {}).get("country")

    # observed timestamp from API payload
    dt_unix = data.get("dt")  # seconds
    observed_at_utc = None
    if isinstance(dt_unix, (int, float)):
        observed_at_utc = datetime.fromtimestamp(dt_unix, tz=timezone.utc).isoformat()

    # coordinates
    coord = data.get("coord") or {}
    lat = coord.get("lat")
    lon = coord.get("lon")

    # main weather metrics
    main = data.get("main") or {}
    temp_c = main.get("temp")
    feels_like_c = main.get("feels_like")
    humidity = main.get("humidity")
    pressure = main.get("pressure")

    # wind
    wind = data.get("wind") or {}
    wind_speed = wind.get("speed")
    wind_deg = wind.get("deg")

    # weather array (take first)
    weather0 = (data.get("weather") or [{}])[0] or {}
    weather_main = weather0.get("main")
    weather_desc = weather0.get("description")

    fetched_at_utc = meta.get("fetched_at_utc")

    row = {
        "observed_at_utc": observed_at_utc,   # will be loaded as TIMESTAMP
        "fetched_at_utc": fetched_at_utc,     # TIMESTAMP
        "city": city,
        "country": country,
        "lat": lat,
        "lon": lon,
        "temp_c": temp_c,
        "feels_like_c": feels_like_c,
        "humidity_pct": humidity,
        "pressure_hpa": pressure,
        "wind_speed_ms": wind_speed,
        "wind_deg": wind_deg,
        "weather_main": weather_main,
        "weather_desc": weather_desc,
    }

    df = pd.DataFrame([row])

    # Basic typing coercions (keeps NULL when invalid)
    for col in ["lat", "lon", "temp_c", "feels_like_c", "wind_speed_ms"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["humidity_pct", "pressure_hpa", "wind_deg"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Ensure timestamps are parseable
    for col in ["observed_at_utc", "fetched_at_utc"]:
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    return df


# -----------------------
# 3) Load to BigQuery Staging (truncate)
# -----------------------

def load_to_staging(settings: Settings, df: pd.DataFrame) -> str:
    """
    Loads dataframe to Staging table (WRITE_TRUNCATE):
      {project}.{dataset}.{table}_staging
    """
    if df.empty:
        raise ValueError("Silver dataframe is empty, nothing to load")

    client = bigquery.Client(project=settings.bq_project, location="europe-west9")
    staging_table_id = f"{settings.bq_project}.{settings.bq_dataset}.{settings.bq_table}_staging"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
    job.result()  # wait

    return staging_table_id


# -----------------------
# 4) Merge Staging to Target
# -----------------------

def merge_staging_to_target(settings: Settings) -> None:
    """
    Executes MERGE statement to upsert from Staging to Target.
    Idempotency key: (city, observed_at_utc)
    """
    client = bigquery.Client(project=settings.bq_project, location="europe-west9")
    
    target_table = f"{settings.bq_project}.{settings.bq_dataset}.{settings.bq_table}"
    staging_table = f"{settings.bq_project}.{settings.bq_dataset}.{settings.bq_table}_staging"

    merge_query = f"""
    MERGE `{target_table}` T
    USING `{staging_table}` S
    ON T.city = S.city AND T.observed_at_utc = S.observed_at_utc
    WHEN MATCHED THEN
      UPDATE SET
        fetched_at_utc = S.fetched_at_utc,
        country = S.country,
        lat = S.lat,
        lon = S.lon,
        temp_c = S.temp_c,
        feels_like_c = S.feels_like_c,
        humidity_pct = S.humidity_pct,
        pressure_hpa = S.pressure_hpa,
        wind_speed_ms = S.wind_speed_ms,
        wind_deg = S.wind_deg,
        weather_main = S.weather_main,
        weather_desc = S.weather_desc
    WHEN NOT MATCHED THEN
      INSERT ROW;
    """

    job = client.query(merge_query)
    job.result()


# -----------------------
# Minimal runnable entrypoint
# -----------------------

def main() -> None:
    settings = load_settings()

    record, gcs_uri = read_latest_bronze_from_gcs(settings)
    df = transform_to_silver(record)

    # 1. Load to Staging
    staging_table = load_to_staging(settings, df)
    print(f"Bronze used: {gcs_uri}")
    print(f"Loaded {len(df)} rows to Staging: {staging_table}")

    # 2. Merge to Target
    merge_staging_to_target(settings)
    print(f"Merged Staging -> Target: {settings.bq_project}.{settings.bq_dataset}.{settings.bq_table}")



if __name__ == "__main__":
    main()
