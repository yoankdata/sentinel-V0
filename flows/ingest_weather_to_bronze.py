from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
from prefect import flow, task, get_run_logger
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# -----------------------
# Settings (via env vars)
# -----------------------

@dataclass(frozen=True)
class Settings:
    # Required
    openweather_api_key: str
    gcs_bucket: str

    # Optional
    city_name: str = "Abidjan,CI"
    gcs_prefix: str = "bronze/weather"  # bronze/weather/YYYY/MM/DD/...
    request_timeout_sec: int = 20
    units: str = "metric"  # metric | imperial | standard


def load_settings() -> Settings:
    api_key = os.getenv("OPENWEATHER_API_KEY", "").strip()
    bucket = os.getenv("GCS_BRONZE_BUCKET", "sentinel-bronze").strip()

    if not api_key:
        raise ValueError("Missing env var: OPENWEATHER_API_KEY")
    if not bucket:
        raise ValueError("Missing env var: GCS_BRONZE_BUCKET")

    return Settings(
        openweather_api_key=api_key,
        gcs_bucket=bucket,
        city_name=os.getenv("WEATHER_CITY", "Abidjan,CI").strip(),
        gcs_prefix=os.getenv("GCS_BRONZE_PREFIX", "bronze/weather").strip(),
        request_timeout_sec=int(os.getenv("REQUEST_TIMEOUT_SEC", "20")),
        units=os.getenv("OPENWEATHER_UNITS", "metric").strip(),
    )


def _utc_partition(now: Optional[datetime] = None) -> tuple[str, str, str]:
    now = now or datetime.now(timezone.utc)
    return now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")


def _build_gcs_object_path(settings: Settings, now: Optional[datetime] = None) -> str:
    yyyy, mm, dd = _utc_partition(now)
    # Required by spec: bronze/weather/YYYY/MM/DD/data.json
    return f"{settings.gcs_prefix}/{yyyy}/{mm}/{dd}/data.json"


# -----------------------
# Tasks
# -----------------------

@task(retries=3, retry_delay_seconds=[10, 30, 90])
def fetch_weather(settings: Settings) -> Dict[str, Any]:
    """
    Fetch raw data from OpenWeather Current Weather API, enrich with minimal metadata.
    Retries (with backoff) handled by Prefect.
    """
    logger = get_run_logger()

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": settings.city_name,
        "appid": settings.openweather_api_key,
        "units": settings.units,
    }

    logger.info(f"Calling OpenWeather | city={settings.city_name} | units={settings.units}")

    try:
        resp = requests.get(url, params=params, timeout=settings.request_timeout_sec)
    except requests.RequestException as e:
        # Retry-worthy (network layer)
        raise RuntimeError(f"Network error calling OpenWeather: {e}") from e

    # Retry on 5xx (provider-side issues)
    if 500 <= resp.status_code <= 599:
        raise RuntimeError(f"OpenWeather 5xx: {resp.status_code} | {resp.text[:200]}")

    # Fail fast on 4xx (usually config: API key/city)
    if resp.status_code != 200:
        raise RuntimeError(f"OpenWeather non-200: {resp.status_code} | {resp.text[:200]}")

    payload = resp.json()

    enriched = {
        "_meta": {
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": "openweather",
            "endpoint": "current_weather",
            "city": settings.city_name,
            "units": settings.units,
            "http_status": resp.status_code,
        },
        "data": payload,
    }

    return enriched


@task
def write_to_gcs(settings: Settings, record: Dict[str, Any]) -> str:
    """
    Write raw JSON (Bronze) to GCS using ADC credentials.
    """
    logger = get_run_logger()

    object_path = _build_gcs_object_path(settings)
    payload_bytes = json.dumps(record, ensure_ascii=False).encode("utf-8")

    # ADC: uses local gcloud ADC or workload identity / service account in cloud
    client = storage.Client()
    bucket = client.bucket(settings.gcs_bucket)
    blob = bucket.blob(object_path)

    logger.info(f"Writing Bronze JSON -> gs://{settings.gcs_bucket}/{object_path} ({len(payload_bytes)} bytes)")
    blob.upload_from_string(payload_bytes, content_type="application/json")

    return f"gs://{settings.gcs_bucket}/{object_path}"


# -----------------------
# Flow
# -----------------------

@flow(name="sentinel_ingest_weather_to_bronze")
def ingest_weather_to_bronze() -> str:
    """
    Orchestrates:
      - fetch_weather (retries + backoff)
      - write_to_gcs (single attempt; if it fails, the flow fails)
    """
    settings = load_settings()
    record = fetch_weather(settings)
    gcs_uri = write_to_gcs(settings, record)
    return gcs_uri


if __name__ == "__main__":
    print(ingest_weather_to_bronze())
