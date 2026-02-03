# Google Cloud Platform Infrastructure

## Buckets
- `sentinel-bronze`: Storage for raw JSON data ingested from APIs.
- `sentinel-silver`: Storage for processed/cleaned data (if using data lakehouse pattern not just BQ).

## BigQuery Datasets
- `sentinel_bronze`: Raw data tables.
- `sentinel_silver`: Cleaned/modeled data.
- `sentinel_gold`: Aggregated/serving data.

## Service Accounts
- `prefect-worker`: Used by Prefect to execute flows. Needs `Storage Object Admin` and `BigQuery Data Editor`.
