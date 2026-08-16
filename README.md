# Online Retail Sales Data Engineering

A reproducible ETL pipeline and analytical warehouse for the UCI-style Online
Retail dataset. The solution ingests CSV data, archives immutable source
versions, validates and cleans records, builds a star schema in DuckDB, exports
partitioned Parquet, monitors quality, and records lineage.

## Quick start

Python 3.11+ is required. On Windows, from the project root:

```powershell
python run_pipeline.py run
python -m unittest discover -s tests -v
python scripts\inspect_warehouse.py
```

The supplied dataset is expected at `online_retail.csv`. Configuration and
quality thresholds are in `config/pipeline.json`.

Docker is optional:

```bash
docker compose build
docker compose run --rm etl
```

DuckDB was chosen because this is a single-node analytical assignment: it is
serverless, columnar, fast on CSV/Parquet, and makes the submission immediately
reproducible. Docker supplies environment reproducibility; it is not used to add
an unnecessary database server. For multi-user production deployment, migrate
the same star schema to PostgreSQL, Snowflake, BigQuery, or a lakehouse.

## Architecture

```text
online_retail.csv
       |
       v
immutable SHA-256 archive ----> metadata.dataset_versions
       |
       v
raw.online_retail  (source values preserved as text)
       |
       v
staging.typed_sales (types, trimming, standardized case, derived amounts)
       |                         \
       |                          +--> quarantine.invalid_sales
       |                          +--> quarantine.duplicate_sales
       v
staging.valid_sales (deduplicated; anomalies flagged)
       |
       v
warehouse dimensions + fact_sales
       |                    \
       v                     +--> quality checks, alerts, lineage
year/month Parquet           +--> cached daily summary
```

## Data rules

- The raw layer is immutable within a run and keeps every source value as text.
- The CSV contains eight columns. A viewer column labelled `#` is a generated row
  index and is not source data.
- Whitespace is trimmed; identifiers/descriptions are standardized to uppercase;
  blank countries become `Unknown`.
- Invalid identifiers, dates, quantities, and prices are quarantined with reasons.
- Zero quantities and negative prices are invalid. A zero price is retained because
  samples and promotions can legitimately be free.
- Exact duplicate business records are quarantined. The first occurrence is kept.
- Invoice numbers beginning with `C`, or negative quantities, are classified as
  returns. Their signed value contributes negatively to net sales.
- Missing customer IDs are retained and linked to customer key `0` (`Unknown`),
  because anonymous sales remain economically valid.
- Quantity and price values above the global 99th percentile are flagged, not
  removed. Statistical rarity alone is not proof of bad retail data.

## Outputs

| Location | Purpose |
|---|---|
| `data/warehouse/retail.duckdb` | Raw, staging, quarantine, warehouse, quality, metadata, and cache schemas |
| `data/archive/<hash>/` | Immutable source snapshot and SHA-256 manifest |
| `data/exports/sales/version=<hash>/` | Versioned ZSTD Parquet, partitioned by invoice year and month |
| `data/quality/quality_<run>.json` | Machine-readable checks for each run |
| `data/quality/alerts.jsonl` | Append-only failed-check alert stream |
| `logs/pipeline.log` | Operational event log |

Generated data is excluded from Git because it is reproducible from the source.

## Scheduling

For Windows Task Scheduler, create a daily task whose program is `powershell.exe`
and argument is:

```text
-NoProfile -ExecutionPolicy Bypass -File "<project>\scripts\run_daily.ps1"
```

Linux cron equivalent (daily at 02:00):

```cron
0 2 * * * /absolute/project/path/scripts/run_daily.sh >> /absolute/project/path/logs/scheduler.log 2>&1
```

In production, an orchestrator such as Airflow, Dagster, Prefect, or a managed
cloud scheduler should call the same idempotent command and route failed alerts
to email/Slack/PagerDuty.

## Repository map

- `src/retail_etl/pipeline.py`: end-to-end ETL and quality logic
- `config/pipeline.json`: paths and quality thresholds
- `sql/analytics.sql`: reporting and monitoring queries
- `tests/test_warehouse.py`: warehouse integration tests
- `docs/DATA_MODEL.md`: schema, lineage, and indexing rationale
- `docs/OPERATIONS.md`: monitoring, recovery, and improvement roadmap
