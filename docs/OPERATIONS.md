# Operations and improvement roadmap

## Daily operation

1. Place or atomically replace the source CSV at the configured path.
2. Run `python run_pipeline.py run` through the scheduler.
3. Confirm exit code zero and `SUCCESS` in `metadata.etl_runs`.
4. Review `data/quality/alerts.jsonl` when any threshold fails.
5. Run `python run_pipeline.py check` for an on-demand health check.

Each distinct file is archived under its SHA-256 checksum. Re-running an unchanged
file reuses the snapshot, rebuilds deterministic warehouse tables, and records a
new operational run. A failed run records its exception while the source snapshot
and previous historical run records remain available.

## Alert routing

The local assignment deliberately writes alerts to an append-only file and exits
non-zero on critical failures. In production, connect that output to the platform
standard: CloudWatch/Azure Monitor, email, Slack, Teams, or PagerDuty. Alert on
pipeline failure, freshness delay, schema drift, volume change, rejection rate,
and referential-integrity failure.

## Recommended next improvements

1. Change full refresh to incremental ingestion using source arrival time and a
   durable invoice-line business key.
2. Add an orchestrator with retries, backfills, service-level objectives, and
   centralized secrets.
3. Move to object storage plus a catalog/lakehouse or a managed warehouse when
   concurrency and scale require it.
4. Introduce product/customer master data and slowly changing dimensions.
5. Learn anomaly thresholds by product/category and season instead of using a
   global percentile.
6. Add reconciliation against upstream control totals and downstream dashboards.
7. Add encryption, role-based access, retention policies, and customer-ID masking.
8. Add CI to run unit tests, linting, a small-fixture ETL, and Docker build checks.

