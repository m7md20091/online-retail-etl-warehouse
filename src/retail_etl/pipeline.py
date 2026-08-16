from __future__ import annotations

import csv
import hashlib
import json
import logging
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from .config import Settings


EXPECTED_COLUMNS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]


class DataQualityError(RuntimeError):
    """Raised when a critical data-quality rule fails."""


class RetailPipeline:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.settings.create_directories()
        self.logger = self._logger()

    @classmethod
    def from_config(cls, config_path: str | Path) -> "RetailPipeline":
        return cls(Settings.load(config_path))

    def _logger(self) -> logging.Logger:
        logger = logging.getLogger("retail_etl")
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%S"
            )
            stream = logging.StreamHandler()
            stream.setFormatter(formatter)
            logger.addHandler(stream)
            file_handler = logging.FileHandler(
                self.settings.log_dir / "pipeline.log", encoding="utf-8"
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        return logger

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest()

    def _validate_source(self) -> None:
        if not self.settings.source_file.is_file():
            raise FileNotFoundError(f"Source not found: {self.settings.source_file}")
        with self.settings.source_file.open("r", encoding="utf-8-sig", newline="") as handle:
            header = next(csv.reader(handle), [])
        if header != EXPECTED_COLUMNS:
            raise DataQualityError(
                f"Schema drift detected. Expected {EXPECTED_COLUMNS}, received {header}"
            )

    def _archive(self, source_hash: str) -> Path:
        version_dir = self.settings.archive_dir / source_hash[:12]
        version_dir.mkdir(parents=True, exist_ok=True)
        archived = version_dir / self.settings.source_file.name
        if not archived.exists():
            shutil.copy2(self.settings.source_file, archived)
            manifest = {
                "sha256": source_hash,
                "source": str(self.settings.source_file),
                "archived_at_utc": datetime.now(timezone.utc).isoformat(),
                "bytes": self.settings.source_file.stat().st_size,
            }
            (version_dir / "manifest.json").write_text(
                json.dumps(manifest, indent=2), encoding="utf-8"
            )
        return archived

    @staticmethod
    def _scalar(connection: duckdb.DuckDBPyConnection, sql: str) -> Any:
        return connection.execute(sql).fetchone()[0]

    def _create_control_tables(self, con: duckdb.DuckDBPyConnection) -> None:
        con.execute("CREATE SCHEMA IF NOT EXISTS metadata")
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute("CREATE SCHEMA IF NOT EXISTS staging")
        con.execute("CREATE SCHEMA IF NOT EXISTS quarantine")
        con.execute("CREATE SCHEMA IF NOT EXISTS warehouse")
        con.execute("CREATE SCHEMA IF NOT EXISTS quality")
        con.execute("CREATE SCHEMA IF NOT EXISTS cache")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata.etl_runs (
                run_id VARCHAR PRIMARY KEY,
                source_hash VARCHAR,
                source_path VARCHAR,
                started_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                status VARCHAR,
                raw_rows BIGINT,
                clean_rows BIGINT,
                rejected_rows BIGINT,
                duplicate_rows BIGINT,
                error_message VARCHAR
            );
            CREATE TABLE IF NOT EXISTS metadata.dataset_versions (
                source_hash VARCHAR PRIMARY KEY,
                archived_path VARCHAR,
                file_bytes BIGINT,
                first_seen_at TIMESTAMPTZ,
                last_processed_at TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS metadata.data_lineage (
                run_id VARCHAR,
                target_object VARCHAR,
                source_object VARCHAR,
                transformation VARCHAR,
                recorded_at TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS quality.check_results (
                run_id VARCHAR,
                check_name VARCHAR,
                severity VARCHAR,
                observed_value DOUBLE,
                threshold VARCHAR,
                passed BOOLEAN,
                checked_at TIMESTAMPTZ
            );
            """
        )

    def _load_and_transform(self, con: duckdb.DuckDBPyConnection, source: Path) -> None:
        source_sql = str(source).replace("'", "''")
        con.execute(
            f"""
            CREATE OR REPLACE TABLE raw.online_retail AS
            SELECT row_number() OVER () AS source_row_number, *
            FROM read_csv('{source_sql}', header=true, all_varchar=true,
                columns={{
                    'InvoiceNo':'VARCHAR', 'StockCode':'VARCHAR',
                    'Description':'VARCHAR', 'Quantity':'VARCHAR',
                    'InvoiceDate':'VARCHAR', 'UnitPrice':'VARCHAR',
                    'CustomerID':'VARCHAR', 'Country':'VARCHAR'
                }});

            CREATE OR REPLACE TABLE staging.typed_sales AS
            SELECT
                source_row_number,
                upper(trim(InvoiceNo)) AS invoice_no,
                upper(trim(StockCode)) AS stock_code,
                nullif(upper(trim(Description)), '') AS description,
                try_cast(Quantity AS INTEGER) AS quantity,
                try_strptime(InvoiceDate, '%Y-%m-%d %H:%M:%S') AS invoice_ts,
                try_cast(UnitPrice AS DECIMAL(18, 4)) AS unit_price,
                try_cast(try_cast(CustomerID AS DOUBLE) AS BIGINT) AS customer_id,
                coalesce(nullif(trim(Country), ''), 'Unknown') AS country,
                CASE
                    WHEN upper(trim(InvoiceNo)) LIKE 'C%' OR try_cast(Quantity AS INTEGER) < 0
                    THEN 'RETURN' ELSE 'SALE'
                END AS transaction_type,
                CASE
                    WHEN try_cast(Quantity AS INTEGER) IS NOT NULL
                     AND try_cast(UnitPrice AS DECIMAL(18, 4)) IS NOT NULL
                    THEN try_cast(Quantity AS INTEGER) * try_cast(UnitPrice AS DECIMAL(18, 4))
                END AS line_amount
            FROM raw.online_retail;

            CREATE OR REPLACE TABLE quarantine.invalid_sales AS
            SELECT *, concat_ws(';',
                CASE WHEN invoice_no IS NULL OR invoice_no = '' THEN 'missing_invoice' END,
                CASE WHEN stock_code IS NULL OR stock_code = '' THEN 'missing_stock_code' END,
                CASE WHEN quantity IS NULL THEN 'invalid_quantity' END,
                CASE WHEN quantity = 0 THEN 'zero_quantity' END,
                CASE WHEN invoice_ts IS NULL THEN 'invalid_invoice_date' END,
                CASE WHEN unit_price IS NULL THEN 'invalid_unit_price' END,
                CASE WHEN unit_price < 0 THEN 'negative_unit_price' END
            ) AS rejection_reason
            FROM staging.typed_sales
            WHERE invoice_no IS NULL OR invoice_no = ''
               OR stock_code IS NULL OR stock_code = ''
               OR quantity IS NULL OR quantity = 0
               OR invoice_ts IS NULL OR unit_price IS NULL OR unit_price < 0;

            CREATE OR REPLACE TABLE staging.valid_sales AS
            SELECT * EXCLUDE (duplicate_rank),
                abs(quantity) > quantile_cont(abs(quantity), 0.99) OVER () AS quantity_outlier,
                unit_price > quantile_cont(unit_price, 0.99) OVER () AS price_outlier
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY invoice_no, stock_code, description, quantity,
                                 invoice_ts, unit_price, customer_id, country
                    ORDER BY source_row_number
                ) AS duplicate_rank
                FROM staging.typed_sales
                WHERE source_row_number NOT IN
                    (SELECT source_row_number FROM quarantine.invalid_sales)
            ) deduplicated
            WHERE duplicate_rank = 1;

            CREATE OR REPLACE TABLE quarantine.duplicate_sales AS
            SELECT * EXCLUDE (duplicate_rank)
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY invoice_no, stock_code, description, quantity,
                                 invoice_ts, unit_price, customer_id, country
                    ORDER BY source_row_number
                ) AS duplicate_rank
                FROM staging.typed_sales
                WHERE source_row_number NOT IN
                    (SELECT source_row_number FROM quarantine.invalid_sales)
            ) duplicates
            WHERE duplicate_rank > 1;
            """
        )

    def _build_warehouse(self, con: duckdb.DuckDBPyConnection) -> None:
        con.execute(
            """
            CREATE OR REPLACE TABLE warehouse.dim_product AS
            WITH ranked AS (
                SELECT stock_code, coalesce(description, 'UNKNOWN PRODUCT') AS description,
                    row_number() OVER (
                        PARTITION BY stock_code
                        ORDER BY count(*) DESC, coalesce(description, 'UNKNOWN PRODUCT')
                    ) AS preference
                FROM staging.valid_sales
                GROUP BY stock_code, description
            )
            SELECT row_number() OVER (ORDER BY stock_code) AS product_key,
                   stock_code, description
            FROM ranked WHERE preference = 1;

            CREATE OR REPLACE TABLE warehouse.dim_customer AS
            WITH customers AS (
                SELECT customer_id, country, count(*) AS appearances,
                    row_number() OVER (
                        PARTITION BY customer_id ORDER BY count(*) DESC, country
                    ) AS preference
                FROM staging.valid_sales
                WHERE customer_id IS NOT NULL
                GROUP BY customer_id, country
            ), chosen AS (
                SELECT customer_id, country FROM customers WHERE preference = 1
            )
            SELECT 0::BIGINT AS customer_key, NULL::BIGINT AS customer_id,
                   'Unknown'::VARCHAR AS country
            UNION ALL
            SELECT row_number() OVER (ORDER BY customer_id) AS customer_key,
                   customer_id, country FROM chosen;

            CREATE OR REPLACE TABLE warehouse.dim_date AS
            SELECT DISTINCT
                cast(strftime(invoice_ts, '%Y%m%d') AS INTEGER) AS date_key,
                cast(invoice_ts AS DATE) AS full_date,
                year(invoice_ts) AS year,
                quarter(invoice_ts) AS quarter,
                month(invoice_ts) AS month,
                monthname(invoice_ts) AS month_name,
                day(invoice_ts) AS day,
                dayofweek(invoice_ts) AS day_of_week,
                dayname(invoice_ts) AS day_name,
                dayofweek(invoice_ts) IN (0, 6) AS is_weekend
            FROM staging.valid_sales;

            CREATE OR REPLACE TABLE warehouse.dim_invoice AS
            SELECT row_number() OVER (ORDER BY invoice_no) AS invoice_key,
                   invoice_no,
                   min(invoice_ts) AS invoice_ts,
                   any_value(transaction_type) AS transaction_type,
                   coalesce(max(customer_id), NULL) AS customer_id,
                   any_value(country) AS country
            FROM staging.valid_sales
            GROUP BY invoice_no;

            CREATE OR REPLACE TABLE warehouse.fact_sales AS
            SELECT
                sha256(concat_ws('|', cast(s.source_row_number AS VARCHAR), s.invoice_no,
                    s.stock_code, cast(s.invoice_ts AS VARCHAR))) AS sales_key,
                i.invoice_key,
                p.product_key,
                coalesce(c.customer_key, 0) AS customer_key,
                cast(strftime(s.invoice_ts, '%Y%m%d') AS INTEGER) AS date_key,
                s.invoice_ts,
                s.quantity,
                s.unit_price,
                s.line_amount,
                s.transaction_type,
                s.quantity_outlier,
                s.price_outlier,
                year(s.invoice_ts) AS partition_year,
                month(s.invoice_ts) AS partition_month
            FROM staging.valid_sales s
            JOIN warehouse.dim_product p USING (stock_code)
            JOIN warehouse.dim_invoice i USING (invoice_no)
            LEFT JOIN warehouse.dim_customer c ON s.customer_id = c.customer_id;

            CREATE OR REPLACE TABLE cache.daily_sales_summary AS
            SELECT d.full_date, d.year, d.month,
                   count(DISTINCT i.invoice_no) AS invoice_count,
                   sum(CASE WHEN f.transaction_type = 'SALE' THEN f.line_amount ELSE 0 END) AS gross_sales,
                   sum(CASE WHEN f.transaction_type = 'RETURN' THEN f.line_amount ELSE 0 END) AS returns,
                   sum(f.line_amount) AS net_sales
            FROM warehouse.fact_sales f
            JOIN warehouse.dim_date d USING (date_key)
            JOIN warehouse.dim_invoice i USING (invoice_key)
            GROUP BY d.full_date, d.year, d.month;

            CREATE INDEX IF NOT EXISTS idx_fact_date ON warehouse.fact_sales(date_key);
            CREATE INDEX IF NOT EXISTS idx_fact_product ON warehouse.fact_sales(product_key);
            CREATE INDEX IF NOT EXISTS idx_fact_customer ON warehouse.fact_sales(customer_key);
            CREATE INDEX IF NOT EXISTS idx_fact_invoice ON warehouse.fact_sales(invoice_key);
            CREATE INDEX IF NOT EXISTS idx_invoice_number ON warehouse.dim_invoice(invoice_no);
            CREATE INDEX IF NOT EXISTS idx_product_code ON warehouse.dim_product(stock_code);
            CREATE INDEX IF NOT EXISTS idx_customer_id ON warehouse.dim_customer(customer_id);
            """
        )

    def _quality_results(self, con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
        raw = self._scalar(con, "SELECT count(*) FROM raw.online_retail")
        valid = self._scalar(con, "SELECT count(*) FROM staging.valid_sales")
        rejected = self._scalar(con, "SELECT count(*) FROM quarantine.invalid_sales")
        duplicates = self._scalar(con, "SELECT count(*) FROM quarantine.duplicate_sales")
        orphan_products = self._scalar(
            con,
            """SELECT count(*) FROM warehouse.fact_sales f LEFT JOIN warehouse.dim_product d
               USING(product_key) WHERE d.product_key IS NULL""",
        )
        orphan_dates = self._scalar(
            con,
            """SELECT count(*) FROM warehouse.fact_sales f LEFT JOIN warehouse.dim_date d
               USING(date_key) WHERE d.date_key IS NULL""",
        )
        fact_difference = abs(
            self._scalar(con, "SELECT count(*) FROM warehouse.fact_sales") - valid
        )
        rejected_pct = 100.0 * rejected / raw if raw else 100.0
        duplicate_pct = 100.0 * duplicates / raw if raw else 100.0
        return [
            {"name": "source_has_rows", "severity": "CRITICAL", "value": raw, "threshold": f">={self.settings.minimum_valid_rows}", "passed": raw >= self.settings.minimum_valid_rows},
            {"name": "valid_rows_exist", "severity": "CRITICAL", "value": valid, "threshold": f">={self.settings.minimum_valid_rows}", "passed": valid >= self.settings.minimum_valid_rows},
            {"name": "rejected_row_percentage", "severity": "WARNING", "value": rejected_pct, "threshold": f"<={self.settings.maximum_rejected_percentage}", "passed": rejected_pct <= self.settings.maximum_rejected_percentage},
            {"name": "duplicate_row_percentage", "severity": "WARNING", "value": duplicate_pct, "threshold": f"<={self.settings.maximum_duplicate_percentage}", "passed": duplicate_pct <= self.settings.maximum_duplicate_percentage},
            {"name": "product_referential_integrity", "severity": "CRITICAL", "value": orphan_products, "threshold": "=0", "passed": orphan_products == 0},
            {"name": "date_referential_integrity", "severity": "CRITICAL", "value": orphan_dates, "threshold": "=0", "passed": orphan_dates == 0},
            {"name": "fact_row_reconciliation", "severity": "CRITICAL", "value": fact_difference, "threshold": "=0", "passed": fact_difference == 0},
        ]

    def _record_quality(self, con: duckdb.DuckDBPyConnection, run_id: str, results: list[dict[str, Any]]) -> None:
        now = datetime.now(timezone.utc)
        con.executemany(
            "INSERT INTO quality.check_results VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(run_id, r["name"], r["severity"], r["value"], r["threshold"], r["passed"], now) for r in results],
        )
        report = {"run_id": run_id, "checked_at_utc": now.isoformat(), "checks": results}
        (self.settings.quality_dir / f"quality_{run_id}.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )
        alerts = [r for r in results if not r["passed"]]
        if alerts:
            with (self.settings.quality_dir / "alerts.jsonl").open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({"run_id": run_id, "alerts": alerts, "at": now.isoformat()}) + "\n")
        critical = [r for r in alerts if r["severity"] == "CRITICAL"]
        if critical:
            raise DataQualityError(f"Critical quality checks failed: {[r['name'] for r in critical]}")

    def _export_partitions(
        self, con: duckdb.DuckDBPyConnection, source_hash: str
    ) -> None:
        destination = str(
            self.settings.export_dir / "sales" / f"version={source_hash[:12]}"
        ).replace("'", "''")
        con.execute(
            f"""COPY (SELECT * FROM warehouse.fact_sales)
                 TO '{destination}'
                 (FORMAT PARQUET, PARTITION_BY (partition_year, partition_month),
                  OVERWRITE_OR_IGNORE TRUE, COMPRESSION ZSTD)"""
        )

    def run(self) -> dict[str, Any]:
        started = time.perf_counter()
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        self._validate_source()
        source_hash = self._sha256(self.settings.source_file)
        archived = self._archive(source_hash)
        self.logger.info("Starting run %s for source version %s", run_id, source_hash[:12])
        con = duckdb.connect(str(self.settings.warehouse_file))
        try:
            self._create_control_tables(con)
            con.execute(
                "INSERT INTO metadata.etl_runs VALUES (?, ?, ?, now(), NULL, 'RUNNING', NULL, NULL, NULL, NULL, NULL)",
                [run_id, source_hash, str(self.settings.source_file)],
            )
            con.execute(
                """INSERT INTO metadata.dataset_versions VALUES (?, ?, ?, now(), now())
                   ON CONFLICT (source_hash) DO UPDATE SET last_processed_at = now()""",
                [source_hash, str(archived), self.settings.source_file.stat().st_size],
            )
            self._load_and_transform(con, archived)
            self._build_warehouse(con)
            results = self._quality_results(con)
            self._record_quality(con, run_id, results)
            self._export_partitions(con, source_hash)
            raw = self._scalar(con, "SELECT count(*) FROM raw.online_retail")
            clean = self._scalar(con, "SELECT count(*) FROM staging.valid_sales")
            rejected = self._scalar(con, "SELECT count(*) FROM quarantine.invalid_sales")
            duplicates = self._scalar(con, "SELECT count(*) FROM quarantine.duplicate_sales")
            con.execute(
                """INSERT INTO metadata.data_lineage VALUES
                   (?, 'staging.valid_sales', 'raw.online_retail', 'type conversion, validation, standardization, deduplication', now()),
                   (?, 'warehouse.fact_sales', 'staging.valid_sales', 'surrogate-key lookup and metric derivation', now()),
                   (?, 'data/exports/sales', 'warehouse.fact_sales', 'year/month partitioned Parquet export', now())""",
                [run_id, run_id, run_id],
            )
            con.execute(
                """UPDATE metadata.etl_runs SET completed_at=now(), status='SUCCESS',
                   raw_rows=?, clean_rows=?, rejected_rows=?, duplicate_rows=? WHERE run_id=?""",
                [raw, clean, rejected, duplicates, run_id],
            )
            summary = {
                "run_id": run_id,
                "status": "SUCCESS",
                "source_version": source_hash[:12],
                "raw_rows": raw,
                "clean_rows": clean,
                "rejected_rows": rejected,
                "duplicate_rows": duplicates,
                "duration_seconds": round(time.perf_counter() - started, 2),
                "warehouse": str(self.settings.warehouse_file),
            }
            self.logger.info("Completed run %s: %s", run_id, summary)
            return summary
        except Exception as exc:
            self.logger.exception("Run %s failed", run_id)
            try:
                con.execute(
                    "UPDATE metadata.etl_runs SET completed_at=now(), status='FAILED', error_message=? WHERE run_id=?",
                    [str(exc), run_id],
                )
            except Exception:
                pass
            raise
        finally:
            con.close()

    def check_existing_warehouse(self) -> dict[str, Any]:
        con = duckdb.connect(str(self.settings.warehouse_file), read_only=True)
        try:
            results = self._quality_results(con)
            return {"status": "PASS" if all(r["passed"] for r in results if r["severity"] == "CRITICAL") else "FAIL", "checks": results}
        finally:
            con.close()

    def explain_representative_query(self) -> dict[str, Any]:
        con = duckdb.connect(str(self.settings.warehouse_file), read_only=True)
        query = """SELECT d.year, d.month, sum(f.line_amount) AS net_sales
                   FROM warehouse.fact_sales f JOIN warehouse.dim_date d USING(date_key)
                   WHERE d.year = 2011 GROUP BY d.year, d.month ORDER BY d.month"""
        try:
            plan = "\n".join(row[1] for row in con.execute("EXPLAIN " + query).fetchall())
            rows = con.execute(query).fetchall()
            return {"query": query, "execution_plan": plan, "result_rows": rows}
        finally:
            con.close()
