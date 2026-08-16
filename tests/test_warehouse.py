import sys
import unittest
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from retail_etl.config import Settings
from retail_etl.pipeline import EXPECTED_COLUMNS


class WarehouseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = Settings.load(ROOT / "config/pipeline.json")
        if not cls.settings.warehouse_file.exists():
            raise unittest.SkipTest("Run the ETL before warehouse integration tests")
        cls.con = duckdb.connect(str(cls.settings.warehouse_file), read_only=True)

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "con"):
            cls.con.close()

    def test_expected_source_schema_is_documented(self):
        self.assertEqual(len(EXPECTED_COLUMNS), 8)
        self.assertNotIn("#", EXPECTED_COLUMNS)

    def test_fact_reconciles_to_clean_staging(self):
        fact, clean = self.con.execute("""
            SELECT (SELECT count(*) FROM warehouse.fact_sales),
                   (SELECT count(*) FROM staging.valid_sales)
        """).fetchone()
        self.assertEqual(fact, clean)

    def test_fact_foreign_keys_resolve(self):
        missing = self.con.execute("""
            SELECT count(*) FROM warehouse.fact_sales f
            LEFT JOIN warehouse.dim_product p USING (product_key)
            LEFT JOIN warehouse.dim_customer c USING (customer_key)
            LEFT JOIN warehouse.dim_date d USING (date_key)
            LEFT JOIN warehouse.dim_invoice i USING (invoice_key)
            WHERE p.product_key IS NULL OR c.customer_key IS NULL
               OR d.date_key IS NULL OR i.invoice_key IS NULL
        """).fetchone()[0]
        self.assertEqual(missing, 0)

    def test_no_duplicate_sales_keys(self):
        total, distinct_keys = self.con.execute(
            "SELECT count(*), count(DISTINCT sales_key) FROM warehouse.fact_sales"
        ).fetchone()
        self.assertEqual(total, distinct_keys)

    def test_critical_quality_checks_pass(self):
        failed = self.con.execute("""
            SELECT count(*) FROM quality.check_results
            WHERE severity = 'CRITICAL' AND NOT passed
        """).fetchone()[0]
        self.assertEqual(failed, 0)


if __name__ == "__main__":
    unittest.main()

