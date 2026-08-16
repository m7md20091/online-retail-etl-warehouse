from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import duckdb


DB = Path(__file__).resolve().parents[1] / "data/warehouse/retail.duckdb"
QUERIES = {
    "Date range": "SELECT min(invoice_ts), max(invoice_ts) FROM warehouse.fact_sales",
    "Products / customers / invoices / dates": """
        SELECT (SELECT count(*) FROM warehouse.dim_product),
               (SELECT count(*) FROM warehouse.dim_customer),
               (SELECT count(*) FROM warehouse.dim_invoice),
               (SELECT count(*) FROM warehouse.dim_date)""",
    "Sales and returns": """
        SELECT transaction_type, count(*), round(sum(line_amount), 2)
        FROM warehouse.fact_sales GROUP BY 1 ORDER BY 1""",
    "Unknown-customer facts": "SELECT count(*) FROM warehouse.fact_sales WHERE customer_key = 0",
    "Quantity / price outliers": """
        SELECT sum(quantity_outlier::INTEGER), sum(price_outlier::INTEGER)
        FROM warehouse.fact_sales""",
}


with duckdb.connect(str(DB), read_only=True) as connection:
    for title, query in QUERIES.items():
        print(f"{title}: {connection.execute(query).fetchall()}")

