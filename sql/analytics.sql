-- Representative reporting queries. Run through DuckDB against retail.duckdb.

-- Monthly net sales: the cached table avoids repeatedly scanning the fact table.
SELECT year, month, round(sum(net_sales), 2) AS net_sales
FROM cache.daily_sales_summary
GROUP BY year, month
ORDER BY year, month;

-- Ten products with the highest gross sales (returns excluded).
SELECT p.stock_code, p.description, round(sum(f.line_amount), 2) AS gross_sales
FROM warehouse.fact_sales f
JOIN warehouse.dim_product p USING (product_key)
WHERE f.transaction_type = 'SALE'
GROUP BY p.stock_code, p.description
ORDER BY gross_sales DESC
LIMIT 10;

-- Countries ranked by net revenue.
SELECT c.country, round(sum(f.line_amount), 2) AS net_sales
FROM warehouse.fact_sales f
JOIN warehouse.dim_customer c USING (customer_key)
GROUP BY c.country
ORDER BY net_sales DESC;

-- Data-quality trend across ETL runs.
SELECT r.started_at, q.check_name, q.observed_value, q.threshold, q.passed
FROM quality.check_results q
JOIN metadata.etl_runs r USING (run_id)
ORDER BY r.started_at DESC, q.check_name;

