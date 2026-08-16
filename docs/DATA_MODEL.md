# Data model and metadata

## Grain and relationships

`warehouse.fact_sales` has one row per unique invoice line from the cleaned
source. It joins many-to-one to each dimension using integer surrogate keys.
Invoice number remains accessible in `dim_invoice`; source row identity is used
only to generate a stable fact key within a version.

| Table | Grain | Important columns |
|---|---|---|
| `dim_product` | One stock code | `product_key`, `stock_code`, canonical `description` |
| `dim_customer` | One known customer plus Unknown | `customer_key`, `customer_id`, `country` |
| `dim_date` | One calendar date | `date_key`, year, quarter, month, weekday flags |
| `dim_invoice` | One invoice | `invoice_key`, `invoice_no`, timestamp, type, customer, country |
| `fact_sales` | One deduplicated invoice line | Dimension keys, quantity, price, line amount, return/outlier flags |

The dataset has no separate product or customer master. Product descriptions and
customer countries are therefore inferred from transaction history: the most
frequent value is selected deterministically. In production, mastered reference
data would be preferred and dimensions would likely use slowly changing dimension
type 2 history.

## Lineage

`metadata.dataset_versions` maps the source checksum to the archived file.
`metadata.etl_runs` stores status, timing, row counts, and errors.
`metadata.data_lineage` documents each source-to-target transformation at run
level. This makes every warehouse build traceable to an exact input file.

## Partitioning and indexing

Each Parquet export is first isolated by source-version hash and then partitioned
by `partition_year/partition_month`. Date is the normal
filter in reporting, and monthly partitions offer useful pruning without the
small-file explosion caused by daily partitions. The dataset spans roughly one
year, so country or customer partitioning would create skew and excessive files.

DuckDB ART indexes are created for fact foreign keys and dimension business keys.
They help selective lookups and joins, although DuckDB's columnar scans and zone
maps are normally more important for broad analytical aggregation. The cached
`daily_sales_summary` avoids scanning line-level facts for the most common time
series report.

Use `python run_pipeline.py explain` to display the physical plan and results of a
representative monthly query. Performance decisions should be re-evaluated using
real query telemetry rather than adding indexes speculatively.

## Validation contracts

Critical checks stop the pipeline when the source is empty, no valid records
remain, dimension relationships break, or staging/fact row counts disagree.
Rejected and duplicate percentages are warnings with configurable thresholds.
Every result is stored historically in `quality.check_results` and emitted as
JSON; failures are appended to the alert stream.
