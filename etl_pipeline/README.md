# ETL Pipeline for Inventory & Warehouse Analytics

Modular ETL pipeline (Extract–Transform–Load) to compute inventory, movement, warehouse, and financial analytics.
Designed for CSV or relational DB sources, incremental loads, data quality handling, and export/report generation.

## Features
- **Extract**: CSV or DB (SQLAlchemy), incremental load via per-table timestamp watermarks.
- **Transform**:
  - Inventory KPIs: turnover, DOH, stock accuracy, dead stock.
  - Movement analytics: avg daily, peaks, daily/weekly/monthly trends, simple seasonality check.
  - Warehouse performance: utilization, in/out efficiency, transfer patterns, geo summary.
  - Financial metrics: inventory value, holding cost, stock-out cost (estimate), ABC analysis.
- **Load**: Export analytics tables to Parquet/CSV, optional database load and simple materialized views (Postgres).
- **Reporting**: Auto-generate an HTML report with top tables.
- **Ops**: YAML configuration, logging, chunked IO, unit tests (pytest), Cron/Airflow friendly.

## Project Structure
```
etl_pipeline/
  main.py
  extract/
    data_extractor.py
  transform/
    inventory_metrics.py
    movement_analytics.py
    warehouse_performance.py
    financial_metrics.py
  load/
    data_loader.py
    report_generator.py
  tests/
    test_transformations.py
  config/
    config.yaml
  state/
    last_run.json        # auto-created to store watermarks
  requirements.txt
  README.md
```

## Setup
1. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration
Edit `config/config.yaml`:
- Set `run.mode` to `"csv"` or `"database"`.
- For CSV: place your files in `_sample_data/` (or change `sources.csv.base_path`) and map filenames under `sources.csv.files`.
- For DB: set `sources.database.url` and table names. Postgres recommended.
- Set business rules like `dead_stock_days`, `holding_cost_rate_annual`, etc.
- Configure export formats and (optionally) target database for analytics outputs.

## Running
From the `etl_pipeline/` folder:
```bash
python main.py --config ./config/config.yaml
```
Outputs will appear under:
- Exports: `targets.export.path` (Parquet/CSV).
- Reports: `run.report_dir` (`analytics_report.html`).
- Watermarks: `run.watermark_state` (JSON).

## Data Expectations
- **movements**: `movement_time`, `movement_type` (`IN`, `OUT`, `TRANSFER_OUT`, `TRANSFER_IN`, `SALE`), `product_id`, `warehouse_id`, `quantity`, optional `from_warehouse_id`, `to_warehouse_id`, `unit_cost`.
- **stock**: snapshot with `product_id`, `warehouse_id`, `quantity`, `last_update`, optional `unit_cost`.
- **products**: `product_id`, optional `category_id`, optional `unit_cost`.
- **warehouses**: `warehouse_id`, optional `capacity`, optional `region`.


> Tip: rename columns to match these if your source schema differs (or extend the code to map columns).

## Incremental Load
Watermarks are stored per-table in `state/last_run.json`. On the next run, the extractor only reads rows with timestamps greater than the saved watermark. Works for both CSVs (filtered after read) and DB queries (filtered in SQL).

## Data Quality
- Duplicate removal, basic not-null filtering, and simple dtype casting are applied.
- Per-column NA filling strategies configurable via `data_quality.fill_na`.

## Performance
- CSV reading with `chunksize`, DB fetch handled by pandas/SQLAlchemy.
- DataFrames processed with vectorized ops; avoid loading unnecessary tables/columns by adjusting your config.

## Tests
Run unit tests with:
```bash
pytest -q
```
(Install `pytest` if you don't have it: `pip install pytest`.)

## Scheduling
### Cron
Add a cron entry (example: run daily at 01:15):
```
15 1 * * * /path/to/venv/bin/python /path/to/etl_pipeline/main.py --config /path/to/etl_pipeline/config/config.yaml >> /var/log/etl_pipeline.log 2>&1
```

### Airflow
- Package this repo as a DAG task operator that calls `python main.py --config ...`.
- Or embed the modules into an Airflow DAG with `PythonOperator` for E/T/L steps separately.
- Persist watermarks to a shared volume or database accessible by the scheduler workers.

## Notes & Limitations
- Some financial estimates (holding/stockout) are simplified; tailor to your business logic.
- PDF generation intentionally omitted to avoid external system deps; convert the generated HTML using your preferred tool if needed (e.g., wkhtmltopdf).

## License
MIT
