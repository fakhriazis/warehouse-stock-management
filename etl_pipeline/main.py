import argparse
import logging
import os
import sys
import yaml
from pathlib import Path

from extract.data_extractor import DataExtractor
from transform.inventory_metrics import InventoryMetrics
from transform.movement_analytics import MovementAnalytics
from transform.warehouse_performance import WarehousePerformance
from transform.financial_metrics import FinancialMetrics
from load.data_loader import DataLoader
from load.report_generator import ReportGenerator

def setup_logging():
    log_fmt = "[%(asctime)s] %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

def read_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def ensure_dirs(*paths):
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)

def main():
    parser = argparse.ArgumentParser(description="ETL Pipeline for Analytics")
    parser.add_argument("--config", default="./config/config.yaml", help="Path to config.yaml")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger("main")

    config = read_config(args.config)
    output_dir = config["run"]["output_dir"]
    report_dir = config["run"]["report_dir"]
    ensure_dirs(output_dir, report_dir, config["targets"]["export"]["path"])

    # 1) Extract
    extractor = DataExtractor(config)
    dfs = extractor.run()  # dict of DataFrames

    # 2) Transform
    inv_metrics = InventoryMetrics(config)
    mov_analytics = MovementAnalytics(config)
    wh_perf = WarehousePerformance(config)
    fin_metrics = FinancialMetrics(config)

    results = {}

    results.update(inv_metrics.compute(dfs))
    results.update(mov_analytics.compute(dfs))
    results.update(wh_perf.compute(dfs))
    results.update(fin_metrics.compute(dfs))

    # 3) Load
    loader = DataLoader(config)
    loader.export_tables(results)         # parquet/csv
    loader.persist_watermark(extractor.watermarks)  # save last processed timestamps

    # Optional: write to DB and create mat. views (if enabled)
    if config.get("targets", {}).get("database", {}).get("enabled", False):
        loader.load_to_database(results)
        loader.create_materialized_views()

    # 4) Reporting
    reporter = ReportGenerator(config)
    reporter.generate_html_report(results)

    logger.info("ETL Pipeline completed successfully.")

if __name__ == "__main__":
    sys.exit(main())
