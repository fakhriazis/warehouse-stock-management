import argparse
import logging
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

# Setup logging
def setup_logging():
    log_fmt = "[%(asctime)s] %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

# Membaca file konfigurasi
def read_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

# Membuat folder jika belum ada
def ensure_dirs(*paths):
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)

def main():
    parser = argparse.ArgumentParser(description="ETL Pipeline Analytics Gudang & Inventori")
    parser.add_argument("--config", default="./config/config.yaml", help="Path ke config.yaml")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger("main")

    config = read_config(args.config)
    ensure_dirs(config["run"]["output_dir"],
                config["run"]["report_dir"],
                config["targets"]["export"]["path"])

    # 1. Extract
    extractor = DataExtractor(config)
    dfs = extractor.run()

    # 2. Transform
    results = {}
    results.update(InventoryMetrics(config).compute(dfs))
    results.update(MovementAnalytics(config).compute(dfs))
    results.update(WarehousePerformance(config).compute(dfs))
    results.update(FinancialMetrics(config).compute(dfs))

    # 3. Load
    loader = DataLoader(config)
    loader.export_tables(results)
    loader.persist_watermark(extractor.watermarks)

    if config["targets"]["database"]["enabled"]:
        loader.load_to_database(results)
        loader.create_materialized_views()

    # 4. Reporting
    ReportGenerator(config).generate_html_report(results)

    logger.info("ETL Pipeline selesai dengan sukses.")

if __name__ == "__main__":
    sys.exit(main())
