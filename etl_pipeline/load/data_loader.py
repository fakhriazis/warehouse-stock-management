import os
import json
import logging
from pathlib import Path
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, config: dict):
        self.config = config
        self.export_cfg = config["targets"]["export"]
        self.db_cfg = config.get("targets", {}).get("database", {})

    def export_tables(self, tables: Dict[str, pd.DataFrame]):
        path = Path(self.export_cfg["path"])
        path.mkdir(parents=True, exist_ok=True)
        fmts = [f.lower() for f in self.export_cfg["format"]]

        for name, df in tables.items():
            if df is None or df.empty:
                continue
            if "parquet" in fmts:
                df.to_parquet(path / f"{name}.parquet", index=False)
            if "csv" in fmts:
                df.to_csv(path / f"{name}.csv", index=False)
            logger.info(f"Exported {name} -> {path}")

    def persist_watermark(self, state: dict):
        state_path = Path(self.config["run"]["watermark_state"])
        state_path.parent.mkdir(parents=True, exist_ok=True)
        with open(state_path, "w") as f:
            json.dump(state, f, indent=2)
        logger.info(f"Saved watermark state to {state_path}")

    def load_to_database(self, tables: Dict[str, pd.DataFrame]):
        url = self.db_cfg["url"]
        engine = create_engine(url)
        schema = self.db_cfg.get("schema", None)
        with engine.begin() as conn:
            for name, df in tables.items():
                if df is None or df.empty:
                    continue
                df.to_sql(name, conn, if_exists="replace", index=False, schema=schema)
                logger.info(f"Loaded table {name} into database.")

    def create_materialized_views(self):
        if not self.db_cfg.get("materialized_views", False):
            return
        url = self.db_cfg["url"]
        engine = create_engine(url)
        # Example MVs (Postgres)
        stmts = [
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS mv_inventory_turnover AS
            SELECT * FROM inventory_turnover_product;
            """,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS mv_movement_trend_monthly AS
            SELECT * FROM movement_trend_monthly;
            """
        ]
        with engine.begin() as conn:
            for s in stmts:
                try:
                    conn.exec_driver_sql(s)
                except Exception as e:
                    logger.warning(f"MV creation failed: {e}")
