import os
import json
import logging
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class DataExtractor:
    """
    Extractor membaca data dari CSV atau Database.
    Mendukung incremental load berdasarkan kolom timestamp (watermark).
    """
    def __init__(self, config):
        self.config = config
        self.mode = config["run"]["mode"]
        self.state_path = Path(config["run"]["watermark_state"])
        self.watermarks = self._load_state()

    def _load_state(self):
        if self.state_path.exists():
            try:
                with open(self.state_path, "r") as f:
                    return json.load(f)
            except:
                logger.warning("Gagal membaca state watermark, mulai baru.")
        return {}

    def _save_state(self):
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_path, "w") as f:
            json.dump(self.watermarks, f, indent=2)

    def run(self):
        if self.mode == "csv":
            dfs = self._read_from_csv()
        else:
            dfs = self._read_from_db()
        self._save_state()
        return dfs

    def _read_from_csv(self):
        cfg = self.config["sources"]["csv"]
        base = Path(cfg["base_path"])
        files = cfg["files"]

        tables = {}
        for name, fname in files.items():
            path = base / fname
            if not path.exists():
                logger.warning(f"File CSV tidak ditemukan: {path}")
                continue

            ts_col = self._timestamp_col(name)
            watermark = self.watermarks.get(name)

            df = pd.read_csv(path)
            if ts_col and ts_col in df.columns and watermark:
                df = df[df[ts_col] > watermark]

            if ts_col and ts_col in df.columns and not df.empty:
                self.watermarks[name] = str(df[ts_col].max())

            tables[name] = df
            logger.info(f"Berhasil load CSV '{name}' rows={len(df)}")

        return tables

    def _read_from_db(self):
        cfg = self.config["sources"]["database"]
        url = cfg["url"]
        tables_cfg = cfg["tables"]
        engine = create_engine(url)

        tables = {}
        for name, table in tables_cfg.items():
            ts_col = self._timestamp_col(name)
            watermark = self.watermarks.get(name)

            if ts_col and watermark:
                query = text(f"SELECT * FROM {table} WHERE {ts_col} > :wm")
                params = {"wm": watermark}
            else:
                query = text(f"SELECT * FROM {table}")
                params = {}

            with engine.connect() as conn:
                df = pd.read_sql_query(query, conn, params=params)

            if ts_col and ts_col in df.columns and not df.empty:
                self.watermarks[name] = str(df[ts_col].max())

            tables[name] = df
            logger.info(f"Berhasil load DB '{name}' rows={len(df)}")

        return tables

    def _timestamp_col(self, table_name):
        col_map = self.config.get("columns_mapping", {})
        if table_name in col_map and "timestamp" in col_map[table_name]:
            return col_map[table_name]["timestamp"]
        return ""
