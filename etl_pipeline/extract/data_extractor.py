import os
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class DataExtractor:
    """
    Extractor that supports CSV files and relational databases.
    Implements incremental load via watermarks on timestamp columns.
    Applies lightweight data-quality handling based on config.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.mode = config["run"]["mode"]
        self.state_path = Path(config["run"]["watermark_state"])
        self.watermarks = self._load_state()

    # ---------- Watermark state ----------
    def _load_state(self) -> Dict[str, str]:
        if self.state_path.exists():
            try:
                with open(self.state_path, "r") as f:
                    return json.load(f)
            except Exception:
                logger.warning("Failed to read watermark state; starting fresh.")
        return {}

    def _save_state(self):
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_path, "w") as f:
            json.dump(self.watermarks, f, indent=2)

    # ---------- Public API ----------
    def run(self) -> Dict[str, pd.DataFrame]:
        if self.mode == "csv":
            dfs = self._read_from_csv()
        elif self.mode == "database":
            dfs = self._read_from_db()
        else:
            raise ValueError(f"Unknown run.mode: {self.mode}")
        # Save watermarks immediately after a successful extract
        self._save_state()
        return dfs

    # ---------- CSV ----------
    def _read_from_csv(self) -> Dict[str, pd.DataFrame]:
        cfg = self.config["sources"]["csv"]
        base = Path(cfg["base_path"])
        files = cfg["files"]

        tables = {}
        for name, fname in files.items():
            path = base / fname
            if not path.exists():
                logger.warning(f"CSV not found for '{name}': {path}")
                continue

            # Incremental by timestamp if available
            ts_col = self._timestamp_col(name)
            watermark = self.watermarks.get(name)

            df_iter = pd.read_csv(path, chunksize=self.config["run"]["chunk_size"])
            chunks = []
            for chunk in df_iter:
                # normalize dtypes early
                chunk = self._apply_dtypes(name, chunk)
                if ts_col and ts_col in chunk.columns and watermark:
                    chunks.append(chunk[chunk[ts_col] > watermark])
                else:
                    chunks.append(chunk)
            df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

            df = self._apply_data_quality(df)

            # Update watermark
            if ts_col and ts_col in df.columns and not df.empty:
                self.watermarks[name] = str(df[ts_col].max())

            tables[name] = df
            logger.info(f"Loaded CSV '{name}' rows={len(df)}")

        return tables

    # ---------- Database ----------
    def _read_from_db(self) -> Dict[str, pd.DataFrame]:
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
            df = self._apply_dtypes(name, df)
            df = self._apply_data_quality(df)

            # Update watermark
            if ts_col and ts_col in df.columns and not df.empty:
                self.watermarks[name] = str(df[ts_col].max())

            tables[name] = df
            logger.info(f"Loaded DB '{name}' rows={len(df)}")
        return tables

    # ---------- Helpers ----------
    def _timestamp_col(self, table_name: str) -> str:
        # Heuristics for typical timestamp columns
        if table_name == "movements":
            return "movement_time"
        if table_name == "stock":
            return "last_update"
        if table_name in ("purchases", "purchase_lines"):
            return "created_at"
        if table_name == "physical_counts":
            return "count_time"
        return ""

    def _apply_dtypes(self, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
        dtypes_cfg = self.config.get("data_quality", {}).get("dtypes", {}).get(table_name, {})
        for col, dtype in dtypes_cfg.items():
            if col in df.columns:
                try:
                    if dtype.startswith("datetime"):
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                    else:
                        df[col] = df[col].astype(dtype)
                except Exception as e:
                    logger.warning(f"dtype cast failed for {table_name}.{col} -> {dtype}: {e}")
        # Best-effort parse common timestamp columns
        for col in ("movement_time", "last_update", "created_at", "count_time"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df

    def _apply_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        dq = self.config.get("data_quality", {})
        if dq.get("drop_duplicates", False):
            df = df.drop_duplicates()

        not_null = dq.get("not_null", [])
        for col in not_null:
            if col in df.columns:
                df = df[df[col].notna()]

        fills = dq.get("fill_na", {})
        for col, strategy in fills.items():
            if col not in df.columns:
                continue
            if strategy in ("mean", "median", "mode"):
                if strategy == "mean":
                    df[col] = df[col].fillna(df[col].mean())
                elif strategy == "median":
                    df[col] = df[col].fillna(df[col].median())
                else:
                    df[col] = df[col].fillna(df[col].mode().iloc[0] if not df[col].mode().empty else df[col])
            else:
                df[col] = df[col].fillna(strategy)
        return df
