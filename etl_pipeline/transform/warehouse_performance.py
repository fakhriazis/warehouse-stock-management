import logging
from typing import Dict, Any
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class WarehousePerformance:
    """
    - Utilization rate per warehouse (sum of qty * volume if available / capacity)
    - In/Out efficiency (throughput per day)
    - Transfer patterns between warehouses
    - Geographic distribution optimization (very simple heuristic summary)
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def compute(self, dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        outputs = {}
        stock = dfs.get("stock", pd.DataFrame()).copy()
        movements = dfs.get("movements", pd.DataFrame()).copy()
        warehouses = dfs.get("warehouses", pd.DataFrame()).copy()
        products = dfs.get("products", pd.DataFrame()).copy()

        # Utilization: need capacity column in warehouses, and optionally product volume
        if not stock.empty and not warehouses.empty:
            wh = stock.groupby("warehouse_id", as_index=False)["quantity"].sum().rename(columns={"quantity":"total_qty"})
            if "capacity" in warehouses.columns:
                util = warehouses[["warehouse_id","capacity"]].merge(wh, on="warehouse_id", how="left")
                util["utilization_rate"] = util["total_qty"].fillna(0) / util["capacity"].replace({0:np.nan})
                outputs["warehouse_utilization"] = util

        # In/Out efficiency: throughput per day
        if not movements.empty:
            movements["movement_time"] = pd.to_datetime(movements["movement_time"])
            movements["date"] = movements["movement_time"].dt.date
            throughput = movements.groupby(["warehouse_id","date","movement_type"], as_index=False)["quantity"].sum()
            daily_tp = throughput.groupby(["warehouse_id","movement_type"], as_index=False)["quantity"].mean().rename(columns={"quantity":"avg_daily_qty"})
            outputs["warehouse_inout_efficiency"] = daily_tp

            # Transfer patterns between warehouses (if from_warehouse_id/to_warehouse_id present)
            cols = set(movements.columns)
            if {"from_warehouse_id","to_warehouse_id"}.issubset(cols):
                transfers = movements[movements["movement_type"].str.upper().str.contains("TRANSFER")]
                trans_summarized = transfers.groupby(["from_warehouse_id","to_warehouse_id"], as_index=False)["quantity"].sum().rename(columns={"quantity":"qty_transferred"})
                outputs["transfer_patterns"] = trans_summarized

        # Geographic distribution optimization (placeholder heuristic)
        # If warehouses have 'region' or 'lat/lon', we can summarize inventory by region
        if not stock.empty and "region" in warehouses.columns:
            by_region = stock.merge(warehouses[["warehouse_id","region"]], on="warehouse_id", how="left") \
                             .groupby("region", as_index=False)["quantity"].sum().rename(columns={"quantity":"total_qty"})
            outputs["geo_distribution_summary"] = by_region

        return outputs
