import logging
import pandas as pd

logger = logging.getLogger(__name__)

class WarehousePerformance:
    """
    Analisis performa gudang:
    - Utilisasi (stok per kapasitas)
    - Efisiensi in/out
    - Pola transfer antar gudang
    - Distribusi geografis
    """

    def __init__(self, config):
        self.config = config

    def compute(self, dfs: dict) -> dict:
        outputs = {}
        stock = dfs.get("stock", pd.DataFrame()).copy()
        movements = dfs.get("movements", pd.DataFrame()).copy()
        warehouses = dfs.get("warehouses", pd.DataFrame()).copy()

        # --- 1. Utilisasi Gudang ---
        if not stock.empty and not warehouses.empty and "capacity" in warehouses.columns:
            wh = stock.groupby("warehouse_id", as_index=False)["quantity"].sum().rename(columns={"quantity":"total_qty"})
            util = warehouses[["warehouse_id","capacity"]].merge(wh, on="warehouse_id", how="left")
            util["utilization_rate"] = util["total_qty"].fillna(0) / util["capacity"].replace({0:None})
            outputs["warehouse_utilization"] = util

        # --- 2. Efisiensi In/Out ---
        if not movements.empty:
            ts_col = self.config["columns_mapping"]["movements"]["timestamp"]
            movements[ts_col] = pd.to_datetime(movements[ts_col], errors="coerce")

            # Normalisasi: tentukan kolom warehouse_id dari from/to
            def resolve_warehouse(row):
                mt = str(row["movement_type"]).upper()
                if mt in ("IN","TRANSFER_IN"):
                    return row.get("to_warehouse_id", None)
                elif mt in ("OUT","SALE","TRANSFER_OUT"):
                    return row.get("from_warehouse_id", None)
                return None

            movements["warehouse_id"] = movements.apply(resolve_warehouse, axis=1)
            movements["date"] = movements[ts_col].dt.date

            throughput = movements.groupby(["warehouse_id","date","movement_type"], as_index=False)["quantity"].sum()
            daily_tp = throughput.groupby(["warehouse_id","movement_type"], as_index=False)["quantity"].mean().rename(columns={"quantity":"avg_daily_qty"})
            outputs["warehouse_inout_efficiency"] = daily_tp

            # --- 3. Pola transfer antar gudang ---
            if {"from_warehouse_id","to_warehouse_id"}.issubset(movements.columns):
                transfers = movements[movements["movement_type"].str.upper().str.contains("TRANSFER")]
                trans_sum = transfers.groupby(["from_warehouse_id","to_warehouse_id"], as_index=False)["quantity"].sum()
                outputs["transfer_patterns"] = trans_sum

        # --- 4. Distribusi geografis ---
        if not stock.empty and "region" in warehouses.columns:
            by_region = stock.merge(warehouses[["warehouse_id","region"]], on="warehouse_id", how="left") \
                             .groupby("region", as_index=False)["quantity"].sum()
            outputs["geo_distribution_summary"] = by_region

        logger.info(f"WarehousePerformance menghasilkan tabel: {list(outputs.keys())}")
        return outputs
