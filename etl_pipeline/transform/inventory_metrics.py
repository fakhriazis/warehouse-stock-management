import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class InventoryMetrics:
    """
    Modul untuk menghitung metrik inventori (tanpa unit_cost):
    - Stock turnover ratio (berbasis jumlah unit)
    - Days of inventory on hand (DOH)
    - Dead stock (produk tidak bergerak > N hari)
    """

    def __init__(self, config):
        self.config = config
        self.dead_days = int(config["business_rules"]["dead_stock_days"])
        self.mov_ts = config["columns_mapping"]["movements"]["timestamp"]

    def compute(self, dfs: dict) -> dict:
        outputs = {}
        stock = dfs.get("stock", pd.DataFrame()).copy()
        movements = dfs.get("movements", pd.DataFrame()).copy()
        products = dfs.get("products", pd.DataFrame()).copy()

        # --- 1. Stock Turnover & Days on Hand ---
        if not movements.empty and self.mov_ts in movements.columns:
            movements[self.mov_ts] = pd.to_datetime(movements[self.mov_ts], errors="coerce")

            recent_mov = movements[movements[self.mov_ts] >= pd.Timestamp.today() - pd.Timedelta(days=365)]
            outbound = recent_mov[recent_mov["movement_type"].str.upper().isin(["OUT","SALE","TRANSFER_OUT"])].copy()

            cogs_per_product = outbound.groupby("product_id", as_index=False)["quantity"].sum()
            cogs_per_product = cogs_per_product.rename(columns={"quantity": "cogs_units"})

            avg_inv = stock.groupby("product_id", as_index=False)["quantity"].mean().rename(columns={"quantity": "avg_qty"})

            turnover = cogs_per_product.merge(avg_inv, on="product_id", how="left")
            turnover["stock_turnover_ratio"] = turnover["cogs_units"] / turnover["avg_qty"].replace({0: np.nan})
            outputs["inventory_turnover_product"] = turnover

            inv_prod = turnover.copy()
            inv_prod["days_on_hand"] = 365 / inv_prod["stock_turnover_ratio"].replace({0: np.nan})
            outputs["doh_product"] = inv_prod[["product_id", "days_on_hand"]]

            if "category_id" in products.columns:
                turn_cat = turnover.merge(products[["product_id", "category_id"]], on="product_id", how="left") \
                                   .groupby("category_id", as_index=False) \
                                   .agg(cogs_units=("cogs_units", "sum"),
                                        avg_qty=("avg_qty", "sum"))
                turn_cat["stock_turnover_ratio"] = turn_cat["cogs_units"] / turn_cat["avg_qty"].replace({0: np.nan})
                outputs["inventory_turnover_category"] = turn_cat

        # --- 2. Dead Stock ---
        if not movements.empty and self.mov_ts in movements.columns:
            last_mov = movements.groupby("product_id", as_index=False)[self.mov_ts].max()
            last_mov = last_mov.rename(columns={self.mov_ts: "last_movement"})
            last_mov["days_since_last_movement"] = (pd.Timestamp.today() - pd.to_datetime(last_mov["last_movement"])).dt.days
            dead = last_mov[last_mov["days_since_last_movement"] > self.dead_days]
            outputs["dead_stock_products"] = dead

        logger.info(f"InventoryMetrics menghasilkan tabel: {list(outputs.keys())}")
        return outputs
