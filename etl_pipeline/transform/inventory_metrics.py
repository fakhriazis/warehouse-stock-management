import logging
from typing import Dict, Any
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class InventoryMetrics:
    """
    Computes core inventory KPIs:
    - Stock turnover ratio per product/category (COGS / Avg Inventory)
    - Days of inventory on hand (DOH)
    - Stock accuracy (physical vs system)
    - Dead stock identification (no movement > N days)
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.dead_days = int(config["business_rules"]["dead_stock_days"])

    def compute(self, dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        outputs = {}
        stock = dfs.get("stock", pd.DataFrame()).copy()
        movements = dfs.get("movements", pd.DataFrame()).copy()
        products = dfs.get("products", pd.DataFrame()).copy()
        physical = dfs.get("physical_counts", pd.DataFrame()).copy()

        # Normalize column names we need
        for df in (stock, movements, physical):
            for c in ["warehouse_id", "product_id", "qty", "quantity"]:
                if c in df.columns:
                    break

        # 1) Turnover & DOH
        # Estimate COGS as total outbound quantity * unit_cost (if available) over last 365 days
        if not movements.empty:
            movements["date"] = pd.to_datetime(movements["movement_time"]).dt.date
            recent_mov = movements[movements["movement_time"] >= pd.Timestamp.today() - pd.Timedelta(days=365)]
            outbound = recent_mov[recent_mov["movement_type"].str.upper().isin(["OUT", "SALE", "TRANSFER_OUT"])].copy()
            # prefer line cost if present
            if "unit_cost" not in outbound.columns and "unit_cost" in products.columns:
                outbound = outbound.merge(products[["product_id", "unit_cost"]], on="product_id", how="left")

            outbound["line_cost"] = outbound.get("unit_cost", 0).fillna(0) * outbound["quantity"].abs()
            cogs_per_product = outbound.groupby("product_id", as_index=False)["line_cost"].sum().rename(columns={"line_cost":"cogs_annual"})

            # Average inventory approximation: from stock snapshot if it contains quantity and unit_cost
            avg_inv = stock.groupby("product_id", as_index=False).agg(
                avg_qty=("quantity","mean")
            )
            if "unit_cost" in products.columns:
                avg_inv = avg_inv.merge(products[["product_id","unit_cost"]], on="product_id", how="left")
                avg_inv["avg_inventory_value"] = avg_inv["avg_qty"].fillna(0) * avg_inv["unit_cost"].fillna(0)
            else:
                avg_inv["avg_inventory_value"] = np.nan

            turnover = cogs_per_product.merge(avg_inv, on="product_id", how="left")
            turnover["stock_turnover_ratio"] = turnover["cogs_annual"] / turnover["avg_inventory_value"].replace({0:np.nan})
            outputs["inventory_turnover_product"] = turnover[["product_id","cogs_annual","avg_inventory_value","stock_turnover_ratio"]]

            # Category-level
            if "category_id" in products.columns:
                turn_cat = turnover.merge(products[["product_id","category_id"]], on="product_id", how="left") \
                                   .groupby("category_id", as_index=False) \
                                   .agg(cogs_annual=("cogs_annual","sum"),
                                        avg_inventory_value=("avg_inventory_value","sum"))
                turn_cat["stock_turnover_ratio"] = turn_cat["cogs_annual"] / turn_cat["avg_inventory_value"].replace({0:np.nan})
                outputs["inventory_turnover_category"] = turn_cat

            # Days of inventory on hand (approx) = 365 / turnover
            inv_prod = outputs["inventory_turnover_product"].copy()
            inv_prod["days_on_hand"] = 365 / inv_prod["stock_turnover_ratio"].replace({0:np.nan})
            outputs["doh_product"] = inv_prod[["product_id","days_on_hand"]]

        # 2) Stock accuracy (physical vs system)
        if not physical.empty and not stock.empty:
            # take the latest physical count per product/warehouse
            physical["count_time"] = pd.to_datetime(physical["count_time"])
            idx = physical.sort_values("count_time").groupby(["warehouse_id","product_id"]).tail(1).index
            last_physical = physical.loc[idx]
            sys_qty = stock.groupby(["warehouse_id","product_id"], as_index=False)["quantity"].sum()
            acc = last_physical.merge(sys_qty, on=["warehouse_id","product_id"], how="left", suffixes=("_physical","_system"))
            acc["accuracy_pct"] = 1 - (acc["quantity_physical"] - acc["quantity_system"]).abs() / acc["quantity_system"].replace({0:np.nan})
            outputs["stock_accuracy"] = acc

        # 3) Dead stock (no movement > N days)
        if not movements.empty:
            last_mov = movements.groupby("product_id", as_index=False)["movement_time"].max().rename(columns={"movement_time":"last_movement"})
            last_mov["days_since_last_movement"] = (pd.Timestamp.today() - pd.to_datetime(last_mov["last_movement"])).dt.days
            dead = last_mov[last_mov["days_since_last_movement"] > self.dead_days]
            outputs["dead_stock_products"] = dead

        return outputs
