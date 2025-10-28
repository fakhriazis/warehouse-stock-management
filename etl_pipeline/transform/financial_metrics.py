import logging
from typing import Dict, Any
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class FinancialMetrics:
    """
    - Inventory value over time
    - Holding cost calculation
    - Stock-out cost estimation
    - ABC analysis (Pareto)
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        br = config.get("business_rules", {})
        self.holding_rate = float(br.get("holding_cost_rate_annual", 0.2))
        self.stockout_cost_per_unit = float(br.get("stockout_cost_per_unit", 5.0))

    def compute(self, dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        outputs = {}
        stock = dfs.get("stock", pd.DataFrame()).copy()
        movements = dfs.get("movements", pd.DataFrame()).copy()
        products = dfs.get("products", pd.DataFrame()).copy()

        # Inventory value over time (approx using monthly end stock * unit_cost)
        if not stock.empty:
            if "last_update" in stock.columns:
                stock["last_update"] = pd.to_datetime(stock["last_update"])
                stock["month"] = stock["last_update"].dt.to_period("M").dt.to_timestamp()
            else:
                stock["month"] = pd.Timestamp.today().to_period("M").to_timestamp()

            if "unit_cost" not in stock.columns and "unit_cost" in products.columns:
                stock = stock.merge(products[["product_id","unit_cost"]], on="product_id", how="left")

            stock["inventory_value"] = stock["quantity"].fillna(0) * stock.get("unit_cost", 0).fillna(0)
            inv_time = stock.groupby(["month"], as_index=False)["inventory_value"].sum().rename(columns={"inventory_value":"inventory_value_total"})
            outputs["inventory_value_over_time"] = inv_time

            # Holding cost (annual rate -> convert to monthly by /12)
            inv_time["holding_cost"] = inv_time["inventory_value_total"] * (self.holding_rate/12.0)
            outputs["holding_cost_over_time"] = inv_time[["month","holding_cost"]]

        # Stock-out cost estimation: count days where demand (outbound) > available stock, multiply by cost/unit
        if not movements.empty and not stock.empty:
            movements["movement_time"] = pd.to_datetime(movements["movement_time"])
            daily_demand = movements[movements["movement_type"].str.upper().isin(["OUT","SALE","TRANSFER_OUT"])].copy()
            daily_demand["date"] = daily_demand["movement_time"].dt.date
            dd = daily_demand.groupby(["product_id","date"], as_index=False)["quantity"].sum().rename(columns={"quantity":"demand_qty"})

            # Approximate available stock as current quantity; for a better model, compute daily available via running balance
            avail = stock.groupby("product_id", as_index=False)["quantity"].sum().rename(columns={"quantity":"available_qty"})
            merged = dd.merge(avail, on="product_id", how="left")
            merged["stockout_units"] = (merged["demand_qty"] - merged["available_qty"]).clip(lower=0)
            merged["stockout_cost"] = merged["stockout_units"] * self.stockout_cost_per_unit
            outputs["stockout_cost_estimation"] = merged.groupby("product_id", as_index=False)["stockout_cost"].sum()

        # ABC analysis by annual consumption value (unit_cost * outbound qty)
        if not movements.empty:
            outbound = movements[movements["movement_type"].str.upper().isin(["OUT","SALE","TRANSFER_OUT"])].copy()
            if "unit_cost" not in outbound.columns and "unit_cost" in products.columns:
                outbound = outbound.merge(products[["product_id","unit_cost"]], on="product_id", how="left")
            outbound["consumption_value"] = outbound.get("unit_cost", 0).fillna(0) * outbound["quantity"].abs()

            agg = outbound.groupby("product_id", as_index=False)["consumption_value"].sum().sort_values("consumption_value", ascending=False)
            agg["cum_share"] = agg["consumption_value"].cumsum() / agg["consumption_value"].sum().clip(lower=1e-9)
            def classify(p):
                if p <= 0.8: return "A"
                if p <= 0.95: return "B"
                return "C"
            agg["abc_class"] = agg["cum_share"].apply(classify)
            outputs["abc_analysis"] = agg

        return outputs
