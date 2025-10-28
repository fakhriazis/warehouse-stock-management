import logging
import pandas as pd

logger = logging.getLogger(__name__)

class FinancialMetrics:
    """
    Modul untuk analisis finansial (berbasis unit karena tidak ada unit_cost):
    - Inventory level over time (jumlah unit)
    - Holding cost (dari jumlah unit × rate)
    - Estimasi biaya stock-out
    - Analisis ABC (Pareto berdasarkan konsumsi unit)
    """
    def __init__(self, config):
        self.config = config
        br = config["business_rules"]
        self.holding_rate = float(br["holding_cost_rate_annual"])
        self.stockout_cost_per_unit = float(br["stockout_cost_per_unit"])

    def compute(self, dfs: dict) -> dict:
        outputs = {}
        stock = dfs.get("stock", pd.DataFrame()).copy()
        movements = dfs.get("movements", pd.DataFrame()).copy()

        # --- 1. Inventory level over time ---
        if not stock.empty:
            ts_col = self.config["columns_mapping"]["stock"]["timestamp"]
            if ts_col in stock.columns:
                stock[ts_col] = pd.to_datetime(stock[ts_col], errors="coerce")
                stock["month"] = stock[ts_col].dt.to_period("M").dt.to_timestamp()
            else:
                stock["month"] = pd.Timestamp.today().to_period("M").to_timestamp()

            stock["inventory_units"] = stock["quantity"]

            inv_time = stock.groupby("month", as_index=False)["inventory_units"].sum()
            inv_time = inv_time.rename(columns={"inventory_units": "total_inventory_units"})
            outputs["inventory_units_over_time"] = inv_time

            # Holding cost berbasis unit (anggap cost per unit = 1)
            inv_time["holding_cost"] = inv_time["total_inventory_units"] * (self.holding_rate / 12.0)
            outputs["holding_cost_over_time"] = inv_time[["month", "holding_cost"]]

        # --- 2. Estimasi biaya stock-out ---
        if not movements.empty and not stock.empty:
            ts_col = self.config["columns_mapping"]["movements"]["timestamp"]
            movements[ts_col] = pd.to_datetime(movements[ts_col], errors="coerce")

            daily_demand = movements[movements["movement_type"].str.upper().isin(["OUT", "SALE", "TRANSFER_OUT"])]
            daily_demand["date"] = daily_demand[ts_col].dt.date
            dd = daily_demand.groupby(["product_id", "date"], as_index=False)["quantity"].sum().rename(columns={"quantity": "demand_qty"})

            avail = stock.groupby("product_id", as_index=False)["quantity"].sum().rename(columns={"quantity": "available_qty"})
            merged = dd.merge(avail, on="product_id", how="left")
            merged["stockout_units"] = (merged["demand_qty"] - merged["available_qty"]).clip(lower=0)
            merged["stockout_cost"] = merged["stockout_units"] * self.stockout_cost_per_unit
            outputs["stockout_cost_estimation"] = merged.groupby("product_id", as_index=False)["stockout_cost"].sum()

        # --- 3. Analisis ABC (Pareto berdasarkan unit) ---
        if not movements.empty:
            outbound = movements[movements["movement_type"].str.upper().isin(["OUT", "SALE", "TRANSFER_OUT"])]
            agg = outbound.groupby("product_id", as_index=False)["quantity"].sum().rename(columns={"quantity": "consumption_units"})
            agg = agg.sort_values("consumption_units", ascending=False)
            agg["cum_share"] = agg["consumption_units"].cumsum() / agg["consumption_units"].sum().clip(lower=1e-9)
            agg["abc_class"] = agg["cum_share"].apply(lambda p: "A" if p <= 0.8 else ("B" if p <= 0.95 else "C"))
            outputs["abc_analysis"] = agg

        logger.info(f"FinancialMetrics menghasilkan tabel: {list(outputs.keys())}")
        return outputs
