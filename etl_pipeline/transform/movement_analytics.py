import logging
from typing import Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)

class MovementAnalytics:
    """
    - Average daily movement per product
    - Peak periods identification
    - Movement trends (daily, weekly, monthly)
    - Seasonal patterns (simple decomposition via resampling summaries)
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.freqs = config["metrics"]["resample"]

    def compute(self, dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        outputs = {}
        movements = dfs.get("movements", pd.DataFrame()).copy()
        if movements.empty:
            return outputs

        movements["movement_time"] = pd.to_datetime(movements["movement_time"])
        movements["quantity_signed"] = movements.apply(
            lambda r: -abs(r["quantity"]) if str(r["movement_type"]).upper() in ("OUT","SALE","TRANSFER_OUT") else abs(r["quantity"]),
            axis=1
        )
        movements["date"] = movements["movement_time"].dt.date

        # Average daily movement per product
        daily = movements.groupby(["product_id","date"], as_index=False)["quantity_signed"].sum()
        avg_daily = daily.groupby("product_id", as_index=False)["quantity_signed"].mean().rename(columns={"quantity_signed":"avg_daily_qty"})
        outputs["avg_daily_movement_product"] = avg_daily

        # Peak periods per product (top 5 absolute movement days)
        peak = daily.copy()
        peak["abs_qty"] = peak["quantity_signed"].abs()
        peak = peak.sort_values(["product_id","abs_qty"], ascending=[True, False]).groupby("product_id").head(5)
        outputs["peak_periods_product"] = peak

        # Trends by resample at multiple granularities
        ts = movements.set_index("movement_time").sort_index()
        for label, rule in self.freqs.items():
            agg = ts["quantity_signed"].resample(rule).sum().reset_index().rename(columns={"quantity_signed":f"qty_{label}"})
            outputs[f"movement_trend_{label}"] = agg

        # Seasonal pattern hint: monthly average by month number
        monthly = ts["quantity_signed"].resample("MS").sum()
        by_month = monthly.groupby(monthly.index.month).mean().reset_index().rename(columns={"index":"month", "quantity_signed":"avg_qty"})
        by_month.columns = ["month","avg_qty"]
        outputs["seasonality_monthly_avg"] = by_month

        return outputs
