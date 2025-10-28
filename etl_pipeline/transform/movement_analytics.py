import logging
import pandas as pd

logger = logging.getLogger(__name__)

class MovementAnalytics:
    """
    Modul untuk menghitung analitik pergerakan stok:
    - Rata-rata harian per produk
    - Periode puncak
    - Tren harian/mingguan/bulanan
    - Pola musiman
    """
    def __init__(self, config):
        self.config = config
        self.mov_ts = config["columns_mapping"]["movements"]["timestamp"]

    def compute(self, dfs: dict) -> dict:
        outputs = {}
        movements = dfs.get("movements", pd.DataFrame()).copy()
        if movements.empty or self.mov_ts not in movements.columns:
            return outputs

        movements[self.mov_ts] = pd.to_datetime(movements[self.mov_ts])
        movements["quantity_signed"] = movements.apply(
            lambda r: -abs(r["quantity"]) if str(r["movement_type"]).upper() in ("OUT","SALE","TRANSFER_OUT") else abs(r["quantity"]),
            axis=1
        )
        movements["date"] = movements[self.mov_ts].dt.date

        # Rata-rata harian per produk
        daily = movements.groupby(["product_id","date"], as_index=False)["quantity_signed"].sum()
        avg_daily = daily.groupby("product_id", as_index=False)["quantity_signed"].mean().rename(columns={"quantity_signed":"avg_daily_qty"})
        outputs["avg_daily_movement_product"] = avg_daily

        # Periode puncak
        peak = daily.copy()
        peak["abs_qty"] = peak["quantity_signed"].abs()
        peak = peak.sort_values(["product_id","abs_qty"], ascending=[True, False]).groupby("product_id").head(5)
        outputs["peak_periods_product"] = peak

        # Tren resampling
        ts = movements.set_index(self.mov_ts).sort_index()
        for label, rule in self.config["metrics"]["resample"].items():
            agg = ts["quantity_signed"].resample(rule).sum().reset_index().rename(columns={"quantity_signed":f"qty_{label}"})
            outputs[f"movement_trend_{label}"] = agg

        # Pola musiman (rerata per bulan)
        monthly = ts["quantity_signed"].resample("MS").sum()
        by_month = monthly.groupby(monthly.index.month).mean().reset_index()
        by_month.columns = ["month","avg_qty"]
        outputs["seasonality_monthly_avg"] = by_month

        return outputs
