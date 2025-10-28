import pandas as pd
from transform.inventory_metrics import InventoryMetrics
from transform.movement_analytics import MovementAnalytics
from transform.financial_metrics import FinancialMetrics

def make_config():
    return {
        "business_rules": {"dead_stock_days": 180, "holding_cost_rate_annual": 0.2, "stockout_cost_per_unit": 5.0},
        "metrics": {"resample": {"daily":"D","weekly":"W","monthly":"MS"}}
    }

def test_avg_daily_movement_product():
    cfg = make_config()
    mov = pd.DataFrame({
        "product_id":["A","A","B"],
        "movement_type":["OUT","IN","OUT"],
        "quantity":[10,5,7],
        "movement_time": pd.to_datetime(["2025-01-01","2025-01-01","2025-01-02"]),
        "warehouse_id":["W1","W1","W2"]
    })
    ma = MovementAnalytics(cfg)
    res = ma.compute({"movements":mov})
    assert "avg_daily_movement_product" in res
    assert set(res["avg_daily_movement_product"]["product_id"]) == {"A","B"}

def test_dead_stock_detection():
    cfg = make_config()
    mov = pd.DataFrame({
        "product_id":["A","B"],
        "movement_type":["OUT","OUT"],
        "quantity":[1,1],
        "movement_time": pd.to_datetime(["2024-01-01","2025-07-01"]),
        "warehouse_id":["W1","W1"]
    })
    im = InventoryMetrics(cfg)
    res = im.compute({"movements":mov, "stock":pd.DataFrame()})
    assert "dead_stock_products" in res
    # product A should be dead (older than 180 days from "today")
    assert "A" in set(res["dead_stock_products"]["product_id"])

def test_abc_analysis():
    cfg = make_config()
    mov = pd.DataFrame({
        "product_id":["A","A","B","C"],
        "movement_type":["OUT","OUT","OUT","OUT"],
        "quantity":[100,100,10,1],
        "movement_time": pd.to_datetime(["2025-01-01","2025-02-01","2025-03-01","2025-04-01"]),
        "warehouse_id":["W1","W1","W1","W1"]
    })
    prod = pd.DataFrame({
        "product_id":["A","B","C"],
        "unit_cost":[10.0, 5.0, 1.0]
    })
    fm = FinancialMetrics(cfg)
    res = fm.compute({"movements":mov, "products":prod, "stock":pd.DataFrame()})
    assert "abc_analysis" in res
    assert set(res["abc_analysis"]["abc_class"]).issubset({"A","B","C"})
