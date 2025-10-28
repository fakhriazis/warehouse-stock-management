import pandas as pd
import numpy as np
import random
from faker import Faker
from tqdm import tqdm
from datetime import datetime, timedelta
import os
import yaml

# === Seed ===
fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# === Load Config ===
CONFIG_PATH = "data_generator/config.yaml"
if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
else:
    config = {
        "warehouses": 10,
        "categories": 50,
        "suppliers": 200,
        "products": 5000,
        "stock": 100000,
        "stock_movements": 500000,
        "purchase_orders": 100000,
        "sales_orders": 200000
    }

OUTPUT_DIR = "./output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Helper Functions ===
def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def generate_id(prefix, num, width=3):
    return [f"{prefix}{str(i+1).zfill(width)}" for i in range(num)]

start_date = datetime.now() - timedelta(days=730)
end_date = datetime.now()

# === 1. Categories ===
categories = pd.DataFrame({
    "category_id": generate_id("CA", config["categories"], width=3),
    "category_name": [f"Category {i+1}" for i in range(config["categories"])]
})
categories.to_csv(f"{OUTPUT_DIR}/categories.csv", index=False)

# === 2. Suppliers ===
suppliers = pd.DataFrame({
    "supplier_id": generate_id("SUP", config["suppliers"], width=3),
    "supplier_name": [fake.company() for _ in range(config["suppliers"])],
    "contact_info": [fake.phone_number() for _ in range(config["suppliers"])]
})
suppliers.to_csv(f"{OUTPUT_DIR}/suppliers.csv", index=False)

# === 3. Warehouses ===
warehouses = pd.DataFrame({
    "warehouse_id": generate_id("WH", config["warehouses"], width=3),
    "warehouse_name": [f"Warehouse {i+1}" for i in range(config["warehouses"])],
    "location": [fake.city() for _ in range(config["warehouses"])]
})
warehouses.to_csv(f"{OUTPUT_DIR}/warehouses.csv", index=False)

# === 4. Products ===
category_ids = categories["category_id"].tolist()
supplier_ids = suppliers["supplier_id"].tolist()

products = pd.DataFrame({
    "product_id": generate_id("P", config["products"], width=5),
    "product_name": [fake.word().capitalize() + f" {i}" for i in range(config["products"])],
    "category_id": np.random.choice(category_ids, config["products"]),
    "supplier_id": np.random.choice(supplier_ids, config["products"]),
    "reorder_point": np.random.randint(5, 50, config["products"]),
    "safety_stock": np.random.randint(2, 20, config["products"])
})
products.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)

# === 5. Stock (Current) ===
stock_records = []
warehouse_ids = warehouses["warehouse_id"].tolist()
product_ids = products["product_id"].tolist()

for _ in tqdm(range(config["stock"]), desc="Generating stock"):
    stock_records.append({
        "warehouse_id": random.choice(warehouse_ids),
        "product_id": random.choice(product_ids),
        "quantity": max(0, int(np.random.normal(100, 50)))
    })

stock = pd.DataFrame(stock_records).drop_duplicates(subset=["warehouse_id", "product_id"])

# Tambahkan stock_id
stock["stock_id"] = generate_id("ST", len(stock), width=6)
stock = stock[["stock_id", "warehouse_id", "product_id", "quantity"]]
stock.to_csv(f"{OUTPUT_DIR}/stock.csv", index=False)

# === 6. Stock Movements ===
movement_types = ["IN", "OUT", "TRANSFER", "ADJUSTMENT"]
movements = []
for _ in tqdm(range(config["stock_movements"]), desc="Generating stock movements"):
    mtype = random.choice(movement_types)
    from_wh, to_wh = None, None
    if mtype == "TRANSFER":
        from_wh, to_wh = random.sample(warehouse_ids, 2)
    elif mtype == "IN":
        to_wh = random.choice(warehouse_ids)
    elif mtype == "OUT":
        from_wh = random.choice(warehouse_ids)
    
    movements.append({
        "product_id": random.choice(product_ids),
        "from_warehouse_id": from_wh,
        "to_warehouse_id": to_wh,
        "movement_type": mtype,
        "quantity": random.randint(1, 500),
        "movement_date": random_date(start_date, end_date),
        "remarks": fake.sentence()
    })

stock_movements = pd.DataFrame(movements)
stock_movements["movement_id"] = generate_id("MV", len(stock_movements), width=7)
stock_movements = stock_movements[[
    "movement_id", "product_id", "from_warehouse_id", "to_warehouse_id",
    "movement_type", "quantity", "movement_date", "remarks"
]]
stock_movements.to_csv(f"{OUTPUT_DIR}/stock_movements.csv", index=False)

# === 7. Purchase Orders ===
purchase_orders = pd.DataFrame({
    "po_id": generate_id("PO", config["purchase_orders"], width=6),
    "supplier_id": np.random.choice(supplier_ids, config["purchase_orders"]),
    "warehouse_id": np.random.choice(warehouse_ids, config["purchase_orders"]),
    "po_date": [random_date(start_date, end_date) for _ in range(config["purchase_orders"])],
    "status": np.random.choice(["DRAFT", "APPROVED", "RECEIVED"], config["purchase_orders"], p=[0.2,0.3,0.5])
})
purchase_orders.to_csv(f"{OUTPUT_DIR}/purchase_orders.csv", index=False)

# === 8. Purchase Order Details ===
po_ids = purchase_orders["po_id"].tolist()
po_details = []
for po_id in tqdm(po_ids, desc="Generating PO details"):
    for _ in range(random.randint(1,5)):
        po_details.append({
            "po_detail_id": f"POD{len(po_details)+1:07d}",
            "po_id": po_id,
            "product_id": random.choice(product_ids),
            "quantity": random.randint(10,200),
            "unit_price": round(random.uniform(10,500),2)
        })
purchase_order_details = pd.DataFrame(po_details)
purchase_order_details = po_details = pd.DataFrame(po_details)
purchase_order_details = purchase_order_details[["po_detail_id", "po_id", "product_id", "quantity", "unit_price"]]
purchase_order_details.to_csv(f"{OUTPUT_DIR}/purchase_order_details.csv", index=False)

# === 9. Sales Orders ===
sales_orders = pd.DataFrame({
    "so_id": generate_id("SO", config["sales_orders"], width=6),
    "warehouse_id": np.random.choice(warehouse_ids, config["sales_orders"]),
    "so_date": [random_date(start_date, end_date) for _ in range(config["sales_orders"])],
    "status": np.random.choice(["DRAFT", "CONFIRMED", "SHIPPED"], config["sales_orders"], p=[0.2,0.3,0.5])
})
sales_orders.to_csv(f"{OUTPUT_DIR}/sales_orders.csv", index=False)

# === 10. Sales Order Details ===
so_ids = sales_orders["so_id"].tolist()
so_details = []
for so_id in tqdm(so_ids, desc="Generating SO details"):
    for _ in range(random.randint(1,4)):
        so_details.append({
            "so_detail_id": f"SOD{len(so_details)+1:07d}",
            "so_id": so_id,
            "product_id": random.choice(product_ids),
            "quantity": random.randint(1,50),
            "unit_price": round(random.uniform(20,600),2)
        })
sales_order_details = pd.DataFrame(so_details)
sales_order_details = sales_order_details[["so_detail_id", "so_id", "product_id", "quantity", "unit_price"]]
sales_order_details.to_csv(f"{OUTPUT_DIR}/sales_order_details.csv", index=False)

print("\nData generation complete! All CSVs saved in /output/")
