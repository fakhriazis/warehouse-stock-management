CREATE TABLE warehouses (
    warehouse_id VARCHAR(20) PRIMARY KEY,
    warehouse_name VARCHAR(100) NOT NULL,
    location TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE categories (
    category_id VARCHAR(20) PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE suppliers (
    supplier_id VARCHAR(20) PRIMARY KEY,
    supplier_name VARCHAR(100) NOT NULL,
    contact_info TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category_id VARCHAR(20) REFERENCES categories(category_id),
    supplier_id VARCHAR(20) REFERENCES suppliers(supplier_id),
    reorder_point INT DEFAULT 10 CHECK (reorder_point >= 0),
    safety_stock INT DEFAULT 5 CHECK (safety_stock >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stock (
    stock_id VARCHAR(20) PRIMARY KEY,
    warehouse_id VARCHAR(20) REFERENCES warehouses(warehouse_id),
    product_id VARCHAR(20) REFERENCES products(product_id),
    quantity INT DEFAULT 0 CHECK (quantity >= 0),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (warehouse_id, product_id)
);

CREATE TABLE stock_movements (
    movement_id VARCHAR(20) PRIMARY KEY,
    product_id VARCHAR(20) REFERENCES products(product_id),
    from_warehouse_id VARCHAR(20) REFERENCES warehouses(warehouse_id),
    to_warehouse_id VARCHAR(20) REFERENCES warehouses(warehouse_id),
    movement_type VARCHAR(20) CHECK (movement_type IN ('IN', 'OUT', 'TRANSFER', 'ADJUSTMENT')),
    quantity INT NOT NULL CHECK (quantity > 0),
    movement_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    remarks TEXT
);

CREATE TABLE purchase_orders (
    po_id VARCHAR(20) PRIMARY KEY,
    supplier_id VARCHAR(20) REFERENCES suppliers(supplier_id),
    warehouse_id VARCHAR(20) REFERENCES warehouses(warehouse_id),
    po_date DATE DEFAULT CURRENT_DATE,
    status VARCHAR(20) CHECK (status IN ('DRAFT', 'APPROVED', 'RECEIVED'))
);

CREATE TABLE purchase_order_details (
    po_detail_id VARCHAR(20) PRIMARY KEY,
    po_id VARCHAR(20) REFERENCES purchase_orders(po_id),
    product_id VARCHAR(20) REFERENCES products(product_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2)
);

CREATE TABLE sales_orders (
    so_id VARCHAR(20) PRIMARY KEY,
    warehouse_id VARCHAR(20) REFERENCES warehouses(warehouse_id),
    so_date DATE DEFAULT CURRENT_DATE,
    status VARCHAR(20) CHECK (status IN ('DRAFT', 'CONFIRMED', 'SHIPPED'))
);

CREATE TABLE sales_order_details (
    so_detail_id VARCHAR(20) PRIMARY KEY,
    so_id VARCHAR(20) REFERENCES sales_orders(so_id),
    product_id VARCHAR(20) REFERENCES products(product_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2)
);
