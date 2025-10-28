-- Data contoh untuk uji fungsi
INSERT INTO warehouses VALUES ('W001','Jakarta','Jl. Sudirman',now()) ON CONFLICT DO NOTHING;
INSERT INTO warehouses VALUES ('W002','Bandung','Jl. Asia Afrika',now()) ON CONFLICT DO NOTHING;

INSERT INTO suppliers VALUES ('S001','PT Sumber Makmur','08123456789',now()) ON CONFLICT DO NOTHING;
INSERT INTO categories VALUES ('C001','Elektronik') ON CONFLICT DO NOTHING;

INSERT INTO products VALUES ('P001','Smartphone','C001','S001',10,5,now()) ON CONFLICT DO NOTHING;
INSERT INTO products VALUES ('P002','Laptop','C001','S001',20,5,now()) ON CONFLICT DO NOTHING;

INSERT INTO stock VALUES ('STK001','W001','P001',8,now()) ON CONFLICT DO NOTHING;
INSERT INTO stock VALUES ('STK002','W001','P002',25,now()) ON CONFLICT DO NOTHING;

-- Uji 1: tambah stok (IN)
SELECT record_stock_movement('P001', NULL, 'W001', 'IN', 5, 'Restock barang');

-- Uji 2: kurangi stok lebih banyak dari yang ada (harus error)
SELECT record_stock_movement('P001', 'W001', NULL, 'OUT', 50, 'Oversell test');

-- Uji 3: transfer stok antar gudang
SELECT transfer_stock('P001', 'W001', 'W002', 3, 'Relokasi stok');

-- Uji 4: cek produk yang perlu reorder
SELECT * FROM check_reorder_points(NULL);

-- Uji 5: hitung nilai stok
SELECT * FROM calculate_stock_value();
