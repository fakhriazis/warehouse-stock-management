CREATE OR REPLACE FUNCTION record_stock_movement(
    p_product_id VARCHAR(20),
    p_from_warehouse_id VARCHAR(20),
    p_to_warehouse_id VARCHAR(20),
    p_movement_type VARCHAR(20), -- 'IN', 'OUT', 'TRANSFER', 'ADJUSTMENT'
    p_quantity INT,
    p_remarks TEXT
) RETURNS JSON LANGUAGE plpgsql AS $$
DECLARE
    v_existing_qty INT := 0;
    v_new_qty INT := 0;
    v_target_warehouse_id VARCHAR(20);
    v_movement_id VARCHAR(20);
BEGIN
    -- Validasi jumlah
    IF p_quantity <= 0 THEN
        RETURN json_build_object('status','error','message','Quantity harus lebih besar dari 0');
    END IF;

    -- Validasi tipe movement
    IF p_movement_type NOT IN ('IN','OUT','TRANSFER','ADJUSTMENT') THEN
        RETURN json_build_object('status','error','message','movement_type tidak valid');
    END IF;

    -- Tentukan gudang target
    IF p_movement_type = 'IN' THEN
        v_target_warehouse_id := p_to_warehouse_id;
    ELSE
        v_target_warehouse_id := p_from_warehouse_id;
    END IF;

    -- Pastikan baris stok ada
    INSERT INTO stock(stock_id, warehouse_id, product_id, quantity)
    VALUES (concat('STK-',gen_random_uuid()), v_target_warehouse_id, p_product_id, 0)
    ON CONFLICT (warehouse_id, product_id) DO NOTHING;

    -- Ambil stok sekarang
    SELECT quantity INTO v_existing_qty
    FROM stock
    WHERE warehouse_id = v_target_warehouse_id AND product_id = p_product_id
    FOR UPDATE;

    -- Hitung stok baru
    IF p_movement_type = 'IN' THEN
        v_new_qty := v_existing_qty + p_quantity;
    ELSIF p_movement_type = 'OUT' THEN
        IF v_existing_qty < p_quantity THEN
            RETURN json_build_object('status','error','message','Stok tidak mencukupi','current_qty',v_existing_qty);
        END IF;
        v_new_qty := v_existing_qty - p_quantity;
    ELSIF p_movement_type = 'ADJUSTMENT' THEN
        v_new_qty := v_existing_qty + p_quantity;
    ELSIF p_movement_type = 'TRANSFER' THEN
        RETURN json_build_object('status','error','message','Gunakan transfer_stock() untuk transfer antar gudang');
    END IF;

    -- Update tabel stok
    UPDATE stock
    SET quantity = v_new_qty, updated_at = now()
    WHERE warehouse_id = v_target_warehouse_id AND product_id = p_product_id;

    -- Simpan riwayat movement
    v_movement_id := concat('MOV-',gen_random_uuid());

    INSERT INTO stock_movements(movement_id, product_id, from_warehouse_id, to_warehouse_id, movement_type, quantity, movement_date, remarks)
    VALUES (v_movement_id, p_product_id, p_from_warehouse_id, p_to_warehouse_id, p_movement_type, p_quantity, now(), p_remarks);

    RETURN json_build_object('status','ok','movement_id',v_movement_id,'old_qty',v_existing_qty,'new_qty',v_new_qty);
END;
$$;

-- ============================================================
-- Fungsi 3.2: transfer_stock
-- Memindahkan stok antar gudang (OUT dari sumber dan IN ke tujuan).
CREATE OR REPLACE FUNCTION transfer_stock(
    p_product_id VARCHAR(20),
    p_from_warehouse_id VARCHAR(20),
    p_to_warehouse_id VARCHAR(20),
    p_quantity INT,
    p_remarks TEXT
) RETURNS JSON LANGUAGE plpgsql AS $$
DECLARE
    v_tx_id UUID := gen_random_uuid();
    v_out JSON;
    v_in JSON;
BEGIN
    IF p_from_warehouse_id = p_to_warehouse_id THEN
        RETURN json_build_object('status','error','message','Gudang asal dan tujuan tidak boleh sama');
    END IF;

    -- Keluarkan stok dari gudang asal
    v_out := record_stock_movement(p_product_id, p_from_warehouse_id, NULL, 'OUT', p_quantity, p_remarks || ' | TX:' || v_tx_id);
    IF v_out->>'status' <> 'ok' THEN
        RETURN json_build_object('status','error','message','Gagal mengambil stok dari gudang asal','detail',v_out);
    END IF;

    -- Masukkan stok ke gudang tujuan
    v_in := record_stock_movement(p_product_id, NULL, p_to_warehouse_id, 'IN', p_quantity, p_remarks || ' | TX:' || v_tx_id);
    IF v_in->>'status' <> 'ok' THEN
        -- Rollback
        PERFORM record_stock_movement(p_product_id, NULL, p_from_warehouse_id, 'IN', p_quantity, 'Rollback transfer ' || v_tx_id);
        RETURN json_build_object('status','error','message','Gagal menambahkan stok ke gudang tujuan','rollback','done');
    END IF;

    -- Simpan riwayat transfer
    INSERT INTO stock_movements(movement_id, product_id, from_warehouse_id, to_warehouse_id, movement_type, quantity, movement_date, remarks)
    VALUES (concat('MOV-',gen_random_uuid()), p_product_id, p_from_warehouse_id, p_to_warehouse_id, 'TRANSFER', p_quantity, now(), p_remarks || ' | TX:' || v_tx_id);

    RETURN json_build_object('status','ok','transfer_tx',v_tx_id,'from',v_out,'to',v_in);
END;
$$;

-- ============================================================
-- Fungsi 3.3: check_reorder_points
-- Mengecek produk yang jumlahnya sudah <= reorder_point.
CREATE OR REPLACE FUNCTION check_reorder_points(
    p_warehouse_id VARCHAR(20) DEFAULT NULL
) RETURNS TABLE (
    product_id VARCHAR(20),
    warehouse_id VARCHAR(20),
    current_qty INT,
    reorder_point INT,
    suggested_order_qty INT
) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT s.product_id,
           s.warehouse_id,
           s.quantity AS current_qty,
           p.reorder_point,
           GREATEST(p.reorder_point - s.quantity, p.safety_stock) AS suggested_order_qty
    FROM stock s
    JOIN products p ON p.product_id = s.product_id
    WHERE (p_warehouse_id IS NULL OR s.warehouse_id = p_warehouse_id)
      AND s.quantity <= p.reorder_point
    ORDER BY s.quantity ASC;
END;
$$;

-- ============================================================
-- Fungsi 3.4: calculate_stock_value
-- Menghitung nilai stok berdasarkan harga rata-rata dari PO yang sudah diterima.
CREATE OR REPLACE FUNCTION calculate_stock_value()
RETURNS TABLE (
    warehouse_id VARCHAR(20),
    product_id VARCHAR(20),
    total_quantity INT,
    avg_unit_price NUMERIC(12,2),
    total_value NUMERIC(12,2)
) LANGUAGE sql AS $$
    SELECT po.warehouse_id,
           pod.product_id,
           SUM(pod.quantity) AS total_quantity,
           AVG(pod.unit_price) AS avg_unit_price,
           SUM(pod.quantity * pod.unit_price) AS total_value
    FROM purchase_orders po
    JOIN purchase_order_details pod ON pod.po_id = po.po_id
    WHERE po.status = 'RECEIVED'
    GROUP BY po.warehouse_id, pod.product_id;
$$;
