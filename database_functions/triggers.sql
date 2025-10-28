-- ==============================================================
-- triggers.sql
-- Trigger audit sederhana untuk mencatat semua perubahan stok
-- ==============================================================

-- Tabel audit untuk menyimpan log perubahan
CREATE TABLE IF NOT EXISTS audit_stock_changes (
    id BIGSERIAL PRIMARY KEY,
    table_name TEXT,
    operation TEXT,
    changed_by TEXT DEFAULT current_user,
    changed_at TIMESTAMP DEFAULT now(),
    row_data JSONB
);

-- Fungsi trigger untuk mencatat data sebelum/sesudah perubahan
CREATE OR REPLACE FUNCTION audit_stock_changes()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_row JSONB;
BEGIN
    IF TG_OP = 'DELETE' THEN
        v_row := to_jsonb(OLD);
    ELSE
        v_row := to_jsonb(NEW);
    END IF;

    INSERT INTO audit_stock_changes(table_name, operation, row_data)
    VALUES (TG_TABLE_NAME, TG_OP, v_row);

    RETURN NEW;
END;
$$;

-- Pasang trigger ke tabel utama
CREATE TRIGGER trg_audit_stock_levels
AFTER INSERT OR UPDATE OR DELETE ON stock_levels
FOR EACH ROW EXECUTE FUNCTION audit_stock_changes();

CREATE TRIGGER trg_audit_stock_movements
AFTER INSERT OR UPDATE OR DELETE ON stock_movements
FOR EACH ROW EXECUTE FUNCTION audit_stock_changes();
