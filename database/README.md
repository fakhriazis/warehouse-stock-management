# Warehouse Stock Management - Data Warehouse Design

## 1. Deskripsi Singkat
Proyek ini merupakan rancangan sistem data warehouse untuk pengelolaan stok gudang pada perusahaan e-commerce yang memiliki beberapa lokasi gudang.  
Tujuannya adalah menyediakan sistem yang mampu mencatat setiap pergerakan barang (in/out/transfer/adjustment), memantau level stok, dan mendukung analisis stok untuk pengambilan keputusan.

Struktur sistem ini dibangun menggunakan pendekatan relasional dengan desain yang fleksibel dan scalable, agar dapat diintegrasikan ke sistem operasional maupun dashboard analitik.

---

## 2. Struktur Direktori
```
/database/
 ├── erd_diagram.png
 ├── schema.sql
 └── README.md
```

---

## 3. Desain Database (ERD)
Entitas utama yang digunakan:
- **warehouses** – menyimpan data lokasi gudang.
- **products** – master produk.
- **categories** – kategori produk.
- **suppliers** – data pemasok barang.
- **stock** – stok terkini per gudang.
- **stock_movements** – riwayat pergerakan stok (in/out/transfer/adjustment).
- **purchase_orders** dan **purchase_order_details** – pencatatan pembelian barang ke supplier.
- **sales_orders** dan **sales_order_details** – pencatatan penjualan barang ke pelanggan.

Relasi utamanya:
- Satu produk dapat tersedia di banyak gudang.
- Setiap transaksi stok akan tercatat di tabel `stock_movements`.
- Gudang dapat melakukan transfer stok ke gudang lain.

---

## 4. Business Rules yang Diakomodasi
| Business Rule | Implementasi |
|----------------|---------------|
| **Satu produk bisa ada di multiple warehouses** | Dikelola melalui tabel `stock`, yang memiliki `warehouse_id` dan `product_id`. Kombinasi keduanya bersifat unik. |
| **Setiap movement harus tercatat (in/out/transfer/adjustment)** | Disimpan di tabel `stock_movements`, dengan kolom `movement_type`, `quantity`, `source_warehouse_id`, dan `destination_warehouse_id`. |
| **Track stock level, reorder point, dan safety stock** | Kolom `current_stock`, `reorder_point`, dan `safety_stock` tersedia di tabel `stock`. |
| **Support transfer antar gudang** | Field `source_warehouse_id` dan `destination_warehouse_id` di tabel `stock_movements` digunakan untuk mencatat perpindahan antar gudang. |
| **Audit trail untuk semua perubahan** | Semua perubahan stok direkam sebagai baris baru di `stock_movements`, disertai kolom `created_at` dan `created_by` untuk keperluan audit. |

---

## 5. Alasan Desain (Design Choices)
1. **Modular dan mudah dikembangkan**  
   Setiap tabel didesain agar dapat berdiri sendiri dengan relasi yang jelas. Hal ini memudahkan ekspansi data warehouse untuk kebutuhan analitik ke depan (misal integrasi dengan dashboard BI).

2. **Riwayat stok terpisah dari stok terkini**  
   Tabel `stock` hanya menyimpan posisi terakhir, sedangkan `stock_movements` merekam seluruh perubahan yang terjadi.  
   Desain ini penting untuk audit trail dan analisis tren pergerakan stok.

3. **Pendekatan normalization di level staging**  
   Database ini menerapkan normalisasi di level 3NF agar mudah dijaga konsistensinya sebelum diolah ke bentuk star schema untuk analitik.

4. **Indexing dan constraint**  
   Index ditambahkan di kolom `product_id`, `warehouse_id`, dan `movement_type` untuk mempercepat query analitik dan laporan stok.  
   Constraint `CHECK` juga digunakan untuk validasi jenis movement (IN, OUT, TRANSFER, ADJUSTMENT).

5. **Kemudahan monitoring dan pelaporan**  
   Desain ini siap untuk diintegrasikan dengan alat seperti Metabase atau Power BI untuk laporan: stok minimum, reorder alert, turnover rate, dan transfer antar gudang.

---

## 6. Contoh Query Analitik
### a. Daftar produk yang perlu restock
```sql
SELECT
  p.product_id,
  p.product_name,
  s.warehouse_id,
  w.warehouse_name,
  s.quantity AS current_stock,
  p.reorder_point
FROM stock s
JOIN products p ON s.product_id = p.product_id
JOIN warehouses w ON s.warehouse_id = w.warehouse_id
WHERE s.quantity <= COALESCE(p.reorder_point, 0)
ORDER BY w.warehouse_name, p.product_name;
```

### b. Riwayat transfer antar gudang
```sql
SELECT
  sm.movement_id,
  sm.movement_date,
  p.product_id,
  p.product_name,
  fw.warehouse_id   AS from_warehouse_id,
  fw.warehouse_name AS from_warehouse_name,
  tw.warehouse_id   AS to_warehouse_id,
  tw.warehouse_name AS to_warehouse_name,
  sm.quantity,
  sm.remarks
FROM stock_movements sm
JOIN products p ON sm.product_id = p.product_id
LEFT JOIN warehouses fw ON sm.from_warehouse_id = fw.warehouse_id
LEFT JOIN warehouses tw ON sm.to_warehouse_id   = tw.warehouse_id
WHERE sm.movement_type = 'TRANSFER'
ORDER BY sm.movement_date DESC, sm.movement_id;
```

### c. Total pergerakan stok per bulan
```sql
SELECT
  DATE_TRUNC('month', movement_date) AS month,
  movement_type,
  SUM(quantity) AS total_qty
FROM stock_movements
GROUP BY 1, 2
ORDER BY 1 DESC, movement_type;

```

---

## 7. Catatan Pengembangan Lanjutan
- Bisa ditambahkan tabel `audit_logs` untuk mencatat setiap aksi user (CREATE/UPDATE/DELETE).
- Untuk analitik lanjutan, dapat dikembangkan ke **Star Schema** dengan `fact_stock_movements` dan beberapa `dimension` seperti `dim_product`, `dim_supplier`, `dim_warehouse`, dll.
- Pipeline ETL bisa diatur menggunakan Airflow untuk pemrosesan otomatis ke data mart.

---

**Dibuat oleh:**  
Fakhri Azis Basiri 
