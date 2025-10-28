# Fungsi & Trigger Database

Dokumen ini berisi kumpulan fungsi PostgreSQL untuk pengelolaan stok. Cocok digunakan sebagai bagian dari data pipeline inventory di sistem gudang.

## Urutan Eksekusi
1. Jalankan `functions.sql`
2. Jalankan `triggers.sql`
3. Jalankan `test_cases.sql` untuk uji coba

## Penjelasan Fungsi
- **record_stock_movement** → mencatat transaksi keluar/masuk stok dan update tabel `stock_levels`
- **transfer_stock** → transfer stok antar gudang (otomatis OUT di gudang asal dan IN di gudang tujuan)
- **check_reorder_points** → menampilkan daftar produk di bawah reorder point
- **calculate_stock_value** → menghitung nilai total stok per produk berdasarkan metode FIFO / LIFO / Rata-rata
- **audit_stock_changes** → trigger untuk mencatat semua perubahan di tabel stok dan batch pembelian

## Catatan Teknis
- Menggunakan `FOR UPDATE` untuk locking agar aman dari race condition
- Dapat dikembangkan untuk multi currency atau sistem multi lokasi
- Cocok untuk implementasi di pipeline ETL (misal untuk update dashboard stok di Metabase)
