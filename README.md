# Warehouse Stock Management System

## Project Overview
Warehouse Stock Management System adalah proyek end-to-end untuk mengelola data stok gudang, menjaga kualitas data operasional, dan menghasilkan insight analitik. Proyek ini mencakup desain database, fungsi & trigger untuk integritas data, generator data berskala besar, hingga ETL pipeline untuk menyusun metrik inventory dan movement analytics, plus dokumentasi teknis.

**Tujuan utama**:
- Menyediakan skema database
- Mengotomasi pembuatan data uji (skala ratusan ribu hingga jutaan baris).
- Menyusun ETL pipeline dengan incremental load, data quality checks, dan batch loading.
- Menghasilkan metrik analitik seperti stock turnover, DOH, dead stock, dan movement patterns.
- Menyediakan dokumentasi dan praktik version control yang jelas.

---

## Repository Structure
```
warehouse-stock-management/
├── README.md (main documentation)
├── .gitignore
├── requirements.txt (global)
├── /database/
│   ├── erd_diagram.png
│   ├── schema.sql
│   └── README.md
├── /database_functions/
│   ├── functions.sql
│   ├── triggers.sql
│   ├── test_cases.sql
│   └── README.md
├── /data_generator/
│   ├── generate_data.py
│   ├── requirements.txt
│   ├── config.yaml
│   └── /output/
├── /etl_pipeline/
│   ├── main.py
│   ├── /extract/
│   ├── /transform/
│   ├── /load/
│   ├── /tests/
│   ├── /config/
│   └── README.md
├── /analytics/
│   └── sample_reports/
└── /docs/
    ├── setup_guide.md
    ├── api_documentation.md
    └── performance_analysis.md
```

---

## Tech Stack
- **Database**: PostgreSQL
- **ETL & Backend**: Python (pandas, SQLAlchemy, PyYAML, psycopg2/pg8000)
- **Data Generation**: Python + Faker (distribusi, parameterizable via YAML)
- **Testing**: pytest (unit test untuk transform & data quality), SQL test cases
- **Reporting**: CSV, notebook opsional
- **Version Control**: Git & GitHub
- **Documentation**: Markdown

---

## Prerequisites
- Python 3.10+
- PostgreSQL 13+ (disarankan 14/15)
- Akses user DB dengan hak CREATE/CONNECT
- Pip/venv (atau Conda) untuk manajemen environment

---

## Setup Instructions (Step-by-Step)

### 1) Clone Repository & Environment
```bash
git clone https://github.com/fakhriazis/warehouse-stock-management.git
cd warehouse-stock-management

python -m venv venv
# Linux/Mac
source venv/bin/activate
# Windows
# venv\Scripts\activate

pip install -r requirements.txt
```

### 2) Konfigurasi Koneksi Database
Buat file konfigurasi di `etl_pipeline/config/` (mis. `config.yaml`) atau gunakan variabel environment.

Contoh `etl_pipeline/config/config.yaml`:
```yaml
database:
  host: localhost
  port: 5433
  user: admin
  password: admin123
  dbname: stock_management
etl:
  incremental_watermark_table: etl_watermarks
  batch_size: 5000
  timezone: "Asia/Jakarta"
paths:
  data_input_dir: "data_generator/output"
  reports_dir: "analytics/sample_reports"
```

### 3) Inisialisasi Database Schema
```bash
# Jalankan perintah SQL berikut secara berurutan:
# 1. Struktur tabel
psql -h localhost -U warehouse_user -d warehouse -f database/schema.sql

# 2. Fungsi & Trigger
psql -h localhost -U warehouse_user -d warehouse -f database_functions/functions.sql
psql -h localhost -U warehouse_user -d warehouse -f database_functions/triggers.sql

# 3. Test Cases (opsional untuk validasi)
psql -h localhost -U warehouse_user -d warehouse -f database_functions/test_cases.sql
```

### 4) Generate Sample Data
```bash
cd data_generator
pip install -r requirements.txt  # jika terpisah dari global
python generate_data.py --config config.yaml
# Output CSV akan tersimpan ke data_generator/output/
```

### 5) Jalankan ETL Pipeline
```bash
cd ../etl_pipeline
python main.py --config config/config.yaml
```

### 6) Verifikasi Hasil & Laporan
- Cek tabel fakta/dimensi hasil load di DB.
- Cek folder `analytics/sample_reports/` untuk output metrik (CSV/summary).

---

## How to Run Each Component

### Database
1. Import `database/schema.sql` terlebih dahulu.
2. Import `database_functions/functions.sql` dan `database_functions/triggers.sql`.
3. (Opsional) Jalankan `database_functions/test_cases.sql` untuk uji fungsi/trigger.

### Data Generator
```bash
cd data_generator
python generate_data.py --config config.yaml
```
Parameter umum (opsional):
- `--rows-stock 100000` menentukan volume current stock
- `--rows-movements 500000` menentukan volume movement 2 tahun terakhir
- `--seed 42` untuk reprodusibilitas

### ETL Pipeline
```bash
cd etl_pipeline
python main.py --config config/config.yaml --mode full   # full load pertama
python main.py --config config/config.yaml --mode inc    # incremental load berikutnya
```
Fitur ETL:
- **Extract**: dari CSV/DB dengan penanganan tipe data & missing values
- **Incremental Load**: berdasarkan kolom `last_updated` atau watermark table
- **Transform**: normalisasi, deduplikasi, kalkulasi metrik
- **Load**: batch inserts, upsert (ON CONFLICT) ke tabel target
- **Data Quality**: validasi foreign key, range tanggal, nilai negatif, duplikasi

### Tests
```bash
cd etl_pipeline
pytest -q
```
- Unit test untuk fungsi transform
- Data quality test (contoh: non-null, unique constraint simulasi)

---

## Sample Outputs / Results
Contoh metrik yang dihasilkan (tersimpan sebagai CSV di `analytics/sample_reports/`):
- **Inventory Metrics**
  - *Stock Turnover Ratio* per produk/kategori
  - *Days of Inventory on Hand (DOH)*
  - *Stock Accuracy* (physical vs system; jika data fisik tersedia)
  - *Dead Stock* (tanpa pergerakan > 180 hari)
- **Movement Analytics**
  - Rata-rata pergerakan harian/mingguan
  - Musiman (seasonality) sederhana berdasarkan bulan/kuartal

Format contoh output (CSV):
```csv
product_id,category_id,turnover_ratio,days_on_hand,is_dead_stock
P000123,CAT05,8.4,31,false
```

---

## Performance Considerations
- **Incremental First**: gunakan mode incremental setelah full load perdana.
- **Indexing**: tambah index pada kolom join & filter (mis. `product_id`, `movement_date`, `last_updated`).
- **Batching**: atur `batch_size` (default 5k–10k) untuk insert/update massal.
- **Memory**: baca data per-chunk saat volume besar untuk menghindari OOM.
- **Transactions**: gunakan transaksi per batch dan retry logic untuk robustnes.
- **Vacuum/Analyze**: jadwalkan maintenance PostgreSQL untuk menjaga performa.

---

## Future Improvements
- Integrasi dashboard (Metabase/BI) untuk visualisasi interaktif.
- Real-time ingestion (Kafka) untuk stock movement streaming.
- Orkestrasi (Airflow) untuk penjadwalan dan monitoring ETL.
- Data contracts & schema registry untuk menjaga kompatibilitas antar komponen.
- CI/CD (GitHub Actions) untuk lint, test, dan deploy otomatis.

---

## Challenges Faced & Solutions
- **Lonjakan Volume Data**: gunakan chunking + batch upsert, serta indeks yang tepat.
- **Kualitas Data Bervariasi**: terapkan data validation (null check, range check, referential check) sebelum load.
- **Duplikasi & Late Arrivals**: strategi upsert berbasis kunci + watermark untuk memproses keterlambatan data.
- **Perubahan Skema**: gunakan migration script versi (mis. numerik/timestamp) dan dokumentasikan di `/docs/`.

---
## Lisensi
Tentukan lisensi sesuai kebutuhan (mis. MIT/Apache-2.0) dan tambahkan file `LICENSE` bila diperlukan.

