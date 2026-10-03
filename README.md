# 📈 VN30 Stock Data Lakehouse Platform

[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.10.4-017CEE?style=flat&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID_Transactions-00ADD8?style=flat)](https://delta.io/)
[![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible_Storage-C72C48?style=flat&logo=minio&logoColor=white)](https://min.io/)
[![Hive Metastore](https://img.shields.io/badge/Hive_Metastore-3.1.3-FDEE21?style=flat&logo=apachehive&logoColor=black)](https://hive.apache.org/)
[![Docker Compose](https://img.shields.io/badge/Docker-Containerized-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)

Hệ thống Data Lakehouse chuyên dụng để thu thập, xử lý và phân tích dữ liệu thị trường chứng khoán Việt Nam (rổ chỉ số VN30). Dự án hiện thực hóa kiến trúc **Medallion Architecture (Bronze - Silver - Gold)** trên nền tảng lưu trữ đối tượng MinIO kết hợp định dạng Delta Lake, với toàn bộ quy trình ETL được điều phối tự động qua Apache Airflow và đóng gói hạ tầng qua Docker Compose.

---

## 🏛️ Kiến trúc hệ thống (System Architecture)

Hệ thống được thiết kế theo hướng phân tách độc lập giữa **Lưu trữ (Storage)** và **Tính toán (Compute)**, đảm bảo khả năng mở rộng linh hoạt và tối ưu chi phí tài nguyên:

<p align="center">
  <img width="1050" height="676" alt="Kiến trúc Data Lakehouse VN30" src="https://github.com/user-attachments/assets/48c7f277-2e9b-4c70-ac0b-2df0cfb8d729" />
</p>

### Các thành phần cốt lõi:

* **Tầng lưu trữ đối tượng (Object Storage Layer):**
  * **MinIO:** Đóng vai trò là Object Storage tương thích chuẩn AWS S3, lưu trữ toàn bộ các tệp dữ liệu vật lý qua các bucket chuyên biệt (`bronze`, `silver`, `gold`).
  * **Delta Lake Format:** Cung cấp tính năng giao dịch ACID, Schema Enforcement (ngăn ngừa sai lệch cấu trúc dữ liệu), Time Travel (truy vết lịch sử phiên bản dữ liệu) và tối ưu hóa hiệu năng truy vấn.
* **Tầng tính toán phân tán (Distributed Compute Layer):**
  * **Apache Spark 3.5 (Master - Worker Cluster):** Động cơ xử lý phân tán chịu trách nhiệm thực thi các job ETL biến đổi dữ liệu lớn, xử lý song song và hỗ trợ truy vấn Spark SQL hiệu năng cao.
* **Tầng quản lý siêu dữ liệu (Metadata Management):**
  * **Hive Metastore Standalone (PostgreSQL Backend):** Quản lý tập trung lược đồ dữ liệu (schema), phân vùng (partition) và liên kết bảng trực tiếp với dữ liệu lưu trên MinIO.
* **Tầng điều phối luồng dữ liệu (Orchestration & Scheduling):**
  * **Apache Airflow 2.10.4:** Quản lý vòng đời dữ liệu, lập lịch tự động, cơ chế retry khi lỗi và giám sát tiến trình pipeline theo mô hình DAG (Directed Acyclic Graph).

---

## 🔄 Luồng dữ liệu Medallion (Data Pipeline & Flow)

Dữ liệu di chuyển qua 3 tầng chuyển hóa tiêu chuẩn để đảm bảo độ tin cậy và chất lượng:

```
[ Nguồn API / VNStock ] 
         │ (Airflow Task: Ingestion)
         ▼
[ 🥉 BRONZE TIER (MinIO) ]  ──► Dữ liệu thô nguyên bản (Raw Data)
         │ (Spark Job: Clean, Deduplicate, Schema Enforcement)
         ▼
[ 🥈 SILVER TIER (Delta Lake) ] ──► Dữ liệu sạch, chuẩn hóa kiểu dữ liệu
         │ (Spark Job: Business Aggregation & Metrics)
         ▼
[ 🥇 GOLD TIER (Delta Lake) ]  ──► Bảng phân tích nghiệp vụ, tối ưu hóa cho BI/Query
         │
         ▼
[ 📊 HIVE METASTORE / SPARK SQL / BI TOOLS ]
```

1. **Bronze Tier (Raw Ingestion):** Dữ liệu giá và giao dịch cổ phiếu VN30 được thu thập từ nguồn thông qua thư viện `vnstock` và ghi nguyên bản vào bucket `bronze` trên MinIO.
2. **Silver Tier (Refined & Validated):** Spark job đọc dữ liệu Bronze, thực hiện loại bỏ trùng lặp (deduplication), chuẩn hóa kiểu dữ liệu timestamp/numeric, kiểm tra tính hợp lệ và lưu trữ dưới định dạng Delta Lake.
3. **Gold Tier (Curated & Aggregated):** Spark job tổng hợp các chỉ số kỹ thuật và số liệu giao dịch theo thời gian, lưu thành các bảng dữ liệu phục vụ trực tiếp cho báo cáo và phân tích định lượng.
4. **Data Cataloging:** Metadata của các bảng Delta được cập nhật liên tục vào Hive Metastore để phục vụ các công cụ truy vấn SQL thống nhất.

---

## 🛠️ Ngăn xếp công nghệ (Tech Stack)

| Phân hệ | Công nghệ | Phiên bản / Chi tiết | Vai trò |
| :--- | :--- | :--- | :--- |
| **Orchestration** | Apache Airflow | `2.10.4` | Điều phối DAG, lập lịch và giám sát tác vụ ETL |
| **Compute Engine** | Apache Spark | `3.5` (PySpark) | Xử lý dữ liệu phân tán, chạy các job chuyển đổi tầng |
| **Storage Layer** | MinIO | `RELEASE.2025-02-07` | Hệ thống lưu trữ đối tượng tương thích chuẩn AWS S3 |
| **Table Format** | Delta Lake | `3.x` | Giao dịch ACID, quản lý schema và tối ưu lưu trữ |
| **Catalog** | Hive Metastore | `3.1.3` + PostgreSQL 11 | Lưu trữ và đồng bộ hóa schema cho Spark SQL |
| **Market Data** | Python Ecosystem | `vnstock`, `pyarrow`, `boto3` | Thư viện thu thập và trích xuất dữ liệu tài chính |
| **Infrastructure** | Docker & Compose | Compose spec `3.8` | Container hóa toàn bộ hệ thống dịch vụ độc lập |

---

## 📂 Cấu trúc dự án (Repository Layout)

```text
vn30-lakehouse/
├── airflow/
│   ├── dags/                       # Định nghĩa các Airflow DAGs
│   └── logs/                       # Log thực thi tác vụ (được gitignore)
├── config/
│   ├── hive/
│   │   └── hive-site.xml           # Cấu hình kết nối Metastore & PostgreSQL
│   └── spark/
│       └── spark-defaults.conf     # Cấu hình tích hợp Spark với S3A, Delta & Hive
├── src/
│   ├── ingestion/                  # Kịch bản kết nối API và trích xuất dữ liệu
│   ├── jobs/                       # Các Spark transformation jobs
│   │   ├── bronze_to_silver.py     # ETL làm sạch và chuẩn hóa dữ liệu
│   │   └── silver_to_gold.py       # ETL tổng hợp số liệu nghiệp vụ
│   └── main.py                     # Entrypoint điều phối quá trình thu thập
├── docker-compose.yaml             # Điều phối hạ tầng (MinIO, Spark, Airflow, Hive)
├── requirements.txt                # Thư viện Python bổ sung
├── .gitignore                      # Loại trừ file rác, cache và credentials
└── README.md                       # Tài liệu dự án
```

---

## 🚀 Hướng dẫn triển khai nhanh (Getting Started)

### 1. Yêu cầu hệ thống (Prerequisites)
* Đã cài đặt **Docker** và **Docker Compose** (Khuyến nghị cấp phát tối thiểu 8GB RAM cho Docker Engine).
* Đã cài đặt **Git** trên máy.

### 2. Khởi tạo toàn bộ hạ tầng
Khởi động cụm dịch vụ phân tán ở chế độ chạy nền (detached mode):

```bash
docker-compose up -d
```

### 3. Thông tin truy cập các dịch vụ

| Dịch vụ | Địa chỉ URL | Tài khoản / Mật khẩu | Chức năng |
| :--- | :--- | :--- | :--- |
| **Airflow Web UI** | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` | Giám sát & kích hoạt Pipeline |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` | Trực quan hóa Data Buckets |
| **Spark Master UI** | [http://localhost:9090](http://localhost:9090) | *(Không yêu cầu)* | Giám sát tài nguyên Worker & Job |
| **Hive Metastore** | `thrift://localhost:9083` | *(Kết nối nội bộ)* | Cổng giao tiếp Catalog dữ liệu |

---

## ⚡ Vận hành Pipeline

### Phương thức 1: Kích hoạt tự động qua Airflow UI (Khuyên dùng)
1. Đăng nhập vào [Airflow Web UI](http://localhost:8080).
2. Bật kích hoạt DAG `stock_analyzer_pipeline`.
3. Nhấn **Trigger DAG** để bắt đầu quy trình ETL tự động theo thứ tự phụ thuộc.

### Phương thức 2: Chạy thủ công từng tác vụ (Dành cho Debug)

* **Bước 1: Kích hoạt Ingestion vào tầng Bronze:**
  ```bash
  docker exec airflow-webserver python /opt/airflow/app/main.py
  ```

* **Bước 2: Gửi Spark Job chuyển đổi Bronze sang Silver:**
  ```bash
  docker exec spark-master-2 spark-submit \
    --master spark://spark-master-2:7077 \
    /opt/bitnami/spark/app/jobs/bronze_to_silver.py \
    --bronze_input_path s3a://bronze/stock_data/vn30/ \
    --silver_output_path s3a://silver/stock_data/vn30/
  ```

---

## 📸 Minh chứng hệ thống (Demonstration)

### 1. Luồng điều phối Airflow DAG
Quy trình thực thi tuần tự từ Ingestion đến các tầng chuyển đổi Delta Lake, đảm bảo tính phụ thuộc chặt chẽ và nhất quán dữ liệu:

<p align="center">
  <img width="966" alt="Airflow DAG Pipeline" src="https://github.com/user-attachments/assets/1a36f2ae-28f6-46cc-9421-7f8513627096" />
</p>

### 2. Tổ chức lưu trữ trên MinIO Object Storage
Các bucket được phân tách độc lập theo kiến trúc Medallion (`bronze`, `silver`, `gold`), lưu trữ dữ liệu phân vùng theo thời gian:

<p align="center">
  <img width="100%" alt="MinIO Object Storage Layout" src="https://github.com/user-attachments/assets/9b098899-1b23-444a-ae79-6cdf5d162eff" />
</p>

---

## 🔒 Khuyến nghị an toàn bảo mật (Security Best Practices)
* File `.env` chứa các chuỗi kết nối và khóa bí mật đã được cấu hình loại trừ qua `.gitignore`.
* Khi triển khai trên môi trường thực tế (Production), cần cập nhật lại toàn bộ mật khẩu mặc định của MinIO, Postgres và khóa `SECRET_KEY` của Airflow trong `docker-compose.yaml`.