# Smart Facility Monitoring – Data Engineering Pipeline

## 📌 Overview

This project implements an end-to-end **cloud-based data pipeline** for a Smart Facility Monitoring use case. It simulates IoT sensor data, processes it through multiple data layers, and exposes curated datasets for analytics and reporting.

The solution follows modern **ELT (Extract, Load, Transform)** principles using AWS serverless services and is designed for scalability, simplicity, and analytics consumption.

---

## 🏗️ Architecture

### High-Level Flow

```
Data Generation (Python)
        ↓
S3 Raw Layer (Bronze)
        ↓
Data Transformation (Python / Pandas)
        ↓
S3 Silver Layer (Parquet)
        ↓
AWS Athena (SQL Views / Gold Layer)
        ↓
Power BI / HTML Dashboard (Consumption Layer)
```

---

## ⚙️ Tech Stack

| Layer           | Technology                     |
| --------------- | ------------------------------ |
| Data Generation | Python, Faker                  |
| Storage         | Amazon S3                      |
| Processing      | Python (Pandas)                |
| Query Engine    | AWS Athena                     |
| Data Format     | CSV (Raw), Parquet (Processed) |
| Visualization   | Power BI / HTML (Chart.js)     |

---

## 📂 Project Structure

```
project-root/
│
├── README.md
├── architecture.png
├── requirements.txt
│
├── src/
│   ├── generate_data.py
│   ├── ingest.py
│   ├── transform.py
│   └── report.py
│
├── sql/
│   ├── silver_layer.sql
│   └── gold_views.sql
│
├── data_sample/
│   └── sample_output_files
│
├── screenshots/
│   └── dashboard.png
│
└── docs/
    └── assumptions.md
```

---

## 📊 Data Model

### 1. Devices

* device_id
* site_id
* device_type
* install_date
* firmware_version
* status

### 2. Sensor Events

* event_id
* event_ts
* device_id
* site_id
* temperature_c
* battery_pct
* signal_strength
* alert_flag
* alert_type

### 3. Site Master

* site_id
* site_name
* city
* region
* site_category

---

## 🔄 Data Pipeline

### 1. Data Generation

* Synthetic data generated for the last 7 days
* Includes intentional data quality issues:

  * Missing battery values
  * Invalid signal strength
  * Duplicate events
  * Inactive devices sending data

Run:

```bash
python src/generate_data.py
```

---

### 2. Raw Layer (Bronze)

* Data stored in Amazon S3 without transformation
* Folder structure:

```
raw/
  ├── devices/
  ├── site_master/
  └── sensor_events/
```

Run:

```bash
python src/ingest.py
```

---

### 3. Transformation Layer (Silver)

Processing includes:

* Duplicate removal
* Null handling (battery, signal strength)
* Alert type standardization
* Data type corrections (especially date fields)
* Joining with reference datasets

Output:

* Stored as **Parquet** for optimized analytics

Run:

```bash
python src/transform.py
```

---

### ⚠️ Important Fix (Schema Handling)

During development, a schema mismatch was identified between Pandas and Athena where:

* `event_date` was stored as BINARY in Parquet
* Athena expected DATE type

This was resolved by explicitly casting:

```python
df['event_date'] = pd.to_datetime(df['event_ts']).dt.date
```

---

### 4. Gold Layer (Curated Views)

Created using AWS Athena:

#### 📌 vw_site_hourly_health

* event_count
* alert_count
* avg_temperature
* avg_battery

#### 📌 vw_device_alert_summary

* total_events
* total_alerts
* alert breakdown

#### 📌 vw_site_daily_kpis

* total_events
* unique_devices
* critical_alerts
* avg_signal_strength

Run SQL:

```sql
-- sql/gold_views.sql
```

---

## 📈 Reporting Layer

### Power BI Dashboard

Connected via Athena ODBC.

### Dashboard Includes:

* Total events
* Total alerts
* Top 5 devices by alerts
* Site-wise daily trends
* Average battery by site
* Alert distribution

Screenshot available in:

```
screenshots/dashboard.png
```

---

## 🚀 How to Run the Project

### Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 2: Generate Data

```bash
python src/generate_data.py
```

### Step 3: Ingest to S3

```bash
python src/ingest.py
```

### Step 4: Transform Data

```bash
python src/transform.py
```

### Step 5: Run Athena Queries

* Create tables
* Execute SQL from `sql/` folder

### Step 6: Connect Power BI

* Use Athena ODBC driver
* Load curated views

---

## 🧠 Design Decisions

* **ELT over ETL**: Raw data preserved for flexibility
* **Parquet format**: Columnar storage for performance
* **Athena**: Serverless analytics engine
* **Python (Pandas)**: Lightweight transformation layer
* **Separation of layers**: Bronze → Silver → Gold

---

## 📌 Assumptions

* Each device belongs to one site
* Alert types are standardized
* Missing battery values replaced with median
* Signal strength bounded between -100 and 0
* Duplicate events identified via event_id

---

## ⚠️ Limitations

* No workflow orchestration (e.g., Airflow not implemented)
* No incremental or streaming ingestion
* No partitioning strategy applied
* Limited data validation checks
* Single-region deployment

---

## 🔮 Future Improvements

* Add Apache Airflow for orchestration
* Partition S3 data by date/hour
* Use AWS Glue Catalog for schema management
* Implement data quality checks (Great Expectations)
* Real-time ingestion using Kinesis
* CI/CD pipeline for deployment
* Add monitoring and alerting

---

## 📸 Proof of Execution

* Athena query outputs
* S3 data structure
* Power BI dashboard

Available in:

```
screenshots/
```

---

## 🏁 Conclusion

This solution demonstrates a complete modern data pipeline using AWS serverless architecture. It is optimized for analytics consumption, scalable design, and real-world data engineering challenges including schema handling, data quality, and transformation logic.

---

## 👤 Author

Dhiraj Bhosale
Aspiring Data Engineer | Python | SQL | Cloud
