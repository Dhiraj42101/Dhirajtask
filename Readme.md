📊 Smart Facility Monitoring – Data Engineering Pipeline
📌 Overview

This project implements a mini end-to-end data engineering pipeline for a Smart Facility Monitoring system.
It simulates IoT sensor data across multiple sites and builds a pipeline to ingest, clean, transform, and analyze the data for business insights.

The solution follows a medallion architecture (Bronze → Silver → Gold) and demonstrates practical data engineering skills using Python, SQL, and cloud concepts.

🏗️ Architecture

Flow:

Data Generation → Raw Layer (Bronze) → Transformation (Silver) → Curated Views (Gold) → Reporting
Components:
Data Generation: Python script to simulate IoT data
Raw Layer (Bronze): Stores raw CSV/JSON data
Transformation Layer (Silver): Cleaned & standardized data
Curated Layer (Gold): Analytical views for reporting
Reporting Layer: Dashboard using Python / BI tool
🧰 Tech Stack
Layer	Tools Used
Data Generation	Python (pandas, numpy, faker)
Storage	AWS S3 / Azure Blob (or local simulation)
Processing	Python / SQL
Transformation	Pandas / SQL
Analytics	SQL Views
Reporting	Python (matplotlib / seaborn / plotly)
Version Control	GitHub
📂 Project Structure
project-root/
│
├── README.md
├── architecture.png
├── requirements.txt
│
├── src/
│   ├── generate_data.py      # Generate synthetic datasets
│   ├── ingest.py             # Load data into raw layer
│   ├── transform.py          # Clean & process data
│   └── report.py             # Generate analytics/report
│
├── sql/
│   ├── silver_layer.sql      # Cleaning logic
│   └── gold_views.sql        # Analytical queries
│
├── data_sample/
│   └── sample_output_files
│
├── screenshots/
│   └── dashboard.png
│
└── docs/
    └── assumptions.md
📊 Datasets
1. Devices
device_id
site_id
device_type
install_date
firmware_version
status
2. Sensor Events
event_id
event_ts
device_id
site_id
temperature_c
battery_pct
signal_strength
alert_flag
alert_type
3. Site Master
site_id
site_name
city
region
site_category
⚙️ Pipeline Steps
🔹 Step 1: Data Generation
Generates 7 days of synthetic data
Includes:
Missing values
Duplicate records
Invalid values
Inactive devices generating events

Run:

python src/generate_data.py
🔹 Step 2: Raw Ingestion (Bronze Layer)
Stores raw data as-is
Organized structure:
raw/devices/
raw/site_master/
raw/sensor_events/

Run:

python src/ingest.py
🔹 Step 3: Transformation (Silver Layer)
Removes duplicates
Handles null values
Standardizes alert types
Joins datasets

Outputs:

silver_devices
silver_sites
silver_sensor_events

Run:

python src/transform.py
🔹 Step 4: Curated Layer (Gold)

Creates analytical views:

📌 vw_site_hourly_health
Events per hour per site
Avg temperature & battery
Alert count
📌 vw_device_alert_summary
Alerts per device
Alert type breakdown
📌 vw_site_daily_kpis
Daily metrics per site
Unique devices
Signal strength

Run SQL scripts:

-- sql/gold_views.sql
🔹 Step 5: Reporting

Dashboard includes:

Total events
Total alerts
Top 5 devices by alerts
Site-wise trends
Battery analysis

Run:

python src/report.py
📈 Key Business Insights

This pipeline answers:

✔ Events and alerts per site
✔ Devices generating most alerts
✔ Average temperature & battery trends
✔ Abnormal activity detection

🚀 Setup Instructions
1. Clone Repository
git clone https://github.com/Dhiraj42101/Dhirajtask.git
cd Dhirajtask
2. Install Dependencies
pip install -r requirements.txt
3. Run Pipeline
python src/generate_data.py
python src/ingest.py
python src/transform.py
python src/report.py
🧠 Assumptions
Data is generated for simulation purposes only
Alert logic is rule-based (threshold driven)
Local filesystem may simulate cloud storage
Timezone consistency assumed
Devices can send events even if inactive (for testing)
⚠️ Known Limitations
Not optimized for large-scale production
No orchestration tool (Airflow) implemented
Limited real-time processing
Basic data validation only
Dashboard is simplified
📸 Proof of Output
Screenshots available in /screenshots
Sample outputs in /data_sample
📌 Future Improvements
Add Airflow for orchestration
Implement dbt for transformations
Use real cloud deployment (AWS/Azure)
Add real-time streaming (Kafka)
Improve anomaly detection
👤 Author

Dhiraj Bhosale
Aspiring Data Engineer | Python | SQL | Cloud
