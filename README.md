# 🌦️ Weather Data Pipeline (Medallion Architecture)

## 📌 Overview

This project implements a scalable ETL pipeline designed to ingest, process, and store weather data from multiple cities.

The pipeline follows the Medallion Architecture (Bronze → Silver → Gold), transforming raw data into high-quality, analytics-ready datasets.

---

## 🎯 Objective

Design and implement a robust data pipeline to transform raw weather data into structured and reliable information, enabling:

* Historical data tracking
* Data consistency and quality
* Analytical querying
* Anomaly detection

---

## 🏗️ Architecture Diagram

```mermaid
flowchart LR
    A[Weather API] --> B[Extract (Python)]
    B --> C[Bronze Layer (Raw Data)]
    C --> D[Transform (Pandas)]
    D --> E[Silver Layer (Clean Data)]
    E --> F[Load (SQLAlchemy)]
    F --> G[Gold Layer (PostgreSQL)]
    G --> H[SQL Analysis]
```

This architecture follows the Medallion pattern, ensuring proper separation between raw, cleaned, and analytical data layers.

---

## 🏗️ Architecture

* **Bronze (Raw Layer)**
  Stores raw data collected directly from the API without transformations.

* **Silver (Clean Layer)**
  Applies data cleaning, type conversion, and standardization.

* **Gold (Analytics Layer)**
  Stores structured and aggregated data optimized for querying and analysis.

---

## 🔄 Pipeline Flow

1. Extract data from external weather API
2. Store raw data in Bronze layer
3. Transform and clean data in Silver layer
4. Load structured data into PostgreSQL (Gold layer)
5. Perform analysis using SQL queries

---

## ⚙️ Tech Stack

* Python
* Pandas
* PostgreSQL
* SQLAlchemy
* Git

---

## 📊 Data Model (Gold Layer)

Example schema:

* city
* timestamp
* temperature
* humidity
* ingestion_time

---

## ⚡ Key Features

* Modular ETL pipeline (extract, transform, load)
* Medallion Architecture implementation
* Incremental data loading (avoids duplicates)
* Data quality validation (null handling, type checks)
* Outlier detection (IQR method)
* Logging and error handling

---

## 🧪 Data Quality

The pipeline ensures data reliability through:

* Handling missing values
* Type validation and standardization
* Outlier filtering using statistical methods

---

## ⏰ Scheduling

The pipeline can be scheduled to run periodically using:

* Cron (Linux/Mac)
* Task Scheduler (Windows)

---

## 📁 Project Structure

```bash
weather-etl-pipeline/
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── pipeline.py
│   └── config.py
│
├── sql/
│   └── analysis.sql
│
├── logs/
│
├── requirements.txt
├── README.md
└── .env
```

---

## 🚀 How to Run

1. Clone the repository
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Configure environment variables:

```bash
DB_HOST=your_host
DB_NAME=your_db
DB_USER=your_user
DB_PASSWORD=your_password
API_KEY=your_api_key
```

4. Run the pipeline:

```bash
python src/pipeline.py
```

---

## 📈 Future Improvements

* Workflow orchestration with Airflow
* Containerization with Docker
* Data partitioning (by date)
* Integration with BI tools (Power BI)
* Monitoring and alerting

---

## 💡 Conclusion

This project demonstrates how to design and implement a structured data pipeline using industry best practices, transforming raw data into valuable insights ready for analysis.