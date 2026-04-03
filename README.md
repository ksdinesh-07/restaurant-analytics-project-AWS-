# 🍽️ Restaurant Analytics Data Engineering Pipeline

An end-to-end **Data Engineering project** that processes restaurant order data using **PySpark on EC2**, stores it in **Amazon S3 (Bronze → Silver → Gold layers)**, and enables analytics using **Amazon Athena**.

---

## 🏗️ Project Architecture

```
S3 (Bronze) → Spark ETL (EC2) → S3 (Silver) → Spark Aggregation → S3 (Gold) → Athena Analytics
```

---

## 📊 Data Layers

| Layer  | Location          | Description                              |
| ------ | ----------------- | ---------------------------------------- |
| Bronze | `restaurant-brz`  | Raw CSV data (orders, restaurants)       |
| Silver | `restaurant-slvr` | Cleaned & enriched joined data (Parquet) |
| Gold   | `restaurant-gold` | Aggregated business metrics (Parquet)    |

---

## 🚀 Tech Stack

* **Compute**: Amazon EC2 (PySpark)
* **Storage**: Amazon S3
* **Processing**: PySpark (Apache Spark)
* **Query Engine**: Amazon Athena
* **Language**: Python
* **Version Control**: GitHub

---

## 📁 Project Structure

```
restaurant-analytics/
│── src/
│   └── etl_pipeline.py
│── sql/
│   └── athena_queries.sql
│── requirements.txt
│── README.md
```

---

## ⚙️ Setup Instructions

### 1️⃣ Clone Repository

```bash
git clone https://github.com/YOUR_USERNAME/restaurant-analytics.git
cd restaurant-analytics
```

---

### 2️⃣ Create Virtual Environment

```bash
python3 -m venv de-env
source de-env/bin/activate
```

---

### 3️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

---

### 4️⃣ Configure AWS Access

```bash
aws configure
```

Provide:

* AWS Access Key
* AWS Secret Key
* Region (e.g., ap-southeast-2)

---

### 5️⃣ Run ETL Pipeline

```bash
spark-submit \
--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.641 \
src/etl_pipeline.py
```

---

## 📂 S3 Bucket Structure

```
restaurant-brz/
    ├── orders.csv
    └── restaurants.csv

restaurant-slvr/
    └── orders_enriched/

restaurant-gold/
    └── daily_restaurant_metrics/
```

---

## 🔄 Data Pipeline Flow

### 🔹 Bronze Layer

* Raw CSV files ingested into S3

### 🔹 Silver Layer

* Data cleaned and joined using PySpark
* Stored in Parquet format

### 🔹 Gold Layer

* Aggregated metrics generated:

  * Orders Delivered
  * GMV (Gross Merchandise Value)
  * Average Delivery Time
  * Late Delivery Rate

---

## 📊 Query Using Athena

1. Open **Amazon Athena Console**
2. Create external table on Gold data:

```sql
CREATE EXTERNAL TABLE daily_restaurant_metrics (
    restaurant_id STRING,
    name STRING,
    cuisine STRING,
    city STRING,
    orders_delivered INT,
    gmv DOUBLE,
    avg_delivery_mins DOUBLE,
    late_count INT,
    late_rate DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 's3://restaurant-gold/daily_restaurant_metrics/';
```

---

### 🔹 Load Partitions

```sql
MSCK REPAIR TABLE daily_restaurant_metrics;
```

---

## 📈 Sample Queries

### 🔹 1. GMV by City

```sql
SELECT city, SUM(gmv) AS total_gmv
FROM daily_restaurant_metrics
GROUP BY city
ORDER BY total_gmv DESC;
```

---

### 🔹 2. Top Restaurants

```sql
SELECT name, SUM(gmv) AS total_gmv
FROM daily_restaurant_metrics
GROUP BY name
ORDER BY total_gmv DESC
LIMIT 10;
```

---

### 🔹 3. Late Delivery Analysis

```sql
SELECT cuisine, AVG(late_rate) AS avg_late_rate
FROM daily_restaurant_metrics
GROUP BY cuisine;
```

---

### 🔹 4. Daily Performance

```sql
SELECT dt, SUM(orders_delivered) AS total_orders
FROM daily_restaurant_metrics
GROUP BY dt;
```

---

## 🔧 Configuration

Update bucket names in `src/etl_pipeline.py`:

```python
BRONZE_BUCKET = "restaurant-brz"
SILVER_BUCKET = "restaurant-slvr"
GOLD_BUCKET = "restaurant-gold"
```

---

## 📊 Output Metrics

| Metric            | Description             |
| ----------------- | ----------------------- |
| orders_delivered  | Total delivered orders  |
| gmv               | Gross merchandise value |
| avg_delivery_mins | Avg delivery time       |
| late_rate         | % of late deliveries    |

---

## 🧪 Requirements

```
pyspark==3.5.1
boto3==1.34.0
pandas==2.2.0
```

---

## 🛠️ Future Improvements

* Automate pipeline using Airflow
* Load data into Amazon Redshift
* Add real-time streaming (Kafka)
* Data quality checks & monitoring

---

## 👨‍💻 Author

**Your Name**

---

## 📅 Last Updated

April 2026

---
