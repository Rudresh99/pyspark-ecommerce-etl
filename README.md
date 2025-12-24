# PySpark E-commerce ETL Pipeline

## 📌 Project Overview
This project demonstrates an end-to-end **ETL pipeline using PySpark** to process
e-commerce order data. It simulates a real-world data engineering workflow including
data ingestion, cleaning, transformation, aggregation, and optimized storage.

The goal of this project is to showcase **production-ready PySpark practices** rather
than toy examples.

---

## 🏗️ Architecture

Raw CSV Data  
→ PySpark ETL Pipeline  
→ Parquet (Analytics-Ready Data)

---

## 📂 Project Structure

<img width="780" height="1060" alt="image" src="https://github.com/user-attachments/assets/11b787aa-89cd-45b7-b354-5547ffd96161" />


---

## 🧾 Dataset Description

The input dataset (`orders.csv`) contains intentionally dirty data to simulate
real-world conditions.

**Columns:**
- `order_id`
- `product_id`
- `customer_id`
- `order_date`
- `price`
- `quantity`

**Data issues handled:**
- Missing values
- Negative prices
- Duplicate records
- Invalid dates

---

## 🔄 ETL Pipeline Steps

### 1. Ingestion
- Reads raw CSV data using Spark DataFrame API

### 2. Data Cleaning
- Drops rows with critical nulls
- Removes invalid numeric values
- Deduplicates orders

### 3. Transformation
- Converts string dates to proper date type
- Derives `total_amount`
- Extracts `order_year`

### 4. Aggregation
- Revenue per product
- Yearly revenue trends

### 5. Storage
- Writes output in **Parquet format**
- Partitioned by `order_year` for performance

---

## 🚀 How to Run the Project

### Prerequisites
- Python 3.8+
- Java 8 or 11
- Git

## 🧠 Key Concepts Demonstrated

1. Distributed data processing with PySpark

2. Lazy evaluation and Spark actions

3. Schema handling and data validation

4. Partitioning strategy

5. Separation of concerns in ETL design

## 🔮 Future Enhancements

1. Explicit schema enforcement

2. Incremental data loading

3. Spark Streaming with Kafka

4. Deployment on EMR / Dataproc

5. Data quality checks and metrics

## 👤 Author

Rudresh Joshi

-Data Engineer
