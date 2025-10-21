# ✈️ Travel Booking SCD2 Merge Project

## 🧾 Overview
This project demonstrates a comprehensive **data engineering pipeline** for travel booking data processing, implementing **SCD2 (Slowly Changing Dimension Type 2)** patterns with **Delta Lake** and **PySpark**.  
The pipeline processes daily booking transactions and customer master data with data quality validation, dimension management, and fact table aggregation.

---

## 🏗️ Architecture

### 🔄 Data Pipeline Flow


```
CSV Files → Bronze Layer → Data Quality → Silver Layer → Analytics
    ↓            ↓             ↓             ↓            ↓
Raw Data    Raw Ingestion   DQ Validation  SCD2 Dims    Fact Tables
Customer    + Metadata      + PyDeequ      + Surrogate  + Aggregations
Booking     + Audit Trail   + Logging      Keys         + Analytics
```

### 💎 Medallion Architecture Implementation
- 🥉 **Bronze Layer**: Raw data ingestion with metadata enrichment  
- 🥈 **Silver Layer**: SCD2 dimensions and aggregated fact tables  
- 🥇 **Gold Layer**: Analytics and reporting tables (via SQL queries)

---

## 🗂️ Project Structure
```
Travel_Booking_SCD2_Merge_Project/
├── notebooks/
│   ├── validate_inputs.ipynb              # Input validation and audit logging
│   ├── 10_ingest_bookings_bronze.ipynb    # Booking data ingestion
│   ├── 11_ingest_customers_bronze.ipynb   # Customer data ingestion
│   ├── 20_dq_bookings.ipynb               # Booking data quality checks
│   ├── 21_dq_customers.ipynb              # Customer data quality checks
│   ├── 30_customer_dim_scd2.ipynb         # SCD2 customer dimension
│   ├── 31_booking_fact_build.ipynb        # Booking fact table
│   ├── 40_optimize_zorder.ipynb           # Table optimization
│   └── 41_analyze_stats.ipynb             # Statistics analysis
├── queries/
│   ├── travel_booking_init.sql            # Schema initialization
│   ├── customer360.sql                    # Customer analytics
│   ├── daily_revenue.sql                  # Revenue analytics
│   ├── data_quality_summary.sql           # DQ reporting
│   └── log_completion_flow.sql            # Pipeline completion
├── booking_data/                          # Sample booking CSV files
├── customer_data/                         # Sample customer CSV files
└── README.md                             # This file
```

---

## 📊 Data Sources

### 1️⃣ Booking Data
- 📁 **Files**: `bookings_YYYY-MM-DD.csv`  
- 🎯 **Purpose**: Daily booking transaction records  
- 🧩 **Schema**: `booking_id`, `customer_id`, `booking_type`, `amount`, `discount`, `quantity`, `booking_date`

### 2️⃣ Customer Data
- 📁 **Files**: `customers_YYYY-MM-DD.csv`  
- 🎯 **Purpose**: Customer master data with potential attribute changes  
- 🧩 **Schema**: `customer_id`, `customer_name`, `customer_address`, `email`

---

## ⚙️ Pipeline Components

### 🧪 1. Input Validation (`validate_inputs.ipynb`)
- **Purpose**: Validates input parameters and file existence  
- **Features**:
  - Parameter extraction with defaults  
  - File existence validation  
  - Audit logging setup  
  - Run tracking  

---

### 🥉 2. Bronze Layer Ingestion

#### 📄 Booking Data (`10_ingest_bookings_bronze.ipynb`)
- **Purpose**: Raw booking data ingestion  
- **Features**:
  - CSV parsing with schema inference  
  - Metadata enrichment (ingestion_time, business_date)  
  - Delta table storage with append mode  

#### 👥 Customer Data (`11_ingest_customers_bronze.ipynb`)
- **Purpose**: Raw customer data ingestion with SCD2 preparation  
- **Features**:
  - SCD2 temporal columns (valid_from, valid_to, is_current)  
  - Business date partitioning  
  - Delta table storage  

---

### 🧹 3. Data Quality Validation

#### 🧾 Booking DQ (`20_dq_bookings.ipynb`)
- **Purpose**: Comprehensive booking data quality checks  
- **Checks**:
  - Data existence (row count > 0)  
  - Completeness (customer_id, amount)  
  - Non-negativity (amount, quantity, discount)  
- **Framework**: PyDeequ with error-level validation  

#### 👤 Customer DQ (`21_dq_customers.ipynb`)
- **Purpose**: Customer data quality validation  
- **Checks**:
  - Data existence and completeness  
  - Email format validation  
  - Required field validation  

---

### 🥈 4. Silver Layer Processing

#### 🔁 SCD2 Customer Dimension (`30_customer_dim_scd2.ipynb`)
- **Purpose**: Implements SCD2 for customer dimension  
- **Features**:
  - Surrogate key generation (IDENTITY column)  
  - Historical version tracking  
  - Merge logic for attribute changes  
  - Temporal column management  

#### 📊 Booking Fact Table (`31_booking_fact_build.ipynb`)
- **Purpose**: Creates aggregated booking fact table  
- **Features**:
  - Daily grain aggregation  
  - Customer surrogate key integration  
  - Financial calculations (amount - discount)  
  - Idempotent merge operations  

---

### ⚡ 5. Table Optimization

#### ⚙️ Z-Order Optimization (`40_optimize_zorder.ipynb`)
- Optimizes table performance with **Z-ORDER BY** and **VACUUM**

#### 📈 Statistics Analysis (`41_analyze_stats.ipynb`)
- Updates table statistics for **query optimization**

---

## 🌟 Key Features

### 🧩 SCD2 Implementation
- 🕰️ Historical Tracking for all customer versions  
- 🔑 Surrogate Key generation  
- 📆 Temporal Columns (`valid_from`, `valid_to`, `is_current`)  
- 🔁 Merge Logic for attribute change detection  

### 🧠 Data Quality Framework
- ✅ **PyDeequ** integration for automated DQ  
- 🧾 **Audit Logging** of all checks  
- 🚫 Pipeline stops on DQ failure to prevent bad data  

### 🧊 Delta Lake Features
- 🔒 **ACID transactions**  
- 🔄 **Schema evolution** support  
- 🕰️ **Time travel** for history access  
- ⚙️ **Z-ORDER** + **VACUUM** optimization  

### ⚙️ Parameterization
- 🧱 Widget-based configuration  
- 🧩 Default values for flexibility  
- 🌍 Environment customization  

---

## 🧰 Setup Instructions

### 🔧 Prerequisites
- Databricks workspace with Unity Catalog  
- PyDeequ library installed  
- Table creation permissions  

### 📂 Data Preparation
```bash
# Upload sample CSV files to Volumes
# Structure: /Volumes/{catalog}/{schema}/data/
# ├── booking_data/
# │   └── bookings_YYYY-MM-DD.csv
# └── customer_data/
#     └── customers_YYYY-MM-DD.csv



### ⚙️ Parameter Configuration
```python
# Set widget parameters (optional - defaults provided)
dbutils.widgets.text("arrival_date", "2025-01-15")
dbutils.widgets.text("catalog", "travel_bookings")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("base_volume", "/Volumes/travel_bookings/default/data")
```

### 3. Pipeline Execution
```python
# Run notebooks in sequence:
# 1. validate_inputs.ipynb
# 2. 10_ingest_bookings_bronze.ipynb
# 3. 11_ingest_customers_bronze.ipynb
# 4. 20_dq_bookings.ipynb
# 5. 21_dq_customers.ipynb
# 6. 30_customer_dim_scd2.ipynb
# 7. 31_booking_fact_build.ipynb
# 8. 40_optimize_zorder.ipynb
# 9. 41_analyze_stats.ipynb
```

## 🧮 Data Model

### Customer Dimension (SCD2)
```sql
CREATE TABLE customer_dim (
  customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
  customer_id INT,                                  -- Natural key
  customer_name STRING,
  customer_address STRING,
  email STRING,
  valid_from DATE,                                  -- SCD2 start date
  valid_to DATE,                                    -- SCD2 end date
  is_current BOOLEAN                                -- Current version flag
)
```

###  💰 Booking Fact Table
```sql
CREATE TABLE booking_fact (
  booking_type STRING,
  customer_id INT,                                  -- Natural key
  customer_sk BIGINT,                               -- Surrogate key
  business_date DATE,                               -- Daily grain
  total_amount_sum DOUBLE,                          -- Aggregated amount
  total_quantity_sum BIGINT                         -- Aggregated quantity
)
```

## SCD2 Logic

### Change Detection
- Compares current dimension records with incoming data
- Identifies changes in: customer_name, customer_address, email
- Ignores changes in: customer_id (natural key)

### Version Management
1. **Close Old Version**: Update valid_to and is_current for changed records
2. **Create New Version**: Insert new record with updated attributes
3. **Surrogate Key**: Auto-generated for new versions

### Query Patterns
```sql
-- Current customer data
SELECT * FROM customer_dim WHERE is_current = true

-- Historical customer data
SELECT * FROM customer_dim 
WHERE customer_id = 123 
  AND '2025-01-15' BETWEEN valid_from AND valid_to

-- Customer changes over time
SELECT customer_id, customer_name, valid_from, valid_to
FROM customer_dim 
WHERE customer_id = 123
ORDER BY valid_from
```

## Data Quality Monitoring

### DQ Results Table
```sql
CREATE TABLE ops.dq_results (
  business_date DATE,
  dataset STRING,
  check_name STRING,
  status STRING,
  constraint STRING,
  message STRING,
  recorded_at TIMESTAMP
)
```

### Monitoring Queries
```sql
-- DQ failure summary
SELECT business_date, dataset, COUNT(*) as failed_checks
FROM ops.dq_results 
WHERE status = 'Failure'
GROUP BY business_date, dataset

-- DQ trend analysis
SELECT business_date, 
       SUM(CASE WHEN status = 'Success' THEN 1 ELSE 0 END) as passed,
       SUM(CASE WHEN status = 'Failure' THEN 1 ELSE 0 END) as failed
FROM ops.dq_results
GROUP BY business_date
ORDER BY business_date
```

## Performance Optimization

### Z-ORDER Clustering
```sql
-- Optimize fact table for common query patterns
OPTIMIZE booking_fact ZORDER BY (business_date, customer_sk)

-- Optimize dimension table for lookups
OPTIMIZE customer_dim ZORDER BY (customer_id, is_current)
```

### Statistics Updates
```sql
-- Update statistics for query optimization
ANALYZE TABLE booking_fact COMPUTE STATISTICS
ANALYZE TABLE customer_dim COMPUTE STATISTICS
```

## Workflow Integration

### Databricks Workflows
The pipeline is designed to be executed as Databricks workflows:

#### Workflow 1: Notebook-driven Pipeline
- **Tasks**: Sequential execution of all notebooks
- **Dependencies**: Each notebook depends on previous completion
- **Error Handling**: Stop on failure with detailed logging

#### Workflow 2: SQL-driven Analytics
- **Tasks**: Execution of SQL queries for reporting
- **Trigger**: Triggered by Workflow 1 completion
- **Output**: Analytics tables and reports

### Workflow Configuration
```json
{
  "name": "travel_booking_pipeline",
  "tasks": [
    {
      "task_key": "validate_inputs",
      "notebook_task": {
        "notebook_path": "/notebooks/validate_inputs"
      }
    },
    {
      "task_key": "ingest_bronze",
      "depends_on": [{"task_key": "validate_inputs"}],
      "notebook_task": {
        "notebook_path": "/notebooks/10_ingest_bookings_bronze"
      }
    }
  ]
}
```

## Best Practices Implemented

### 1. Data Quality
- Comprehensive validation at multiple stages
- Automated DQ monitoring and alerting
- Graceful error handling and logging

### 2. Performance
- Z-ORDER clustering for common query patterns
- Statistics updates for query optimization
- Efficient merge operations with Delta Lake

### 3. Maintainability
- Clear naming conventions and documentation
- Parameterized notebooks for flexibility
- Modular design for easy updates

### 4. Scalability
- Incremental processing by business date
- Delta Lake for large-scale data processing
- Optimized storage and query performance

## Troubleshooting

### Common Issues

#### 1. File Not Found Errors
- **Symptom**: `FileNotFoundError` in validate_inputs
- **Solution**: Verify file paths and ensure CSV files exist

#### 2. SCD2 Merge Failures
- **Symptom**: Merge operations fail in customer_dim
- **Solution**: Check for duplicate customer_ids or schema mismatches

#### 3. DQ Validation Failures
- **Symptom**: PyDeequ checks fail
- **Solution**: Review source data quality and adjust DQ rules

### Debugging Steps
1. Check run_log table for pipeline status
2. Review DQ results for data quality issues
3. Validate input data format and content
4. Check Unity Catalog permissions

## Future Enhancements

### 1. Advanced Analytics
- Machine learning integration for booking predictions
- Customer segmentation and behavior analysis
- Revenue forecasting and trend analysis

### 2. Real-time Processing
- Streaming data ingestion with Delta Live Tables
- Real-time SCD2 processing
- Event-driven architecture

### 3. Data Governance
- Column-level security and masking
- Data lineage tracking
- Compliance and audit frameworks

## Conclusion

This Travel Booking SCD2 Merge Project demonstrates a production-ready data engineering pipeline with comprehensive SCD2 implementation, data quality validation, and performance optimization. The pipeline provides a solid foundation for travel booking data processing with built-in monitoring, audit trails, and scalability features.

## Contact

For questions or issues with this pipeline, please refer to the Databricks documentation or contact the data engineering team.
