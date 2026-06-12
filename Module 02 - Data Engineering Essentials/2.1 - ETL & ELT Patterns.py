# Databricks notebook source
# DBTITLE 1,Module 2.1: ETL & ELT
# MAGIC %md
# MAGIC # 🔄 Module 2.1: ETL & ELT Patterns
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand ETL vs ELT approaches
# MAGIC * Learn when to use each pattern
# MAGIC * Build data pipelines with Databricks
# MAGIC * Apply best practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔀 ETL vs ELT
# MAGIC
# MAGIC ### **ETL (Extract, Transform, Load)**
# MAGIC ```
# MAGIC Source → Extract → Transform → Load → Target
# MAGIC ```
# MAGIC * Transform BEFORE loading into target
# MAGIC * Use when: Limited storage, complex transforms
# MAGIC * Example: On-premise data warehouses
# MAGIC
# MAGIC ### **ELT (Extract, Load, Transform)**
# MAGIC ```
# MAGIC Source → Extract → Load → Transform → Target
# MAGIC ```
# MAGIC * Load raw data FIRST, then transform
# MAGIC * Use when: Cloud data lakes, flexible schemas
# MAGIC * ✅ **Recommended for Databricks**
# MAGIC
# MAGIC ### **Key Differences**
# MAGIC | Aspect | ETL | ELT |
# MAGIC |--------|-----|-----|
# MAGIC | **Transform** | Before load | After load |
# MAGIC | **Storage** | Limited | Cheap (cloud) |
# MAGIC | **Flexibility** | Fixed schema | Flexible |
# MAGIC | **Speed** | Slower | Faster |
# MAGIC | **Best For** | Legacy systems | Modern cloud |

# COMMAND ----------

# DBTITLE 1,Modern ELT with Databricks
# MAGIC %md
# MAGIC ## 🏗️ Modern ELT Architecture
# MAGIC
# MAGIC ### **Databricks ELT Flow**
# MAGIC ```
# MAGIC 1. Extract: Read from source (S3, APIs, databases)
# MAGIC 2. Load: Write raw data to Bronze layer (Delta)
# MAGIC 3. Transform: Clean & enrich (Silver → Gold)
# MAGIC ```
# MAGIC
# MAGIC ### **Why ELT on Databricks?**
# MAGIC ✅ **Store everything** - Cheap cloud storage
# MAGIC ✅ **Transform at scale** - Spark power
# MAGIC ✅ **Replay transformations** - Re-process as needed
# MAGIC ✅ **Schema evolution** - Adapt to changes
# MAGIC
# MAGIC ### **Delta Lake Enables ELT**
# MAGIC * **ACID transactions** - Reliable writes
# MAGIC * **Time travel** - View historical data
# MAGIC * **Schema enforcement** - Data quality
# MAGIC * **Upserts & deletes** - Update data easily

# COMMAND ----------

# DBTITLE 1,Demo - Simple ELT Pipeline
# 🔄 Demo: Simple ELT Pipeline

print("Building a simple ELT pipeline...")
print("=" * 50)

# Step 1: EXTRACT - Simulate reading from source
print("\n1️⃣ EXTRACT: Reading source data")
raw_data = [
    {"user_id": 1, "event": "login", "timestamp": "2024-01-15 10:30:00"},
    {"user_id": 2, "event": "purchase", "timestamp": "2024-01-15 11:20:00"},
    {"user_id": 1, "event": "logout", "timestamp": "2024-01-15 12:00:00"}
]
df_raw = spark.createDataFrame(raw_data)
print(f"✅ Extracted {df_raw.count()} records")

# Step 2: LOAD - Write to Bronze (raw)
print("\n2️⃣ LOAD: Writing to Bronze layer (raw data)")
df_raw.write.mode("overwrite").format("delta").save("/tmp/bronze/events")
print("✅ Loaded to Bronze layer")

# Step 3: TRANSFORM - Clean and enrich
print("\n3️⃣ TRANSFORM: Cleaning & enriching")
from pyspark.sql.functions import to_timestamp, upper

df_silver = spark.read.format("delta").load("/tmp/bronze/events") \
    .withColumn("timestamp", to_timestamp("timestamp")) \
    .withColumn("event", upper("event"))

df_silver.write.mode("overwrite").format("delta").save("/tmp/silver/events")
print(f"✅ Transformed {df_silver.count()} records to Silver")

print("\n🎯 ELT Pipeline Complete!")
print("Bronze → Raw data | Silver → Cleaned data")

# COMMAND ----------

# DBTITLE 1,Best Practices
# MAGIC %md
# MAGIC ## ✅ ELT Best Practices
# MAGIC
# MAGIC ### **1. Layer Your Data**
# MAGIC * **Bronze**: Raw, immutable source data
# MAGIC * **Silver**: Cleaned, validated, deduplicated
# MAGIC * **Gold**: Business-level aggregations
# MAGIC
# MAGIC ### **2. Use Delta Lake**
# MAGIC ```python
# MAGIC df.write.format("delta").mode("append").save("/path")
# MAGIC ```
# MAGIC
# MAGIC ### **3. Idempotent Pipelines**
# MAGIC * Re-running should produce same result
# MAGIC * Use `merge` for upserts
# MAGIC * Track processing timestamps
# MAGIC
# MAGIC ### **4. Monitor & Log**
# MAGIC * Log record counts at each stage
# MAGIC * Track processing time
# MAGIC * Set up alerts for failures
# MAGIC
# MAGIC ### **5. Incremental Processing**
# MAGIC * Don't reprocess everything
# MAGIC * Use Auto Loader for new files
# MAGIC * Use change data capture (CDC) for databases

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **ELT** is preferred over ETL in modern cloud architectures
# MAGIC ✅ **Extract → Load → Transform** leverages cheap storage
# MAGIC ✅ **Databricks + Delta Lake** = Perfect ELT platform
# MAGIC ✅ **Medallion Architecture**: Bronze → Silver → Gold
# MAGIC
# MAGIC ## 🎯 Key Takeaways
# MAGIC * Store raw data first (Bronze)
# MAGIC * Transform later with full Spark power (Silver/Gold)
# MAGIC * Delta Lake ensures reliability
# MAGIC * Incremental processing for efficiency
# MAGIC
# MAGIC ## ➡️ Next: Module 2.2 - Data Ingestion Techniques
# MAGIC
# MAGIC 🎉 **Module 2.1 Complete!**
