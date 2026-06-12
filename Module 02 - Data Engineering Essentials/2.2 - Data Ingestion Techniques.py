# Databricks notebook source
# DBTITLE 1,Module 2.2: Data Ingestion
# MAGIC %md
# MAGIC # 📥 Module 2.2: Data Ingestion Techniques
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand batch vs streaming ingestion
# MAGIC * Learn to read from multiple data sources
# MAGIC * Master different file formats
# MAGIC * Build robust ingestion pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📦 Ingestion Patterns
# MAGIC
# MAGIC ### **1. Batch Ingestion**
# MAGIC * Process data in scheduled intervals (hourly, daily)
# MAGIC * Use `spark.read` for one-time loads
# MAGIC * Good for: Historical data, reporting, overnight jobs
# MAGIC
# MAGIC ### **2. Streaming Ingestion**
# MAGIC * Process data continuously as it arrives
# MAGIC * Use `spark.readStream` for real-time
# MAGIC * Good for: Real-time analytics, fraud detection, IoT
# MAGIC
# MAGIC ### **3. Micro-Batch**
# MAGIC * Hybrid: Small batches processed frequently
# MAGIC * Databricks default for streaming
# MAGIC * Balance between latency and throughput
# MAGIC
# MAGIC | Pattern | Latency | Complexity | Use Case |
# MAGIC |---------|---------|------------|----------|
# MAGIC | **Batch** | Hours | Low | Reports, ETL |
# MAGIC | **Micro-batch** | Minutes | Medium | Near real-time |
# MAGIC | **Streaming** | Seconds | High | Real-time apps |

# COMMAND ----------

# DBTITLE 1,Reading File Formats
# MAGIC %md
# MAGIC ## 📄 Common File Formats
# MAGIC
# MAGIC ### **1. CSV (Comma Separated)**
# MAGIC ```python
# MAGIC df = spark.read.format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .option("inferSchema", "true") \
# MAGIC     .load("/path/to/file.csv")
# MAGIC ```
# MAGIC ⚠️ Slow for large datasets, use Parquet instead
# MAGIC
# MAGIC ### **2. Parquet (Columnar)**
# MAGIC ```python
# MAGIC df = spark.read.parquet("/path/to/file.parquet")
# MAGIC ```
# MAGIC ✅ Fast, compressed, recommended for analytics
# MAGIC
# MAGIC ### **3. JSON (Semi-structured)**
# MAGIC ```python
# MAGIC df = spark.read.json("/path/to/file.json")
# MAGIC ```
# MAGIC ✅ Good for nested data, APIs
# MAGIC
# MAGIC ### **4. Delta Lake** ⭐ (Recommended)
# MAGIC ```python
# MAGIC df = spark.read.format("delta").load("/path/to/delta_table")
# MAGIC ```
# MAGIC ✅ ACID, time travel, schema evolution

# COMMAND ----------

# DBTITLE 1,Demo - Read Multiple Formats
# 📚 Demo: Reading Different File Formats

print("📥 Data Ingestion Examples")
print("=" * 50)

# Create sample data
sample_data = [(1, "Alice", 85), (2, "Bob", 92), (3, "Charlie", 78)]
df = spark.createDataFrame(sample_data, ["id", "name", "score"])

# 1. Write & Read CSV
print("\n1️⃣ CSV Format:")
df.write.mode("overwrite").option("header", "true").csv("/tmp/demo.csv")
df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv("/tmp/demo.csv")
print(f"   Rows: {df_csv.count()}")

# 2. Write & Read Parquet
print("\n2️⃣ Parquet Format:")
df.write.mode("overwrite").parquet("/tmp/demo.parquet")
df_parquet = spark.read.parquet("/tmp/demo.parquet")
print(f"   Rows: {df_parquet.count()} | ✅ Faster & compressed")

# 3. Write & Read JSON
print("\n3️⃣ JSON Format:")
df.write.mode("overwrite").json("/tmp/demo.json")
df_json = spark.read.json("/tmp/demo.json")
print(f"   Rows: {df_json.count()} | Good for nested data")

# 4. Write & Read Delta
print("\n4️⃣ Delta Lake Format: ⭐")
df.write.mode("overwrite").format("delta").save("/tmp/demo.delta")
df_delta = spark.read.format("delta").load("/tmp/demo.delta")
print(f"   Rows: {df_delta.count()} | ✅ ACID + Time Travel")

print("\n🎯 Recommendation: Use Delta Lake for production!")

# COMMAND ----------

# DBTITLE 1,Data Sources
# MAGIC %md
# MAGIC ## 📊 Data Sources
# MAGIC
# MAGIC ### **Cloud Storage**
# MAGIC ```python
# MAGIC # S3 (AWS)
# MAGIC df = spark.read.parquet("s3://bucket/path/")
# MAGIC
# MAGIC # ADLS (Azure)
# MAGIC df = spark.read.parquet("abfss://container@account.dfs.core.windows.net/path/")
# MAGIC
# MAGIC # GCS (Google Cloud)
# MAGIC df = spark.read.parquet("gs://bucket/path/")
# MAGIC ```
# MAGIC
# MAGIC ### **Databases (JDBC)**
# MAGIC ```python
# MAGIC df = spark.read.format("jdbc") \
# MAGIC     .option("url", "jdbc:postgresql://host:5432/db") \
# MAGIC     .option("dbtable", "users") \
# MAGIC     .option("user", "username") \
# MAGIC     .option("password", "password") \
# MAGIC     .load()
# MAGIC ```
# MAGIC
# MAGIC ### **Streaming Sources**
# MAGIC ```python
# MAGIC # Kafka
# MAGIC df = spark.readStream.format("kafka") \
# MAGIC     .option("kafka.bootstrap.servers", "host:9092") \
# MAGIC     .option("subscribe", "topic") \
# MAGIC     .load()
# MAGIC
# MAGIC # Event Hubs (Azure)
# MAGIC df = spark.readStream.format("eventhubs").load()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Batch** for scheduled loads, **Streaming** for real-time
# MAGIC ✅ **File Formats**: Delta > Parquet > CSV
# MAGIC ✅ **Sources**: Cloud storage, databases, streaming
# MAGIC ✅ **Always use Delta Lake** for production tables
# MAGIC
# MAGIC ## 🎯 Key Points
# MAGIC * Start with batch ingestion, add streaming when needed
# MAGIC * Parquet/Delta for performance
# MAGIC * Auto Loader for cloud files (next module!)
# MAGIC * Schema inference helpful but validate in production
# MAGIC
# MAGIC ## ➡️ Next: Module 2.3 - Auto Loader & Incremental Processing
# MAGIC
# MAGIC 🎉 **Module 2.2 Complete!**
