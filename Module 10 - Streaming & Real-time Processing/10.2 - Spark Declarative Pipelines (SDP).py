# Databricks notebook source
# DBTITLE 1,Module 10.2: Spark Declarative Pipelines
# MAGIC %md
# MAGIC # 🔄 Module 10.2: Spark Declarative Pipelines (SDP)
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand Spark Declarative Pipelines
# MAGIC * Build declarative ETL pipelines
# MAGIC * Implement data quality checks
# MAGIC * Manage pipeline lifecycle
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 What is Spark Declarative Pipelines?
# MAGIC
# MAGIC **Spark Declarative Pipelines (SDP)** (formerly Delta Live Tables) is a declarative framework for building reliable, maintainable data pipelines.
# MAGIC
# MAGIC ### **Key Features**
# MAGIC
# MAGIC ✅ **Declarative** - Define WHAT, not HOW
# MAGIC ✅ **Automatic** - Dependency management
# MAGIC ✅ **Data Quality** - Built-in expectations
# MAGIC ✅ **Monitoring** - Pipeline observability
# MAGIC ✅ **Auto-Scaling** - Elastic compute
# MAGIC ✅ **Exactly-Once** - Reliable processing
# MAGIC
# MAGIC ### **Why SDP?**
# MAGIC * **Simplicity** - Less code, more results
# MAGIC * **Reliability** - Built-in retry and recovery
# MAGIC * **Quality** - Enforce data expectations
# MAGIC * **Lineage** - Automatic tracking
# MAGIC * **Operations** - Managed infrastructure

# COMMAND ----------

# DBTITLE 1,Core Concepts
# MAGIC %md
# MAGIC ## 🧠 Core Concepts
# MAGIC
# MAGIC ### **Tables**
# MAGIC
# MAGIC #### **Streaming Tables**
# MAGIC Process incremental data
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC
# MAGIC @dlt.table(
# MAGIC     name="raw_orders",
# MAGIC     comment="Raw order data"
# MAGIC )
# MAGIC def raw_orders():
# MAGIC     return spark.readStream.format("cloudFiles") \
# MAGIC         .option("cloudFiles.format", "json") \
# MAGIC         .load("/data/orders")
# MAGIC ```
# MAGIC
# MAGIC #### **Materialized Views**
# MAGIC Batch or incremental processing
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC     name="clean_orders"
# MAGIC )
# MAGIC def clean_orders():
# MAGIC     return dlt.read("raw_orders") \
# MAGIC         .filter("amount > 0") \
# MAGIC         .select("order_id", "customer_id", "amount")
# MAGIC ```
# MAGIC
# MAGIC ### **Views**
# MAGIC Intermediate, non-materialized
# MAGIC
# MAGIC ```python
# MAGIC @dlt.view(
# MAGIC     name="filtered_orders"
# MAGIC )
# MAGIC def filtered_orders():
# MAGIC     return spark.read.table("raw_orders") \
# MAGIC         .filter("status = 'completed'")
# MAGIC ```
# MAGIC
# MAGIC ### **Dependencies**
# MAGIC Automatic from `dlt.read()`
# MAGIC
# MAGIC ```
# MAGIC raw_orders (streaming)
# MAGIC     ↓
# MAGIC clean_orders (materialized)
# MAGIC     ↓
# MAGIC order_metrics (materialized)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Data Quality Expectations
# MAGIC %md
# MAGIC ## ✅ Data Quality Expectations
# MAGIC
# MAGIC ### **Expect**
# MAGIC Log violations, continue
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC @dlt.expect("valid_amount", "amount > 0")
# MAGIC def orders():
# MAGIC     return spark.read.table("raw_orders")
# MAGIC ```
# MAGIC
# MAGIC ### **Expect or Drop**
# MAGIC Drop invalid records
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC @dlt.expect_or_drop("valid_email", "email IS NOT NULL")
# MAGIC def customers():
# MAGIC     return spark.read.table("raw_customers")
# MAGIC ```
# MAGIC
# MAGIC ### **Expect or Fail**
# MAGIC Stop pipeline on violations
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC @dlt.expect_or_fail("unique_id", "COUNT(*) = COUNT(DISTINCT id)")
# MAGIC def unique_records():
# MAGIC     return spark.read.table("source")
# MAGIC ```
# MAGIC
# MAGIC ### **Multiple Expectations**
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC @dlt.expect("valid_amount", "amount > 0")
# MAGIC @dlt.expect("valid_date", "order_date IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_status", "status IN ('pending', 'completed', 'cancelled')")
# MAGIC def validated_orders():
# MAGIC     return dlt.read("raw_orders")
# MAGIC ```
# MAGIC
# MAGIC ### **Benefits**
# MAGIC ✅ **Data Quality** - Enforce rules
# MAGIC ✅ **Monitoring** - Track violations
# MAGIC ✅ **Documentation** - Self-documenting
# MAGIC ✅ **Debugging** - Identify issues

# COMMAND ----------

# DBTITLE 1,Complete Pipeline Example
# 🔄 Demo: Complete SDP Pipeline

import dlt
from pyspark.sql.functions import col, current_timestamp

print("🔄 Spark Declarative Pipeline Example")
print("=" * 60)

print("\n📝 Pipeline Structure:")
print("""\nraw_orders (streaming)
  ↓
clean_orders (materialized + quality)
  ↓
order_metrics (aggregated)
""")

# 1. Raw streaming table
print("\n1️⃣ Raw Orders (Streaming):")
print("""\n@dlt.table(
    name="raw_orders",
    comment="Raw order data from files"
)
def raw_orders():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/data/orders")
""")

# 2. Clean with quality checks
print("\n2️⃣ Clean Orders (With Quality):")
print("""\n@dlt.table(
    name="clean_orders",
    comment="Cleaned and validated orders"
)
@dlt.expect("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_status", "status IN ('pending', 'completed')")
def clean_orders():
    return dlt.read_stream("raw_orders") \
        .select(
            "order_id",
            "customer_id",
            "amount",
            "status",
            "order_date"
        )
""")

# 3. Aggregated metrics
print("\n3️⃣ Order Metrics (Aggregation):")
print("""\n@dlt.table(
    name="order_metrics",
    comment="Daily order metrics"
)
def order_metrics():
    return dlt.read("clean_orders") \
        .groupBy("order_date", "status") \
        .agg(
            count("*").alias("order_count"),
            sum("amount").alias("total_amount")
        )
""")

print("\n✅ Key Features:")
print("   • Automatic dependency resolution")
print("   • Built-in data quality checks")
print("   • Streaming and batch processing")
print("   • Automatic retries on failure")
print("   • Complete lineage tracking")

# COMMAND ----------

# DBTITLE 1,Change Data Capture
# MAGIC %md
# MAGIC ## 🔄 Change Data Capture (CDC)
# MAGIC
# MAGIC ### **Apply Changes**
# MAGIC Process CDC feeds (inserts, updates, deletes)
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC @dlt.table(
# MAGIC     name="target_table"
# MAGIC )
# MAGIC def target_table():
# MAGIC     return None  # Initial state
# MAGIC
# MAGIC dlt.apply_changes(
# MAGIC     target="target_table",
# MAGIC     source="cdc_stream",
# MAGIC     keys=["id"],
# MAGIC     sequence_by="updated_at",
# MAGIC     stored_as_scd_type="1"  # or "2" for SCD Type 2
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### **SCD Type 1**
# MAGIC Overwrite with latest
# MAGIC
# MAGIC ```python
# MAGIC dlt.apply_changes(
# MAGIC     target="customers",
# MAGIC     source="customer_changes",
# MAGIC     keys=["customer_id"],
# MAGIC     sequence_by="updated_at",
# MAGIC     stored_as_scd_type="1"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### **SCD Type 2**
# MAGIC Track historical changes
# MAGIC
# MAGIC ```python
# MAGIC dlt.apply_changes(
# MAGIC     target="customers_history",
# MAGIC     source="customer_changes",
# MAGIC     keys=["customer_id"],
# MAGIC     sequence_by="updated_at",
# MAGIC     stored_as_scd_type="2",
# MAGIC     track_history_column_list=["email", "phone"]
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### **CDC Benefits**
# MAGIC ✅ **Automatic** - Handle inserts/updates/deletes
# MAGIC ✅ **Ordering** - Process changes in order
# MAGIC ✅ **Deduplication** - Handle duplicates
# MAGIC ✅ **History** - Track changes (SCD Type 2)

# COMMAND ----------

# DBTITLE 1,Pipeline Management
# MAGIC %md
# MAGIC ## 🎛️ Pipeline Management
# MAGIC
# MAGIC ### **Pipeline Modes**
# MAGIC
# MAGIC #### **Triggered**
# MAGIC Run once, process all data
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "continuous": false
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Use for:**
# MAGIC * Batch processing
# MAGIC * Scheduled runs
# MAGIC * Development/testing
# MAGIC
# MAGIC #### **Continuous**
# MAGIC Always running, process incrementally
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "continuous": true
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Use for:**
# MAGIC * Real-time pipelines
# MAGIC * Low-latency requirements
# MAGIC * Streaming data
# MAGIC
# MAGIC ### **Development vs Production**
# MAGIC
# MAGIC #### **Development Mode**
# MAGIC * Fast iteration
# MAGIC * Reuse clusters
# MAGIC * Lower cost
# MAGIC * No SLA
# MAGIC
# MAGIC #### **Production Mode**
# MAGIC * Auto-scaling
# MAGIC * Auto-recovery
# MAGIC * Enhanced monitoring
# MAGIC * Production SLA
# MAGIC
# MAGIC ### **Pipeline Configuration**
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "id": "my-pipeline",
# MAGIC   "name": "Sales ETL Pipeline",
# MAGIC   "storage": "/pipelines/sales",
# MAGIC   "target": "sales_db",
# MAGIC   "continuous": false,
# MAGIC   "development": false,
# MAGIC   "libraries": [
# MAGIC     {"notebook": {"path": "/Pipelines/sales_pipeline"}}
# MAGIC   ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Best Practices
# MAGIC %md
# MAGIC ## 🎯 Best Practices
# MAGIC
# MAGIC ### **Pipeline Design**
# MAGIC
# MAGIC ✅ **Bronze-Silver-Gold**
# MAGIC Medallion architecture
# MAGIC
# MAGIC ```
# MAGIC Bronze (raw)
# MAGIC   ↓
# MAGIC Silver (clean, validated)
# MAGIC   ↓
# MAGIC Gold (aggregated, business)
# MAGIC ```
# MAGIC
# MAGIC ✅ **Use expectations**
# MAGIC ```python
# MAGIC @dlt.expect_or_drop("valid_data", "field IS NOT NULL")
# MAGIC ```
# MAGIC
# MAGIC ✅ **Meaningful names**
# MAGIC ```python
# MAGIC @dlt.table(name="clean_orders", comment="Validated orders")
# MAGIC ```
# MAGIC
# MAGIC ✅ **Incremental processing**
# MAGIC ```python
# MAGIC # Use streaming tables when possible
# MAGIC @dlt.table
# MAGIC def orders():
# MAGIC     return spark.readStream...  # Incremental
# MAGIC ```
# MAGIC
# MAGIC ### **Data Quality**
# MAGIC
# MAGIC ✅ **Layer-appropriate checks**
# MAGIC * **Bronze** - Basic validity
# MAGIC * **Silver** - Business rules
# MAGIC * **Gold** - Aggregation rules
# MAGIC
# MAGIC ✅ **Monitor expectations**
# MAGIC * Review data quality metrics
# MAGIC * Alert on high violation rates
# MAGIC * Document quality rules
# MAGIC
# MAGIC ### **Operations**
# MAGIC
# MAGIC ✅ **Use development mode** for testing
# MAGIC ✅ **Production mode** for live
# MAGIC ✅ **Monitor pipeline health**
# MAGIC ✅ **Set up alerts** for failures
# MAGIC ✅ **Version control** pipeline code
# MAGIC
# MAGIC ### **Performance**
# MAGIC
# MAGIC ✅ **Auto-scaling** in production
# MAGIC ✅ **Optimize data layout** (Z-Order)
# MAGIC ✅ **Partition** appropriately
# MAGIC ✅ **Monitor** cluster utilization

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Spark Declarative Pipelines** - Declarative ETL framework
# MAGIC ✅ **Streaming Tables** - Incremental processing
# MAGIC ✅ **Materialized Views** - Batch or incremental
# MAGIC ✅ **Expectations** - Built-in data quality
# MAGIC ✅ **CDC** - Change data capture (SCD Type 1/2)
# MAGIC ✅ **Auto-Scaling** - Managed compute
# MAGIC ✅ **Monitoring** - Complete observability
# MAGIC
# MAGIC ## 🎯 Key Concepts
# MAGIC
# MAGIC ### **Table Types**
# MAGIC ```python
# MAGIC # Streaming
# MAGIC @dlt.table
# MAGIC def streaming_table():
# MAGIC     return spark.readStream...
# MAGIC
# MAGIC # Materialized
# MAGIC @dlt.table
# MAGIC def batch_table():
# MAGIC     return spark.read...
# MAGIC
# MAGIC # View (intermediate)
# MAGIC @dlt.view
# MAGIC def temp_view():
# MAGIC     return ...
# MAGIC ```
# MAGIC
# MAGIC ### **Data Quality**
# MAGIC ```python
# MAGIC @dlt.expect("rule", "condition")           # Log
# MAGIC @dlt.expect_or_drop("rule", "condition")  # Drop
# MAGIC @dlt.expect_or_fail("rule", "condition")  # Fail
# MAGIC ```
# MAGIC
# MAGIC ### **CDC**
# MAGIC ```python
# MAGIC dlt.apply_changes(
# MAGIC     target="table",
# MAGIC     source="changes",
# MAGIC     keys=["id"],
# MAGIC     sequence_by="timestamp",
# MAGIC     stored_as_scd_type="1"  # or "2"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### **Medallion Architecture**
# MAGIC ```
# MAGIC Bronze: Raw, minimal processing
# MAGIC   ↓
# MAGIC Silver: Cleaned, validated, conformed
# MAGIC   ↓
# MAGIC Gold: Business-level aggregates
# MAGIC ```
# MAGIC
# MAGIC ## 💡 Remember
# MAGIC
# MAGIC * **Declarative** - Define what, not how
# MAGIC * **Dependencies** - Automatic from dlt.read()
# MAGIC * **Quality** - Use expectations at every layer
# MAGIC * **Streaming** - Use streaming tables for incremental
# MAGIC * **CDC** - Use apply_changes for updates
# MAGIC * **Monitoring** - Review pipeline metrics
# MAGIC * **Development mode** - For testing
# MAGIC * **Production mode** - For live pipelines
# MAGIC
# MAGIC ## 🎓 Module 10 Complete!
# MAGIC
# MAGIC You've mastered:
# MAGIC * Structured Streaming fundamentals
# MAGIC * Windowing and aggregations
# MAGIC * Spark Declarative Pipelines (SDP)
# MAGIC * Data quality expectations
# MAGIC * Change Data Capture
# MAGIC * Pipeline operations
# MAGIC
# MAGIC ## ➡️ Next: Module 11 - Security & Compliance
# MAGIC
# MAGIC 🎉 **Congratulations on completing Module 10!**
