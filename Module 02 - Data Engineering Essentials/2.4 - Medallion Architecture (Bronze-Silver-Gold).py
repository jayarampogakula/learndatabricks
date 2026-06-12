# Databricks notebook source
# DBTITLE 1,Module 2.4: Medallion Architecture
# MAGIC %md
# MAGIC # 🏅 Module 2.4: Medallion Architecture
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand Bronze-Silver-Gold pattern
# MAGIC * Learn data quality improvement across layers
# MAGIC * Build production-ready data pipelines
# MAGIC * Apply best practices for each layer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ What is Medallion Architecture?
# MAGIC
# MAGIC A **data design pattern** that organizes data into three layers with **increasing quality**:
# MAGIC
# MAGIC ```
# MAGIC Bronze → Silver → Gold
# MAGIC (Raw)    (Clean)   (Business)
# MAGIC ```
# MAGIC
# MAGIC ### **Why Medallion?**
# MAGIC ✅ **Clear data flow** - Easy to understand
# MAGIC ✅ **Incremental quality** - Improve at each stage
# MAGIC ✅ **Reusability** - Silver tables used by multiple Gold
# MAGIC ✅ **Debugging** - Track data through layers
# MAGIC ✅ **Governance** - Apply security by layer
# MAGIC
# MAGIC ### **Data Flow**
# MAGIC ```
# MAGIC Source Systems → Bronze (Raw)
# MAGIC                     ↓
# MAGIC                  Silver (Cleaned)
# MAGIC                     ↓
# MAGIC                  Gold (Aggregated)
# MAGIC                     ↓
# MAGIC               BI Tools & ML Models
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,The Three Layers
# MAGIC %md
# MAGIC ## 🪙 Bronze Layer (Raw Zone)
# MAGIC
# MAGIC ### **Purpose**: Store raw, immutable source data
# MAGIC
# MAGIC **Characteristics**:
# MAGIC * 📥 Exact copy of source data
# MAGIC * 🔒 Immutable - never modified
# MAGIC * 📅 Includes ingestion metadata
# MAGIC * 📋 Full history retained
# MAGIC
# MAGIC **Example**:
# MAGIC ```python
# MAGIC # Bronze: Raw JSON from API
# MAGIC df_bronze = spark.readStream \
# MAGIC     .format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .load("/landing/")
# MAGIC
# MAGIC df_bronze.writeStream.toTable("bronze.raw_events")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🪙 Silver Layer (Cleaned Zone)
# MAGIC
# MAGIC ### **Purpose**: Cleaned, validated, deduplicated data
# MAGIC
# MAGIC **Characteristics**:
# MAGIC * ✨ Data quality applied
# MAGIC * 📄 Schema enforcement
# MAGIC * 🚫 Duplicates removed
# MAGIC * 🔄 Type conversions applied
# MAGIC
# MAGIC **Transformations**:
# MAGIC * Parse timestamps
# MAGIC * Remove nulls
# MAGIC * Standardize formats
# MAGIC * Deduplicate records
# MAGIC
# MAGIC **Example**:
# MAGIC ```python
# MAGIC # Silver: Clean and validate
# MAGIC df_silver = spark.readStream.table("bronze.raw_events") \
# MAGIC     .filter(col("user_id").isNotNull()) \
# MAGIC     .withColumn("event_time", to_timestamp("timestamp")) \
# MAGIC     .dropDuplicates(["event_id"])
# MAGIC
# MAGIC df_silver.writeStream.toTable("silver.clean_events")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🪙 Gold Layer (Business Zone)
# MAGIC
# MAGIC ### **Purpose**: Business-level aggregations
# MAGIC
# MAGIC **Characteristics**:
# MAGIC * 📊 Aggregated metrics
# MAGIC * 🎯 Business logic applied
# MAGIC * 🚀 Optimized for queries
# MAGIC * 📈 BI-ready
# MAGIC
# MAGIC **Transformations**:
# MAGIC * Aggregations (SUM, COUNT, AVG)
# MAGIC * Joins across domains
# MAGIC * Business calculations
# MAGIC * Star/snowflake schemas
# MAGIC
# MAGIC **Example**:
# MAGIC ```python
# MAGIC # Gold: Business metrics
# MAGIC df_gold = spark.readStream.table("silver.clean_events") \
# MAGIC     .groupBy("user_id", window("event_time", "1 day")) \
# MAGIC     .agg(
# MAGIC         count("*").alias("event_count"),
# MAGIC         countDistinct("session_id").alias("sessions")
# MAGIC     )
# MAGIC
# MAGIC df_gold.writeStream.toTable("gold.daily_user_activity")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo - Medallion Pipeline
# 🏅 Demo: Complete Medallion Pipeline

from pyspark.sql.functions import col, to_timestamp, upper, current_timestamp

print("🏅 Building Medallion Architecture")
print("=" * 60)

# Simulate source data
raw_data = [
    {"id": 1, "name": "alice", "email": "alice@example.com", "timestamp": "2024-01-15 10:30"},
    {"id": 2, "name": "bob", "email": "bob@example.com", "timestamp": "2024-01-15 11:20"},
    {"id": 1, "name": "alice", "email": "alice@example.com", "timestamp": "2024-01-15 10:30"},  # duplicate
]

# BRONZE: Raw ingestion
print("\n🪙 BRONZE Layer: Raw ingestion")
df_bronze = spark.createDataFrame(raw_data) \
    .withColumn("ingestion_time", current_timestamp())

df_bronze.write.mode("overwrite").format("delta").save("/tmp/medallion/bronze/users")
print(f"   ✅ Stored {df_bronze.count()} raw records (includes duplicate)")

# SILVER: Clean and validate
print("\n🪙 SILVER Layer: Cleaning & validation")
df_silver = spark.read.format("delta").load("/tmp/medallion/bronze/users") \
    .withColumn("name", upper(col("name"))) \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .dropDuplicates(["id", "timestamp"])

df_silver.write.mode("overwrite").format("delta").save("/tmp/medallion/silver/users")
print(f"   ✅ Cleaned to {df_silver.count()} records (duplicate removed)")
print("   ✅ Applied: Uppercase names, parsed timestamps")

# GOLD: Business aggregation
print("\n🪙 GOLD Layer: Business metrics")
df_gold = spark.read.format("delta").load("/tmp/medallion/silver/users") \
    .groupBy("name").count() \
    .withColumnRenamed("count", "total_records")

df_gold.write.mode("overwrite").format("delta").save("/tmp/medallion/gold/user_summary")
print(f"   ✅ Created summary: {df_gold.count()} users")
print("\n📊 Gold table ready for BI tools!")

# Show final result
print("\n🎯 Final Gold Output:")
spark.read.format("delta").load("/tmp/medallion/gold/user_summary").show()

print("\n✅ Medallion Pipeline Complete!")
print("   Bronze → Silver → Gold")

# COMMAND ----------

# DBTITLE 1,Layer Comparison
# MAGIC %md
# MAGIC ## 📊 Layer Comparison
# MAGIC
# MAGIC | Aspect | Bronze | Silver | Gold |
# MAGIC |--------|--------|--------|------|
# MAGIC | **Data** | Raw | Cleaned | Aggregated |
# MAGIC | **Quality** | Low | High | Very High |
# MAGIC | **Schema** | Flexible | Enforced | Business |
# MAGIC | **Users** | Data Engineers | Analysts | Business Users |
# MAGIC | **Size** | Largest | Medium | Smallest |
# MAGIC | **Updates** | Append-only | Upsert | Replace |
# MAGIC
# MAGIC ## ✅ Best Practices
# MAGIC
# MAGIC ### **Bronze Layer**
# MAGIC * Keep everything - storage is cheap
# MAGIC * Add ingestion timestamp
# MAGIC * Never delete or modify
# MAGIC * Use Auto Loader for files
# MAGIC
# MAGIC ### **Silver Layer**
# MAGIC * Apply schema enforcement
# MAGIC * Remove duplicates
# MAGIC * Handle nulls appropriately
# MAGIC * Add data quality checks
# MAGIC
# MAGIC ### **Gold Layer**
# MAGIC * Model for business questions
# MAGIC * Optimize for query performance
# MAGIC * Add business logic here
# MAGIC * Update based on Silver changes

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Medallion Architecture** = Bronze → Silver → Gold
# MAGIC ✅ **Bronze**: Raw, immutable source data
# MAGIC ✅ **Silver**: Cleaned, validated, deduplicated
# MAGIC ✅ **Gold**: Business-level aggregations
# MAGIC
# MAGIC ## 🎯 Key Principles
# MAGIC * Each layer adds value and quality
# MAGIC * Bronze never changes (audit trail)
# MAGIC * Silver is the foundation for analytics
# MAGIC * Gold is optimized for business users
# MAGIC
# MAGIC ## 💡 Real-World Example
# MAGIC ```
# MAGIC Bronze: Raw API logs (JSON)
# MAGIC    ↓
# MAGIC Silver: Parsed, validated user events
# MAGIC    ↓
# MAGIC Gold: Daily active users, revenue metrics
# MAGIC ```
# MAGIC
# MAGIC ## 🎓 Module 02 Complete!
# MAGIC
# MAGIC You've learned:
# MAGIC * ETL vs ELT patterns
# MAGIC * Data ingestion techniques
# MAGIC * Auto Loader for incremental processing
# MAGIC * Medallion Architecture
# MAGIC
# MAGIC ## ➡️ Next: Module 03 - Delta Lake Deep Dive
# MAGIC
# MAGIC 🎉 **Congratulations on completing Module 02!**
