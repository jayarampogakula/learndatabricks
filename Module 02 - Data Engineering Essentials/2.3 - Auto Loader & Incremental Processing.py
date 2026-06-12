# Databricks notebook source
# DBTITLE 1,Module 2.3: Auto Loader
# MAGIC %md
# MAGIC # ⚡ Module 2.3: Auto Loader & Incremental Processing
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand incremental data processing
# MAGIC * Master Auto Loader for cloud files
# MAGIC * Build efficient data pipelines
# MAGIC * Handle schema evolution
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 What is Auto Loader?
# MAGIC
# MAGIC Auto Loader (`cloudFiles`) is Databricks' optimized solution for **incrementally ingesting new files** from cloud storage.
# MAGIC
# MAGIC ### **Why Auto Loader?**
# MAGIC ✅ **Automatic discovery** - Finds new files automatically
# MAGIC ✅ **Scalable** - Handles millions of files
# MAGIC ✅ **Schema inference** - Auto-detects column types
# MAGIC ✅ **Exactly-once** - No duplicates, no missed files
# MAGIC ✅ **Cost-effective** - Only processes new data
# MAGIC
# MAGIC ### **Without Auto Loader (Bad)**
# MAGIC ```python
# MAGIC # Processes ALL files every time!
# MAGIC df = spark.read.parquet("/data/")
# MAGIC ```
# MAGIC
# MAGIC ### **With Auto Loader (Good)** ⭐
# MAGIC ```python
# MAGIC # Only processes NEW files!
# MAGIC df = spark.readStream \
# MAGIC     .format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "parquet") \
# MAGIC     .load("/data/")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Auto Loader Syntax
# MAGIC %md
# MAGIC ## 📝 Auto Loader Syntax
# MAGIC
# MAGIC ### **Basic Pattern**
# MAGIC ```python
# MAGIC df = spark.readStream \
# MAGIC     .format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "<file_format>") \
# MAGIC     .option("cloudFiles.schemaLocation", "<checkpoint_path>") \
# MAGIC     .load("<source_path>")
# MAGIC
# MAGIC df.writeStream \
# MAGIC     .option("checkpointLocation", "<checkpoint_path>") \
# MAGIC     .toTable("<target_table>")
# MAGIC ```
# MAGIC
# MAGIC ### **Supported Formats**
# MAGIC * CSV, JSON, Parquet, Avro, ORC, Text
# MAGIC * Any format Spark can read!
# MAGIC
# MAGIC ### **Key Options**
# MAGIC | Option | Description |
# MAGIC |--------|-------------|
# MAGIC | `cloudFiles.format` | File format (csv, json, parquet) |
# MAGIC | `cloudFiles.schemaLocation` | Where to store inferred schema |
# MAGIC | `cloudFiles.useNotifications` | Use cloud events (faster) |
# MAGIC | `cloudFiles.schemaEvolutionMode` | Handle schema changes |

# COMMAND ----------

# DBTITLE 1,Demo - Auto Loader
# ⚡ Demo: Auto Loader for Incremental Processing

print("🚀 Auto Loader Demo")
print("=" * 50)

# Step 1: Create sample files (simulating landing zone)
print("\n1️⃣ Creating sample data files...")
sample_data = [(1, "Alice", 85), (2, "Bob", 92)]
df = spark.createDataFrame(sample_data, ["id", "name", "score"])
df.write.mode("overwrite").json("/tmp/landing/batch1")
print("✅ Created batch 1")

# Step 2: Read with Auto Loader
print("\n2️⃣ Setting up Auto Loader...")
stream_df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/tmp/schema/autoloader") \
    .load("/tmp/landing/")

print("✅ Auto Loader configured")
print("   Format: JSON")
print("   Source: /tmp/landing/")
print("   Schema: Auto-inferred and stored")

# Step 3: Write to target (batch mode for demo)
print("\n3️⃣ Processing data...")
query = stream_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoint/autoloader") \
    .trigger(availableNow=True) \
    .toTable("demo_autoloader")

query.awaitTermination()
print("✅ Data ingested successfully!")

# Verify
result_count = spark.table("demo_autoloader").count()
print(f"\n📊 Total records: {result_count}")
print("\n💡 Next run will only process NEW files!")

# COMMAND ----------

# DBTITLE 1,Incremental Patterns
# MAGIC %md
# MAGIC ## 🔄 Incremental Processing Patterns
# MAGIC
# MAGIC ### **1. Append-Only (New Records)**
# MAGIC ```python
# MAGIC df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .toTable("target")
# MAGIC ```
# MAGIC Use when: Adding new records only
# MAGIC
# MAGIC ### **2. Upsert (Merge Updates)**
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC def upsert_data(batch_df, batch_id):
# MAGIC     delta_table = DeltaTable.forName(spark, "target")
# MAGIC     delta_table.alias("target").merge(
# MAGIC         batch_df.alias("source"),
# MAGIC         "target.id = source.id"
# MAGIC     ).whenMatchedUpdateAll() \
# MAGIC      .whenNotMatchedInsertAll() \
# MAGIC      .execute()
# MAGIC
# MAGIC df.writeStream \
# MAGIC     .foreachBatch(upsert_data) \
# MAGIC     .start()
# MAGIC ```
# MAGIC Use when: Updating existing records
# MAGIC
# MAGIC ### **3. Watermarking (Late Data)**
# MAGIC ```python
# MAGIC df.withWatermark("timestamp", "1 hour") \
# MAGIC     .groupBy("user_id").count()
# MAGIC ```
# MAGIC Use when: Handling out-of-order events

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Auto Loader** = Incremental ingestion from cloud storage
# MAGIC ✅ **Only processes new files** = Cost & time savings
# MAGIC ✅ **Schema inference** = No manual schema management
# MAGIC ✅ **Exactly-once** = Reliable data processing
# MAGIC
# MAGIC ## 🎯 Key Benefits
# MAGIC * Automatic file discovery
# MAGIC * Scalable to millions of files
# MAGIC * Handles schema evolution
# MAGIC * Built-in checkpointing
# MAGIC
# MAGIC ## 💡 When to Use
# MAGIC ✅ **Use Auto Loader when**:
# MAGIC * Files land continuously in cloud storage
# MAGIC * You need incremental processing
# MAGIC * Files come from external systems
# MAGIC
# MAGIC ❌ **Don't use when**:
# MAGIC * One-time historical load (use batch read)
# MAGIC * Data already in Delta tables (use CDC)
# MAGIC
# MAGIC ## ➡️ Next: Module 2.4 - Medallion Architecture
# MAGIC
# MAGIC 🎉 **Module 2.3 Complete!**
