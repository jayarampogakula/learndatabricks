# Databricks notebook source
# DBTITLE 1,Module 10.1: Structured Streaming
# MAGIC %md
# MAGIC # 🌊 Module 10.1: Structured Streaming Fundamentals
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand Structured Streaming
# MAGIC * Process streaming data
# MAGIC * Handle real-time analytics
# MAGIC * Manage streaming state
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌊 What is Structured Streaming?
# MAGIC
# MAGIC **Structured Streaming** is Spark's scalable and fault-tolerant stream processing engine built on Spark SQL.
# MAGIC
# MAGIC ### **Key Features**
# MAGIC
# MAGIC ✅ **Unified API** - Same code for batch and streaming
# MAGIC ✅ **Exactly-Once** - Guaranteed processing semantics
# MAGIC ✅ **Fault-Tolerant** - Automatic failure recovery
# MAGIC ✅ **Late Data** - Handle out-of-order events
# MAGIC ✅ **State Management** - Stateful aggregations
# MAGIC ✅ **Incremental** - Process only new data
# MAGIC
# MAGIC ### **Why Structured Streaming?**
# MAGIC * **Real-time insights** - Process as data arrives
# MAGIC * **Low latency** - Sub-second processing
# MAGIC * **Scalability** - Handle high-volume streams
# MAGIC * **Simplicity** - Familiar DataFrame/SQL API
# MAGIC * **Reliability** - Exactly-once guarantees

# COMMAND ----------

# DBTITLE 1,Streaming Concepts
# MAGIC %md
# MAGIC ## 🧠 Core Concepts
# MAGIC
# MAGIC ### **Stream as Unbounded Table**
# MAGIC Think of streaming data as a table that grows continuously
# MAGIC
# MAGIC ```
# MAGIC Batch Processing:
# MAGIC +-----+--------+
# MAGIC | ID  | Value  |
# MAGIC +-----+--------+
# MAGIC | 1   | 100    |
# MAGIC | 2   | 200    |
# MAGIC +-----+--------+
# MAGIC
# MAGIC Streaming Processing:
# MAGIC +-----+--------+
# MAGIC | ID  | Value  | ← New rows arrive continuously
# MAGIC +-----+--------+
# MAGIC | 1   | 100    |
# MAGIC | 2   | 200    |
# MAGIC | 3   | 300    | ← New
# MAGIC | 4   | 400    | ← New
# MAGIC ...
# MAGIC ```
# MAGIC
# MAGIC ### **Triggers**
# MAGIC Control when to process data
# MAGIC
# MAGIC * **Default (micro-batch)** - Process as fast as possible
# MAGIC * **Fixed interval** - Every N seconds/minutes
# MAGIC * **Once** - Process all available data and stop
# MAGIC * **Available-now** - Process all available, then stop
# MAGIC * **Continuous** - Low-latency (~1ms)
# MAGIC
# MAGIC ### **Output Modes**
# MAGIC
# MAGIC 1. **Append** - Only new rows (default)
# MAGIC 2. **Complete** - Entire result table
# MAGIC 3. **Update** - Only updated rows
# MAGIC
# MAGIC ### **Checkpointing**
# MAGIC Track progress for fault tolerance
# MAGIC
# MAGIC ```python
# MAGIC .option("checkpointLocation", "/path/to/checkpoint")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Basic Streaming Example
# 🌊 Demo: Basic Structured Streaming

from pyspark.sql.functions import col, window, count

print("🌊 Structured Streaming Demo")
print("=" * 60)

# Read from a streaming source (example with rate source)
print("\n📊 Creating streaming DataFrame...")
streaming_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 10) \
    .load()

print("✅ Streaming DataFrame created")
print("   Schema:")
streaming_df.printSchema()

# Simple transformation
print("\n🔄 Applying transformations...")
transformed_df = streaming_df \
    .selectExpr(
        "timestamp",
        "value",
        "value % 10 as category"
    )

# Write to console (for demo)
print("\n📝 Starting streaming query...")
query = transformed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

print("✅ Streaming query started")
print(f"   Query ID: {query.id}")
print(f"   Status: {query.status}")

# Let it run for a few seconds
import time
time.sleep(5)

# Stop the query
query.stop()
print("\n⏹️ Streaming query stopped")

print("\n💡 Key Points:")
print("   • readStream - Read from streaming source")
print("   • writeStream - Write streaming results")
print("   • Transformations same as batch")
print("   • Query runs continuously until stopped")

# COMMAND ----------

# DBTITLE 1,Streaming Sources
# MAGIC %md
# MAGIC ## 📥 Streaming Sources
# MAGIC
# MAGIC ### **1. File Sources**
# MAGIC Monitor directory for new files
# MAGIC
# MAGIC ```python
# MAGIC # Read new JSON files
# MAGIC df = spark.readStream \
# MAGIC     .format("json") \
# MAGIC     .schema(schema) \
# MAGIC     .load("/path/to/files")
# MAGIC
# MAGIC # Read new CSV files
# MAGIC df = spark.readStream \
# MAGIC     .format("csv") \
# MAGIC     .option("header", True) \
# MAGIC     .schema(schema) \
# MAGIC     .load("/path/to/files")
# MAGIC ```
# MAGIC
# MAGIC ### **2. Kafka**
# MAGIC Read from Kafka topics
# MAGIC
# MAGIC ```python
# MAGIC df = spark.readStream \
# MAGIC     .format("kafka") \
# MAGIC     .option("kafka.bootstrap.servers", "host:port") \
# MAGIC     .option("subscribe", "topic_name") \
# MAGIC     .load()
# MAGIC ```
# MAGIC
# MAGIC ### **3. Delta Lake**
# MAGIC Stream changes from Delta tables
# MAGIC
# MAGIC ```python
# MAGIC df = spark.readStream \
# MAGIC     .format("delta") \
# MAGIC     .load("/path/to/delta/table")
# MAGIC ```
# MAGIC
# MAGIC ### **4. Auto Loader**
# MAGIC Efficient file ingestion (Databricks)
# MAGIC
# MAGIC ```python
# MAGIC df = spark.readStream \
# MAGIC     .format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .schema(schema) \
# MAGIC     .load("/path/to/files")
# MAGIC ```
# MAGIC
# MAGIC ### **5. Rate Source**
# MAGIC Generate test data
# MAGIC
# MAGIC ```python
# MAGIC # For testing
# MAGIC df = spark.readStream \
# MAGIC     .format("rate") \
# MAGIC     .option("rowsPerSecond", 10) \
# MAGIC     .load()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Streaming Sinks
# MAGIC %md
# MAGIC ## 📤 Streaming Sinks
# MAGIC
# MAGIC ### **1. Delta Lake (Recommended)**
# MAGIC Write to Delta tables
# MAGIC
# MAGIC ```python
# MAGIC query = df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/checkpoint") \
# MAGIC     .table("output_table")
# MAGIC ```
# MAGIC
# MAGIC ### **2. Console**
# MAGIC Debug output
# MAGIC
# MAGIC ```python
# MAGIC query = df.writeStream \
# MAGIC     .format("console") \
# MAGIC     .outputMode("append") \
# MAGIC     .start()
# MAGIC ```
# MAGIC
# MAGIC ### **3. Memory**
# MAGIC In-memory table (testing)
# MAGIC
# MAGIC ```python
# MAGIC query = df.writeStream \
# MAGIC     .format("memory") \
# MAGIC     .queryName("my_table") \
# MAGIC     .start()
# MAGIC
# MAGIC # Query the in-memory table
# MAGIC spark.sql("SELECT * FROM my_table").show()
# MAGIC ```
# MAGIC
# MAGIC ### **4. Kafka**
# MAGIC Write to Kafka
# MAGIC
# MAGIC ```python
# MAGIC query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
# MAGIC     .writeStream \
# MAGIC     .format("kafka") \
# MAGIC     .option("kafka.bootstrap.servers", "host:port") \
# MAGIC     .option("topic", "output_topic") \
# MAGIC     .option("checkpointLocation", "/checkpoint") \
# MAGIC     .start()
# MAGIC ```
# MAGIC
# MAGIC ### **5. Files**
# MAGIC Write to Parquet/JSON
# MAGIC
# MAGIC ```python
# MAGIC query = df.writeStream \
# MAGIC     .format("parquet") \
# MAGIC     .option("path", "/output/path") \
# MAGIC     .option("checkpointLocation", "/checkpoint") \
# MAGIC     .start()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Windowing and Aggregations
# MAGIC %md
# MAGIC ## 🪟 Windowing and Aggregations
# MAGIC
# MAGIC ### **Tumbling Windows**
# MAGIC Fixed, non-overlapping windows
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import window, count
# MAGIC
# MAGIC # 5-minute tumbling windows
# MAGIC windowed = df \
# MAGIC     .groupBy(window("timestamp", "5 minutes")) \
# MAGIC     .agg(count("*").alias("count"))
# MAGIC ```
# MAGIC
# MAGIC ### **Sliding Windows**
# MAGIC Overlapping windows
# MAGIC
# MAGIC ```python
# MAGIC # 10-minute windows, sliding every 5 minutes
# MAGIC windowed = df \
# MAGIC     .groupBy(window("timestamp", "10 minutes", "5 minutes")) \
# MAGIC     .agg(count("*").alias("count"))
# MAGIC ```
# MAGIC
# MAGIC ### **Session Windows**
# MAGIC Dynamic windows based on gaps
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import session_window
# MAGIC
# MAGIC # Windows with 30-minute gaps
# MAGIC windowed = df \
# MAGIC     .groupBy(session_window("timestamp", "30 minutes")) \
# MAGIC     .agg(count("*").alias("count"))
# MAGIC ```
# MAGIC
# MAGIC ### **Watermarking**
# MAGIC Handle late data
# MAGIC
# MAGIC ```python
# MAGIC # Tolerate up to 10 minutes of late data
# MAGIC windowed = df \
# MAGIC     .withWatermark("timestamp", "10 minutes") \
# MAGIC     .groupBy(window("timestamp", "5 minutes")) \
# MAGIC     .agg(count("*").alias("count"))
# MAGIC ```
# MAGIC
# MAGIC ### **Complete Example**
# MAGIC ```python
# MAGIC from pyspark.sql.functions import window, avg
# MAGIC
# MAGIC streaming_df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/checkpoint") \
# MAGIC     .start()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Managing Streaming Queries
# 🎛️ Demo: Managing Streaming Queries

print("🎛️ Streaming Query Management")
print("=" * 60)

# Start a streaming query
print("\n🚀 Starting streaming query...")
streaming_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5) \
    .load()

query = streaming_df.writeStream \
    .format("memory") \
    .queryName("demo_query") \
    .start()

print(f"✅ Query started: {query.name}")

# Wait a moment
import time
time.sleep(3)

# Check query status
print("\n📊 Query Status:")
print(f"   ID: {query.id}")
print(f"   Name: {query.name}")
print(f"   Is Active: {query.isActive}")
print(f"   Recent Progress:")
if query.recentProgress:
    progress = query.recentProgress[-1]
    print(f"      Batch: {progress.get('batchId', 'N/A')}")
    print(f"      Rows: {progress.get('numInputRows', 0)}")

# List all active queries
print("\n📋 All Active Queries:")
for q in spark.streams.active:
    print(f"   • {q.name} (ID: {q.id})")

# Stop the query
print("\n⏹️ Stopping query...")
query.stop()
query.awaitTermination(timeout=5)
print("✅ Query stopped")

print("\n💡 Query Management:")
print("   • query.status - Current status")
print("   • query.recentProgress - Recent metrics")
print("   • query.stop() - Stop query")
print("   • query.awaitTermination() - Wait for completion")
print("   • spark.streams.active - List all queries")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Structured Streaming** - Unified API for batch and streaming
# MAGIC ✅ **Exactly-Once** - Guaranteed processing semantics
# MAGIC ✅ **Sources** - Files, Kafka, Delta, Auto Loader
# MAGIC ✅ **Sinks** - Delta Lake, Kafka, console
# MAGIC ✅ **Windowing** - Tumbling, sliding, session windows
# MAGIC ✅ **Watermarking** - Handle late data
# MAGIC ✅ **Checkpointing** - Fault tolerance
# MAGIC
# MAGIC ## 🎯 Key Concepts
# MAGIC
# MAGIC ### **Basic Pattern**
# MAGIC ```python
# MAGIC # Read stream
# MAGIC df = spark.readStream.format(...).load(...)
# MAGIC
# MAGIC # Transform (same as batch)
# MAGIC result = df.filter(...).select(...)
# MAGIC
# MAGIC # Write stream
# MAGIC query = result.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/checkpoint") \
# MAGIC     .start()
# MAGIC ```
# MAGIC
# MAGIC ### **Output Modes**
# MAGIC * **Append** - Only new rows (most common)
# MAGIC * **Complete** - Full result table (aggregations)
# MAGIC * **Update** - Only changed rows
# MAGIC
# MAGIC ### **Triggers**
# MAGIC ```python
# MAGIC # Micro-batch (default)
# MAGIC .trigger(processingTime="10 seconds")
# MAGIC
# MAGIC # Once (process all and stop)
# MAGIC .trigger(once=True)
# MAGIC
# MAGIC # Available now (process all, then stop)
# MAGIC .trigger(availableNow=True)
# MAGIC ```
# MAGIC
# MAGIC ## 🎯 Best Practices
# MAGIC
# MAGIC ✅ **Always checkpoint** - Required for production
# MAGIC ✅ **Use Delta Lake** - Best streaming sink
# MAGIC ✅ **Set watermarks** - For windowed aggregations
# MAGIC ✅ **Monitor progress** - query.recentProgress
# MAGIC ✅ **Handle late data** - Watermarking
# MAGIC ✅ **Right-size batches** - Balance latency vs throughput
# MAGIC ✅ **Test with rate source** - Before production
# MAGIC
# MAGIC ## 💡 Remember
# MAGIC
# MAGIC * Streaming = Unbounded table
# MAGIC * Same transformations as batch
# MAGIC * Checkpoint for fault tolerance
# MAGIC * Delta Lake for exactly-once
# MAGIC * Watermarks for late data
# MAGIC * Monitor query progress
# MAGIC
# MAGIC ## ➡️ Next: Module 10.2 - Spark Declarative Pipelines (SDP)
# MAGIC
# MAGIC 🎉 **Module 10.1 Complete!**
