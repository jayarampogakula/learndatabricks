# Databricks notebook source
# DBTITLE 1,Module 3.1: Delta Lake Fundamentals
# MAGIC %md
# MAGIC # 🔷 Module 3.1: Delta Lake Fundamentals & ACID
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand what Delta Lake is and why it matters
# MAGIC * Master ACID transaction properties
# MAGIC * Learn Delta Lake architecture
# MAGIC * Build reliable data pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌊 What is Delta Lake?
# MAGIC
# MAGIC **Delta Lake** is an **open-source storage layer** that brings **ACID transactions** to data lakes.
# MAGIC
# MAGIC ### **The Problem Delta Solves**
# MAGIC
# MAGIC **Traditional Data Lakes** (Parquet, CSV, JSON):
# MAGIC * ❌ No ACID transactions
# MAGIC * ❌ Data corruption on failures
# MAGIC * ❌ Difficult to update/delete
# MAGIC * ❌ No schema enforcement
# MAGIC * ❌ Slow metadata operations
# MAGIC
# MAGIC **Delta Lake** (ACID on data lakes):
# MAGIC * ✅ ACID transactions
# MAGIC * ✅ Reliable writes
# MAGIC * ✅ Easy updates/deletes
# MAGIC * ✅ Schema enforcement
# MAGIC * ✅ Fast metadata
# MAGIC * ✅ Time travel
# MAGIC * ✅ Scalable
# MAGIC
# MAGIC ### **Key Features**
# MAGIC ```
# MAGIC ✨ ACID Transactions
# MAGIC ⏰ Time Travel (versioning)
# MAGIC 🔄 MERGE/UPDATE/DELETE
# MAGIC 📊 Schema Evolution
# MAGIC 🚀 Performance Optimization
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,ACID Properties
# MAGIC %md
# MAGIC ## 🔐 ACID Properties Explained
# MAGIC
# MAGIC ### **A - Atomicity**
# MAGIC **All or nothing** - Transactions either complete fully or not at all
# MAGIC
# MAGIC ```python
# MAGIC # Either ALL rows are written or NONE
# MAGIC df.write.format("delta").save("/data")
# MAGIC ```
# MAGIC
# MAGIC ✅ **Success**: All 1M rows written
# MAGIC ❌ **Failure**: Zero rows written (automatic rollback)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **C - Consistency**
# MAGIC **Data integrity** - Data always valid, constraints enforced
# MAGIC
# MAGIC ```python
# MAGIC # Schema enforcement - rejects invalid data
# MAGIC df.write.format("delta").save("/data")
# MAGIC ```
# MAGIC
# MAGIC ✅ Schema matches: Write succeeds
# MAGIC ❌ Schema mismatch: Write fails (data stays consistent)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **I - Isolation**
# MAGIC **Concurrent operations** - Transactions don't interfere
# MAGIC
# MAGIC ```
# MAGIC Reader 1: SELECT * FROM table  (reads v10)
# MAGIC Writer:   INSERT INTO table    (creates v11)
# MAGIC Reader 2: SELECT * FROM table  (reads v10)
# MAGIC ```
# MAGIC
# MAGIC ✅ Readers see consistent snapshot
# MAGIC ✅ Writers don't block readers
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **D - Durability**
# MAGIC **Persistence** - Committed data survives failures
# MAGIC
# MAGIC ```python
# MAGIC df.write.format("delta").save("/data")
# MAGIC # ✅ Committed - data is durable
# MAGIC # Even if cluster crashes, data is safe
# MAGIC ```
# MAGIC
# MAGIC ✅ Transaction log ensures durability

# COMMAND ----------

# DBTITLE 1,Demo - ACID in Action
# 🔷 Demo: Delta Lake ACID Properties

from pyspark.sql.functions import col

print("🔷 Delta Lake ACID Demo")
print("=" * 60)

# Create sample data
data = [(1, "Alice", 85), (2, "Bob", 92), (3, "Charlie", 78)]
df = spark.createDataFrame(data, ["id", "name", "score"])

# 1. ATOMICITY - Write completes fully or not at all
print("\n1️⃣ ATOMICITY: All-or-nothing writes")
df.write.format("delta").mode("overwrite").save("/tmp/delta/students")
print("   ✅ All 3 rows written atomically")
print("   ✅ If cluster crashed mid-write, 0 rows (not partial)")

# 2. CONSISTENCY - Schema enforcement
print("\n2️⃣ CONSISTENCY: Schema enforcement")
try:
    bad_data = [(4, "Invalid")]  # Missing 'score' column
    bad_df = spark.createDataFrame(bad_data, ["id", "name"])
    bad_df.write.format("delta").mode("append").save("/tmp/delta/students")
except Exception as e:
    print("   ✅ Schema mismatch rejected! Data stays consistent.")
    print(f"   Error: {str(e)[:50]}...")

# 3. ISOLATION - Concurrent reads work
print("\n3️⃣ ISOLATION: Concurrent operations")
df_read = spark.read.format("delta").load("/tmp/delta/students")
print(f"   ✅ Reader sees consistent snapshot: {df_read.count()} rows")
print("   ✅ Readers don't block writers (and vice versa)")

# 4. DURABILITY - Data persists
print("\n4️⃣ DURABILITY: Committed data survives")
print("   ✅ Transaction log ensures durability")
print("   ✅ Even after cluster restart, data is safe")

print("\n🎯 Delta Lake guarantees ACID properties!")
print("   This is what makes Delta Lake production-ready.")

# COMMAND ----------

# DBTITLE 1,Delta Lake Architecture
# MAGIC %md
# MAGIC ## 🏗️ Delta Lake Architecture
# MAGIC
# MAGIC ### **File Structure**
# MAGIC ```
# MAGIC /delta_table/
# MAGIC ├── _delta_log/           # Transaction log (metadata)
# MAGIC │   ├── 00000000000.json  # Version 0
# MAGIC │   ├── 00000001.json     # Version 1
# MAGIC │   └── 00000002.json     # Version 2
# MAGIC ├── part-001.parquet       # Data files
# MAGIC ├── part-002.parquet
# MAGIC └── part-003.parquet
# MAGIC ```
# MAGIC
# MAGIC ### **Transaction Log**
# MAGIC The **"secret sauce"** of Delta Lake!
# MAGIC
# MAGIC * **JSON files** tracking every change
# MAGIC * **Atomic operations** via log entries
# MAGIC * **Version history** for time travel
# MAGIC * **Metadata** (schema, partitions, stats)
# MAGIC
# MAGIC ### **How Writes Work**
# MAGIC ```
# MAGIC 1. Write data files (Parquet)
# MAGIC 2. Write transaction log entry (JSON)
# MAGIC 3. Commit becomes visible atomically
# MAGIC ```
# MAGIC
# MAGIC ### **How Reads Work**
# MAGIC ```
# MAGIC 1. Read transaction log
# MAGIC 2. Identify active data files
# MAGIC 3. Read only necessary files
# MAGIC 4. Return consistent snapshot
# MAGIC ```
# MAGIC
# MAGIC 💡 **The log is the source of truth, not the files!**

# COMMAND ----------

# DBTITLE 1,Creating Delta Tables
# 📝 Demo: Creating Delta Tables

print("📝 Creating Delta Tables")
print("=" * 60)

# Method 1: Write DataFrame as Delta
print("\n1️⃣ Method 1: DataFrame.write")
data = [(1, "Product A", 19.99), (2, "Product B", 29.99)]
df = spark.createDataFrame(data, ["id", "name", "price"])

df.write.format("delta").mode("overwrite").save("/tmp/delta/products")
print("   ✅ Created Delta table at /tmp/delta/products")

# Method 2: SQL CREATE TABLE
print("\n2️⃣ Method 2: SQL CREATE TABLE")
spark.sql("""
    CREATE OR REPLACE TABLE demo_customers (
        customer_id INT,
        name STRING,
        email STRING
    ) USING DELTA
""")
print("   ✅ Created managed Delta table 'demo_customers'")

# Method 3: SaveAsTable
print("\n3️⃣ Method 3: saveAsTable")
df.write.format("delta").mode("overwrite").saveAsTable("demo_products")
print("   ✅ Created managed table 'demo_products'")

# Verify
print("\n📊 Tables created:")
spark.sql("SHOW TABLES").filter("tableName LIKE 'demo%'").show()

print("\n💡 All three methods create ACID-compliant Delta tables!")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Delta Lake** = ACID transactions on data lakes
# MAGIC ✅ **ACID**: Atomicity, Consistency, Isolation, Durability
# MAGIC ✅ **Transaction Log** = Core architecture component
# MAGIC ✅ **Parquet + Metadata** = Fast and reliable
# MAGIC
# MAGIC ## 🎯 Key Benefits
# MAGIC * **Reliability**: ACID guarantees
# MAGIC * **Performance**: Optimized reads/writes
# MAGIC * **Simplicity**: Standard Spark API
# MAGIC * **Compatibility**: Works with Parquet ecosystem
# MAGIC
# MAGIC ## 💡 When to Use Delta Lake
# MAGIC ✅ **Production data pipelines**
# MAGIC ✅ **Data warehouses on lakes**
# MAGIC ✅ **Real-time + batch workloads**
# MAGIC ✅ **When you need reliability**
# MAGIC
# MAGIC ❌ **Don't use** for:
# MAGIC * Temporary scratch data
# MAGIC * Write-once, never-update datasets (plain Parquet is fine)
# MAGIC
# MAGIC ## ➡️ Next: Module 3.2 - Time Travel & Versioning
# MAGIC
# MAGIC 🎉 **Module 3.1 Complete!**
