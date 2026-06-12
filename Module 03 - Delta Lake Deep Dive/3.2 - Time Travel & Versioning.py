# Databricks notebook source
# DBTITLE 1,Module 3.2: Time Travel
# MAGIC %md
# MAGIC # ⏰ Module 3.2: Time Travel & Versioning
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand Delta Lake versioning
# MAGIC * Master time travel queries
# MAGIC * Learn to recover from mistakes
# MAGIC * Use version history for auditing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⏰ What is Time Travel?
# MAGIC
# MAGIC **Time Travel** lets you query **previous versions** of your Delta table.
# MAGIC
# MAGIC ### **Every Write Creates a Version**
# MAGIC ```
# MAGIC Version 0: Initial data (100 rows)
# MAGIC Version 1: INSERT (150 rows)
# MAGIC Version 2: UPDATE (150 rows, scores changed)
# MAGIC Version 3: DELETE (120 rows)
# MAGIC ```
# MAGIC
# MAGIC ### **Why Time Travel?**
# MAGIC ✅ **Audit data changes** - See what changed when
# MAGIC ✅ **Rollback mistakes** - Undo bad updates/deletes
# MAGIC ✅ **Reproduce results** - Use exact version for ML
# MAGIC ✅ **Debug pipelines** - Compare versions to find issues
# MAGIC ✅ **Compliance** - Regulatory requirements
# MAGIC
# MAGIC ### **How It Works**
# MAGIC * Transaction log stores all versions
# MAGIC * Data files are immutable (never modified)
# MAGIC * Metadata tracks which files belong to which version
# MAGIC * Read any version by referencing version number or timestamp

# COMMAND ----------

# DBTITLE 1,Time Travel Syntax
# MAGIC %md
# MAGIC ## 📝 Time Travel Syntax
# MAGIC
# MAGIC ### **Method 1: Version Number**
# MAGIC ```python
# MAGIC # Read specific version
# MAGIC df = spark.read.format("delta") \
# MAGIC     .option("versionAsOf", 2) \
# MAGIC     .load("/path/to/table")
# MAGIC
# MAGIC # SQL syntax
# MAGIC spark.sql("SELECT * FROM table VERSION AS OF 2")
# MAGIC ```
# MAGIC
# MAGIC ### **Method 2: Timestamp**
# MAGIC ```python
# MAGIC # Read data as of specific time
# MAGIC df = spark.read.format("delta") \
# MAGIC     .option("timestampAsOf", "2024-01-15") \
# MAGIC     .load("/path/to/table")
# MAGIC
# MAGIC # SQL syntax
# MAGIC spark.sql("SELECT * FROM table TIMESTAMP AS OF '2024-01-15'")
# MAGIC ```
# MAGIC
# MAGIC ### **Method 3: @ Syntax (Python)**
# MAGIC ```python
# MAGIC # Version
# MAGIC df = spark.read.format("delta").load("/path@v2")
# MAGIC
# MAGIC # Timestamp
# MAGIC df = spark.read.format("delta").load("/path@20240115")
# MAGIC ```
# MAGIC
# MAGIC ## 📊 View History
# MAGIC ```sql
# MAGIC DESCRIBE HISTORY table_name
# MAGIC ```
# MAGIC Shows all versions with timestamps and operations

# COMMAND ----------

# DBTITLE 1,Demo - Time Travel
# ⏰ Demo: Time Travel in Action

from pyspark.sql.functions import col
import time

print("⏰ Delta Lake Time Travel Demo")
print("=" * 60)

# Version 0: Create initial table
print("\n💾 Version 0: Creating initial data...")
data_v0 = [(1, "Alice", 85), (2, "Bob", 92), (3, "Charlie", 78)]
df_v0 = spark.createDataFrame(data_v0, ["id", "name", "score"])
df_v0.write.format("delta").mode("overwrite").save("/tmp/delta/timetravel")
print(f"   ✅ Created with {df_v0.count()} rows")
time.sleep(1)

# Version 1: Add more data
print("\n💾 Version 1: Adding new records...")
data_v1 = [(4, "Diana", 88), (5, "Eve", 95)]
df_v1 = spark.createDataFrame(data_v1, ["id", "name", "score"])
df_v1.write.format("delta").mode("append").save("/tmp/delta/timetravel")
print("   ✅ Added 2 more rows")
time.sleep(1)

# Version 2: Update scores
print("\n💾 Version 2: Updating scores...")
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "/tmp/delta/timetravel")
delta_table.update(
    condition = col("id") == 1,
    set = {"score": 90}  # Alice's score 85 -> 90
)
print("   ✅ Updated Alice's score to 90")

# Now let's time travel!
print("\n" + "=" * 60)
print("⏰ TIME TRAVEL QUERIES")
print("=" * 60)

# Read Version 0 (original)
print("\n🔙 Reading Version 0 (original data):")
df_version_0 = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta/timetravel")
print(f"   Rows: {df_version_0.count()}")
print("   Alice's score:", df_version_0.filter(col("id") == 1).select("score").collect()[0][0])

# Read Version 1 (after insert)
print("\n🔙 Reading Version 1 (after insert):")
df_version_1 = spark.read.format("delta").option("versionAsOf", 1).load("/tmp/delta/timetravel")
print(f"   Rows: {df_version_1.count()}")

# Read Version 2 (current, after update)
print("\n🔙 Reading Version 2 (current, after update):")
df_version_2 = spark.read.format("delta").load("/tmp/delta/timetravel")
print(f"   Rows: {df_version_2.count()}")
print("   Alice's score:", df_version_2.filter(col("id") == 1).select("score").collect()[0][0])

print("\n🎯 Time Travel lets you query ANY previous version!")

# COMMAND ----------

# DBTITLE 1,View History
# 📊 Demo: View Table History

print("📊 Delta Table History")
print("=" * 60)

# Get history using DataFrame API
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "/tmp/delta/timetravel")
history_df = delta_table.history()

print("\n📋 Version History:")
history_df.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

print("\n💡 Each operation creates a new version!")
print("   - Version 0: WRITE (initial data)")
print("   - Version 1: WRITE (append)")
print("   - Version 2: UPDATE")

print("\n➡️ You can query any version using versionAsOf!")

# COMMAND ----------

# DBTITLE 1,Rollback & Restore
# MAGIC %md
# MAGIC ## 🔙 Rollback & Restore
# MAGIC
# MAGIC ### **Restore to Previous Version**
# MAGIC ```sql
# MAGIC RESTORE TABLE table_name TO VERSION AS OF 5
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC delta_table = DeltaTable.forPath(spark, "/path")
# MAGIC delta_table.restoreToVersion(5)
# MAGIC ```
# MAGIC
# MAGIC ### **Restore to Timestamp**
# MAGIC ```sql
# MAGIC RESTORE TABLE table_name TO TIMESTAMP AS OF '2024-01-15'
# MAGIC ```
# MAGIC
# MAGIC ### **Common Use Cases**
# MAGIC
# MAGIC 1. **Undo Bad Update**
# MAGIC ```sql
# MAGIC -- Oh no! Updated wrong records
# MAGIC RESTORE TABLE users TO VERSION AS OF 10
# MAGIC ```
# MAGIC
# MAGIC 2. **Recover Deleted Data**
# MAGIC ```sql
# MAGIC -- Accidentally deleted data
# MAGIC RESTORE TABLE orders TO TIMESTAMP AS OF '2024-01-15 14:00:00'
# MAGIC ```
# MAGIC
# MAGIC 3. **Reproduce ML Model**
# MAGIC ```python
# MAGIC # Use exact version for training
# MAGIC df = spark.read.format("delta") \
# MAGIC     .option("versionAsOf", 42) \
# MAGIC     .load("/ml/training_data")
# MAGIC ```
# MAGIC
# MAGIC 💡 **Restore creates a new version** - it doesn't actually delete history!

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Time Travel** = Query previous versions of Delta tables
# MAGIC ✅ **Every write** creates a new version
# MAGIC ✅ **Two methods**: Version number or timestamp
# MAGIC ✅ **RESTORE** = Rollback to previous state
# MAGIC
# MAGIC ## 🎯 Key Commands
# MAGIC ```sql
# MAGIC -- Read old version
# MAGIC SELECT * FROM table VERSION AS OF 5
# MAGIC SELECT * FROM table TIMESTAMP AS OF '2024-01-15'
# MAGIC
# MAGIC -- View history
# MAGIC DESCRIBE HISTORY table
# MAGIC
# MAGIC -- Restore
# MAGIC RESTORE TABLE table TO VERSION AS OF 5
# MAGIC ```
# MAGIC
# MAGIC ## 💡 Use Cases
# MAGIC * **Audit**: Track all changes over time
# MAGIC * **Rollback**: Undo mistakes
# MAGIC * **Reproduce**: Use exact data version for ML
# MAGIC * **Compare**: Debug by comparing versions
# MAGIC * **Compliance**: Meet regulatory requirements
# MAGIC
# MAGIC ## ➡️ Next: Module 3.3 - MERGE, UPDATE, DELETE & Optimization
# MAGIC
# MAGIC 🎉 **Module 3.2 Complete!**
