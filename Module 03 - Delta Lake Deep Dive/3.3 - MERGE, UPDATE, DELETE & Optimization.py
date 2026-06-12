# Databricks notebook source
# DBTITLE 1,Module 3.3: MERGE, UPDATE, DELETE
# MAGIC %md
# MAGIC # 🔄 Module 3.3: MERGE, UPDATE, DELETE & Optimization
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Master UPDATE and DELETE operations
# MAGIC * Learn MERGE for upserts (CDC)
# MAGIC * Understand optimization techniques
# MAGIC * Build high-performance Delta tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Why This Matters
# MAGIC
# MAGIC **Traditional data lakes** (Parquet/CSV):
# MAGIC * ❌ Can't update records
# MAGIC * ❌ Can't delete records
# MAGIC * ❌ Rewrite entire dataset for changes
# MAGIC
# MAGIC **Delta Lake**:
# MAGIC * ✅ UPDATE specific rows
# MAGIC * ✅ DELETE specific rows
# MAGIC * ✅ MERGE (upsert) for CDC
# MAGIC * ✅ Efficient - only modifies affected files
# MAGIC
# MAGIC ### **Common Use Cases**
# MAGIC * **CDC (Change Data Capture)** - Apply database changes
# MAGIC * **GDPR compliance** - Delete user data
# MAGIC * **Data corrections** - Fix errors
# MAGIC * **Dimension tables** - Type 2 slowly changing dimensions

# COMMAND ----------

# DBTITLE 1,UPDATE Operations
# MAGIC %md
# MAGIC ## 🔄 UPDATE Operations
# MAGIC
# MAGIC ### **SQL Syntax**
# MAGIC ```sql
# MAGIC UPDATE table_name
# MAGIC SET column1 = value1, column2 = value2
# MAGIC WHERE condition
# MAGIC ```
# MAGIC
# MAGIC ### **Python Syntax**
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC delta_table = DeltaTable.forPath(spark, "/path")
# MAGIC
# MAGIC delta_table.update(
# MAGIC     condition = col("status") == "pending",
# MAGIC     set = {"status": "'completed'", "updated_at": "current_timestamp()"}
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### **Examples**
# MAGIC ```sql
# MAGIC -- Update single column
# MAGIC UPDATE customers SET status = 'active' WHERE status = 'pending'
# MAGIC
# MAGIC -- Update multiple columns
# MAGIC UPDATE orders 
# MAGIC SET status = 'shipped', shipped_date = current_date()
# MAGIC WHERE order_id IN (1001, 1002, 1003)
# MAGIC
# MAGIC -- Conditional update
# MAGIC UPDATE products
# MAGIC SET price = price * 1.1  -- 10% increase
# MAGIC WHERE category = 'electronics'
# MAGIC ```
# MAGIC
# MAGIC 💡 **Only affected files are rewritten** - not the entire table!

# COMMAND ----------

# DBTITLE 1,DELETE Operations
# MAGIC %md
# MAGIC ## 🗑️ DELETE Operations
# MAGIC
# MAGIC ### **SQL Syntax**
# MAGIC ```sql
# MAGIC DELETE FROM table_name WHERE condition
# MAGIC ```
# MAGIC
# MAGIC ### **Python Syntax**
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC delta_table = DeltaTable.forPath(spark, "/path")
# MAGIC delta_table.delete(condition = "status = 'cancelled'")
# MAGIC ```
# MAGIC
# MAGIC ### **Examples**
# MAGIC ```sql
# MAGIC -- Delete by condition
# MAGIC DELETE FROM orders WHERE status = 'cancelled'
# MAGIC
# MAGIC -- Delete old data (retention)
# MAGIC DELETE FROM logs WHERE timestamp < '2023-01-01'
# MAGIC
# MAGIC -- GDPR: Delete user data
# MAGIC DELETE FROM users WHERE user_id = 12345
# MAGIC
# MAGIC -- Delete duplicates (keep latest)
# MAGIC DELETE FROM events 
# MAGIC WHERE event_id NOT IN (
# MAGIC     SELECT MAX(event_id) FROM events GROUP BY user_id, event_type
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ⚠️ **Warning**: DELETE without WHERE deletes ALL rows!
# MAGIC
# MAGIC 💡 **Files are marked for deletion** - use VACUUM to reclaim space

# COMMAND ----------

# DBTITLE 1,Demo - UPDATE & DELETE
# 🔄 Demo: UPDATE and DELETE Operations

from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

print("🔄 UPDATE & DELETE Demo")
print("=" * 60)

# Create sample data
data = [
    (1, "Alice", "active", 85),
    (2, "Bob", "pending", 92),
    (3, "Charlie", "inactive", 78),
    (4, "Diana", "pending", 88)
]
df = spark.createDataFrame(data, ["id", "name", "status", "score"])
df.write.format("delta").mode("overwrite").save("/tmp/delta/operations")

print("\n💾 Initial data:")
spark.read.format("delta").load("/tmp/delta/operations").show()

# UPDATE: Change pending to active
print("\n1️⃣ UPDATE: Changing 'pending' to 'active'")
delta_table = DeltaTable.forPath(spark, "/tmp/delta/operations")
delta_table.update(
    condition = col("status") == "pending",
    set = {"status": "'active'"}
)

print("   ✅ Updated!")
spark.read.format("delta").load("/tmp/delta/operations").show()

# DELETE: Remove inactive users
print("\n2️⃣ DELETE: Removing inactive users")
delta_table.delete(condition = col("status") == "inactive")

print("   ✅ Deleted!")
result = spark.read.format("delta").load("/tmp/delta/operations")
result.show()

print(f"\n📊 Final count: {result.count()} rows (was 4, deleted 1)")
print("\n🎯 UPDATE and DELETE are efficient - only affected files rewritten!")

# COMMAND ----------

# DBTITLE 1,MERGE Operations (Upsert)
# MAGIC %md
# MAGIC ## 🔀 MERGE Operations (Upsert)
# MAGIC
# MAGIC **MERGE** = Combine INSERT + UPDATE in one operation
# MAGIC
# MAGIC ### **Use Cases**
# MAGIC * **Change Data Capture (CDC)** - Apply database changes
# MAGIC * **Slowly Changing Dimensions (SCD)** - Update dimensions
# MAGIC * **Upsert pattern** - Insert if new, update if exists
# MAGIC
# MAGIC ### **SQL Syntax**
# MAGIC ```sql
# MAGIC MERGE INTO target_table
# MAGIC USING source_table
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC ### **Python Syntax**
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC delta_table = DeltaTable.forPath(spark, "/path/target")
# MAGIC
# MAGIC delta_table.alias("target").merge(
# MAGIC     source_df.alias("source"),
# MAGIC     "target.id = source.id"
# MAGIC ).whenMatchedUpdateAll() \
# MAGIC  .whenNotMatchedInsertAll() \
# MAGIC  .execute()
# MAGIC ```
# MAGIC
# MAGIC ### **Advanced MERGE**
# MAGIC ```python
# MAGIC # Conditional updates
# MAGIC delta_table.alias("t").merge(
# MAGIC     updates.alias("s"),
# MAGIC     "t.id = s.id"
# MAGIC ).whenMatchedUpdate(
# MAGIC     condition = "s.timestamp > t.timestamp",
# MAGIC     set = {"value": "s.value", "updated_at": "s.timestamp"}
# MAGIC ).whenNotMatchedInsertAll() \
# MAGIC  .execute()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo - MERGE (CDC)
# 🔀 Demo: MERGE for Change Data Capture

from delta.tables import DeltaTable

print("🔀 MERGE (Upsert) Demo - CDC Pattern")
print("=" * 60)

# Target table (existing data)
print("\n💾 Creating target table (existing customer data)...")
target_data = [
    (1, "Alice", "alice@old.com", 100),
    (2, "Bob", "bob@example.com", 200)
]
target_df = spark.createDataFrame(target_data, ["id", "name", "email", "balance"])
target_df.write.format("delta").mode("overwrite").save("/tmp/delta/customers")

print("Target (before):")
spark.read.format("delta").load("/tmp/delta/customers").show()

# Source data (CDC changes from database)
print("\n📥 CDC changes from source database:")
source_data = [
    (1, "Alice", "alice@new.com", 150),  # UPDATED: email and balance
    (3, "Charlie", "charlie@example.com", 300)  # NEW customer
]
source_df = spark.createDataFrame(source_data, ["id", "name", "email", "balance"])
print("Source (changes):")
source_df.show()

# MERGE: Apply changes
print("\n🔀 Executing MERGE...")
delta_table = DeltaTable.forPath(spark, "/tmp/delta/customers")

delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id"  # Match on customer ID
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("   ✅ MERGE complete!")

# Show results
print("\n🎯 Target (after MERGE):")
result = spark.read.format("delta").load("/tmp/delta/customers").orderBy("id")
result.show()

print("\nWhat happened:")
print("  ✅ ID 1 (Alice): UPDATED email and balance")
print("  ✅ ID 2 (Bob): No change (not in source)")
print("  ✅ ID 3 (Charlie): INSERTED as new customer")
print("\n💡 MERGE = One operation for INSERT + UPDATE!")

# COMMAND ----------

# DBTITLE 1,Optimization Techniques
# MAGIC %md
# MAGIC ## 🚀 Delta Lake Optimization
# MAGIC
# MAGIC ### **1. OPTIMIZE (Compaction)**
# MAGIC Combines small files into larger ones for better performance
# MAGIC
# MAGIC ```sql
# MAGIC OPTIMIZE table_name
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC delta_table = DeltaTable.forPath(spark, "/path")
# MAGIC delta_table.optimize().executeCompaction()
# MAGIC ```
# MAGIC
# MAGIC **When to run**: After many small writes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **2. Z-ORDER (Data Skipping)**
# MAGIC Co-locates related data for faster queries
# MAGIC
# MAGIC ```sql
# MAGIC OPTIMIZE table_name ZORDER BY (column1, column2)
# MAGIC ```
# MAGIC
# MAGIC **When to use**: Frequently filtered columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **3. VACUUM (Cleanup)**
# MAGIC Deletes old file versions to reclaim storage
# MAGIC
# MAGIC ```sql
# MAGIC VACUUM table_name RETAIN 168 HOURS  -- 7 days
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC delta_table.vacuum(168)  # 168 hours = 7 days
# MAGIC ```
# MAGIC
# MAGIC ⚠️ **Warning**: Can't time travel past VACUUM'd versions!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **4. Data Skipping**
# MAGIC Automatic! Delta collects stats (min/max) to skip files
# MAGIC
# MAGIC ```sql
# MAGIC -- Delta automatically skips files that don't match
# MAGIC SELECT * FROM table WHERE date = '2024-01-15'
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **UPDATE**: Modify specific rows efficiently
# MAGIC ✅ **DELETE**: Remove rows (VACUUM to reclaim space)
# MAGIC ✅ **MERGE**: Upsert pattern for CDC
# MAGIC ✅ **OPTIMIZE**: Compact small files
# MAGIC ✅ **Z-ORDER**: Improve query performance
# MAGIC ✅ **VACUUM**: Clean up old versions
# MAGIC
# MAGIC ## 🎯 Key Commands
# MAGIC ```sql
# MAGIC -- DML Operations
# MAGIC UPDATE table SET col = val WHERE condition
# MAGIC DELETE FROM table WHERE condition
# MAGIC MERGE INTO target USING source ON key ...
# MAGIC
# MAGIC -- Optimization
# MAGIC OPTIMIZE table
# MAGIC OPTIMIZE table ZORDER BY (columns)
# MAGIC VACUUM table RETAIN 168 HOURS
# MAGIC ```
# MAGIC
# MAGIC ## 💡 Best Practices
# MAGIC * Run OPTIMIZE after many small writes
# MAGIC * Use Z-ORDER on frequently filtered columns
# MAGIC * VACUUM regularly (but keep 7+ days for time travel)
# MAGIC * Use MERGE for CDC/upsert patterns
# MAGIC * Monitor with `DESCRIBE HISTORY`
# MAGIC
# MAGIC ## 🎓 Module 03 Complete!
# MAGIC
# MAGIC You've mastered:
# MAGIC * Delta Lake fundamentals & ACID
# MAGIC * Time travel & versioning
# MAGIC * UPDATE, DELETE, MERGE operations
# MAGIC * Optimization techniques
# MAGIC
# MAGIC ## ➡️ Next: Module 04 - Unity Catalog & Governance
# MAGIC
# MAGIC 🎉 **Congratulations on completing Module 03!**
