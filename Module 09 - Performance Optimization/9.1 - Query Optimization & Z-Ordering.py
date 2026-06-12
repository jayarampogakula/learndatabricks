# Databricks notebook source
# DBTITLE 1,Module 9.1: Performance Optimization
# MAGIC %md
# MAGIC # ⚡ Module 9.1: Query Optimization & Z-Ordering
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Optimize query performance
# MAGIC * Understand Z-Ordering
# MAGIC * Apply data skipping
# MAGIC * Use partitioning effectively
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Performance Optimization Overview
# MAGIC
# MAGIC **Performance optimization** in Databricks focuses on reducing query time and compute costs.
# MAGIC
# MAGIC ### **Key Optimization Areas**
# MAGIC
# MAGIC ✅ **Query Optimization** - Efficient SQL and Spark code
# MAGIC ✅ **Data Layout** - Z-Ordering, partitioning
# MAGIC ✅ **Caching** - Intelligent data caching
# MAGIC ✅ **Compute** - Right-sizing clusters
# MAGIC ✅ **File Format** - Delta Lake optimizations
# MAGIC
# MAGIC ### **Why Optimize?**
# MAGIC * **Faster queries** - Better user experience
# MAGIC * **Lower costs** - Less compute time
# MAGIC * **Better scalability** - Handle more data
# MAGIC * **Resource efficiency** - More work per dollar
# MAGIC
# MAGIC ### **Performance Hierarchy**
# MAGIC ```
# MAGIC 1. Data Layout (biggest impact)
# MAGIC 2. Query Design
# MAGIC 3. Cluster Configuration
# MAGIC 4. Caching Strategy
# MAGIC 5. Code Optimization
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Query Optimization Techniques
# MAGIC %md
# MAGIC ## 🎯 Query Optimization Techniques
# MAGIC
# MAGIC ### **1. Predicate Pushdown**
# MAGIC Filter early, reduce data scanned
# MAGIC
# MAGIC ```python
# MAGIC # ❌ Bad: Filter after reading all data
# MAGIC df = spark.table("sales")
# MAGIC df_filtered = df.filter("date >= '2024-01-01'")
# MAGIC
# MAGIC # ✅ Good: Filter during read (predicate pushdown)
# MAGIC df = spark.table("sales").filter("date >= '2024-01-01'")
# MAGIC ```
# MAGIC
# MAGIC ### **2. Projection Pushdown**
# MAGIC Select only needed columns
# MAGIC
# MAGIC ```python
# MAGIC # ❌ Bad: Read all columns
# MAGIC df = spark.table("sales").filter(...)
# MAGIC result = df.select("customer_id", "amount")
# MAGIC
# MAGIC # ✅ Good: Select early
# MAGIC df = spark.table("sales") \
# MAGIC     .select("customer_id", "amount", "date") \
# MAGIC     .filter(...)
# MAGIC ```
# MAGIC
# MAGIC ### **3. Broadcast Joins**
# MAGIC For small dimension tables
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import broadcast
# MAGIC
# MAGIC # ✅ Broadcast small table
# MAGIC large_df.join(
# MAGIC     broadcast(small_df),
# MAGIC     "customer_id"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### **4. Avoid Shuffles**
# MAGIC Minimize data movement
# MAGIC
# MAGIC ```python
# MAGIC # ❌ Causes shuffle
# MAGIC df.repartition(100)
# MAGIC
# MAGIC # ✅ Coalesce (no shuffle for reducing partitions)
# MAGIC df.coalesce(10)
# MAGIC ```
# MAGIC
# MAGIC ### **5. Use Delta Cache**
# MAGIC ```python
# MAGIC # Cache frequently accessed data
# MAGIC df.cache()
# MAGIC # Or in SQL
# MAGIC spark.sql("CACHE SELECT * FROM sales WHERE date >= '2024-01-01'")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Z-Ordering Concept
# MAGIC %md
# MAGIC ## 🔤 Z-Ordering (ZOrder)
# MAGIC
# MAGIC **Z-Ordering** co-locates related data in the same files to improve data skipping.
# MAGIC
# MAGIC ### **How It Works**
# MAGIC
# MAGIC ```
# MAGIC Without Z-Order:
# MAGIC File 1: customer_ids: 1, 500, 1000, 1500
# MAGIC File 2: customer_ids: 2, 501, 1001, 1501
# MAGIC File 3: customer_ids: 3, 502, 1002, 1502
# MAGIC
# MAGIC With Z-Order on customer_id:
# MAGIC File 1: customer_ids: 1, 2, 3, 4, 5...
# MAGIC File 2: customer_ids: 500, 501, 502, 503...
# MAGIC File 3: customer_ids: 1000, 1001, 1002...
# MAGIC ```
# MAGIC
# MAGIC ### **Benefits**
# MAGIC ✅ **Data Skipping** - Skip irrelevant files
# MAGIC ✅ **Faster Queries** - Read less data
# MAGIC ✅ **Lower Costs** - Less data scanned
# MAGIC ✅ **No Overhead** - Query time improvement only
# MAGIC
# MAGIC ### **When to Use Z-Order**
# MAGIC * High-cardinality columns (customer_id, user_id)
# MAGIC * Common filter columns
# MAGIC * Join keys
# MAGIC * Columns with selective queries
# MAGIC
# MAGIC ### **When NOT to Use Z-Order**
# MAGIC * Low-cardinality columns (use partitioning)
# MAGIC * Columns rarely used in filters
# MAGIC * Very small tables
# MAGIC
# MAGIC ### **Z-Order vs Partitioning**
# MAGIC
# MAGIC | Feature | Partitioning | Z-Ordering |
# MAGIC |---------|-------------|------------|
# MAGIC | **Cardinality** | Low (10-1000) | High (>1000) |
# MAGIC | **Files** | Separate dirs | Same dir |
# MAGIC | **Columns** | Few (1-3) | Many (up to 32) |
# MAGIC | **Overhead** | Can create small files | No overhead |
# MAGIC | **Example** | date, country | customer_id, product_id |

# COMMAND ----------

# DBTITLE 1,Demo - Z-Ordering
# ⚡ Demo: Z-Ordering Performance

from pyspark.sql.functions import col, rand
import time

print("⚡ Z-Ordering Demo")
print("=" * 60)

# Create sample table
print("\n📊 Creating sample table...")
df = spark.range(0, 10000000).select(
    col("id").alias("customer_id"),
    (rand() * 1000).cast("int").alias("product_id"),
    (rand() * 10000).alias("amount")
)

# Write without Z-Order
table_name = "demo_sales"
df.write.format("delta").mode("overwrite").saveAsTable(table_name)
print(f"✅ Created table: {table_name}")

# Query before Z-Order
print("\n🔍 Query BEFORE Z-Order:")
start = time.time()
result = spark.sql(f"""
    SELECT COUNT(*) as count
    FROM {table_name}
    WHERE customer_id BETWEEN 1000 AND 2000
""").collect()
time_before = time.time() - start
print(f"   Query time: {time_before:.2f} seconds")
print(f"   Result: {result[0]['count']} rows")

# Apply Z-Order
print("\n🔤 Applying Z-Order on customer_id...")
spark.sql(f"OPTIMIZE {table_name} ZORDER BY (customer_id)")
print("✅ Z-Order complete")

# Query after Z-Order
print("\n🔍 Query AFTER Z-Order:")
start = time.time()
result = spark.sql(f"""
    SELECT COUNT(*) as count
    FROM {table_name}
    WHERE customer_id BETWEEN 1000 AND 2000
""").collect()
time_after = time.time() - start
print(f"   Query time: {time_after:.2f} seconds")
print(f"   Result: {result[0]['count']} rows")

# Compare
if time_before > time_after:
    improvement = ((time_before - time_after) / time_before) * 100
    print(f"\n✅ Performance improvement: {improvement:.1f}% faster")
else:
    print(f"\n💡 Small dataset - benefits show on larger tables")

print("\n💡 Z-Order benefits:")
print("   • Data skipping - Read only relevant files")
print("   • No query changes needed")
print("   • Works with any query pattern")

# COMMAND ----------

# DBTITLE 1,Partitioning Strategy
# MAGIC %md
# MAGIC ## 📁 Partitioning Strategy
# MAGIC
# MAGIC ### **What is Partitioning?**
# MAGIC Organize data into separate directories by column values
# MAGIC
# MAGIC ```
# MAGIC table/
# MAGIC ├── date=2024-01-01/
# MAGIC │   ├── file1.parquet
# MAGIC │   └── file2.parquet
# MAGIC ├── date=2024-01-02/
# MAGIC │   ├── file1.parquet
# MAGIC │   └── file2.parquet
# MAGIC ```
# MAGIC
# MAGIC ### **When to Partition**
# MAGIC ✅ **Low-cardinality** columns (10-1000 values)
# MAGIC ✅ **Common filters** - Frequently used in WHERE
# MAGIC ✅ **Time series** - Date/timestamp columns
# MAGIC ✅ **Geographic** - Country, region
# MAGIC
# MAGIC ### **Creating Partitioned Tables**
# MAGIC ```python
# MAGIC # Write partitioned table
# MAGIC df.write.format("delta") \
# MAGIC     .partitionBy("date", "country") \
# MAGIC     .saveAsTable("sales")
# MAGIC ```
# MAGIC
# MAGIC ### **Best Practices**
# MAGIC
# MAGIC ✅ **Partition on common filters**
# MAGIC ```sql
# MAGIC -- This query benefits from date partitioning
# MAGIC SELECT * FROM sales WHERE date = '2024-01-15'
# MAGIC ```
# MAGIC
# MAGIC ✅ **Avoid over-partitioning**
# MAGIC * Target: 1GB+ per partition
# MAGIC * Avoid: Millions of small files
# MAGIC
# MAGIC ✅ **Combine with Z-Order**
# MAGIC ```sql
# MAGIC -- Partition by date, Z-Order by customer_id
# MAGIC CREATE TABLE sales (
# MAGIC     customer_id BIGINT,
# MAGIC     amount DECIMAL,
# MAGIC     date DATE
# MAGIC )
# MAGIC PARTITIONED BY (date);
# MAGIC
# MAGIC OPTIMIZE sales ZORDER BY (customer_id);
# MAGIC ```
# MAGIC
# MAGIC ❌ **Don't partition on high-cardinality**
# MAGIC ```python
# MAGIC # ❌ Bad: Creates millions of partitions
# MAGIC partitionBy("customer_id")  # Use Z-Order instead
# MAGIC
# MAGIC # ✅ Good: Low cardinality
# MAGIC partitionBy("date")  # ~365 partitions per year
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Additional Optimizations
# MAGIC %md
# MAGIC ## 🚀 Additional Optimizations
# MAGIC
# MAGIC ### **1. File Compaction**
# MAGIC Combine small files for better performance
# MAGIC
# MAGIC ```sql
# MAGIC -- Compact files
# MAGIC OPTIMIZE sales;
# MAGIC
# MAGIC -- Compact and Z-Order
# MAGIC OPTIMIZE sales ZORDER BY (customer_id, product_id);
# MAGIC ```
# MAGIC
# MAGIC ### **2. Auto-Optimize**
# MAGIC Automatic file compaction and optimization
# MAGIC
# MAGIC ```sql
# MAGIC -- Enable for table
# MAGIC ALTER TABLE sales 
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### **3. Data Skipping**
# MAGIC Automatic with Delta Lake
# MAGIC
# MAGIC ```python
# MAGIC # Delta automatically collects statistics
# MAGIC # and skips files that don't match filters
# MAGIC df.filter("amount > 1000")  # Skips files with max < 1000
# MAGIC ```
# MAGIC
# MAGIC ### **4. Bloom Filters**
# MAGIC For high-cardinality columns
# MAGIC
# MAGIC ```sql
# MAGIC -- Create bloom filter index
# MAGIC CREATE BLOOMFILTER INDEX ON TABLE sales 
# MAGIC FOR COLUMNS (customer_id OPTIONS (fpp=0.1, numItems=10000000));
# MAGIC ```
# MAGIC
# MAGIC ### **5. Liquid Clustering**
# MAGIC Automatic data layout optimization (newer feature)
# MAGIC
# MAGIC ```sql
# MAGIC -- Create table with liquid clustering
# MAGIC CREATE TABLE sales (
# MAGIC     customer_id BIGINT,
# MAGIC     product_id BIGINT,
# MAGIC     amount DECIMAL
# MAGIC )
# MAGIC CLUSTER BY (customer_id, product_id);
# MAGIC ```
# MAGIC
# MAGIC ### **6. Analyze Table**
# MAGIC Update statistics for query optimizer
# MAGIC
# MAGIC ```sql
# MAGIC -- Compute statistics
# MAGIC ANALYZE TABLE sales COMPUTE STATISTICS;
# MAGIC
# MAGIC -- Compute column statistics
# MAGIC ANALYZE TABLE sales COMPUTE STATISTICS FOR COLUMNS customer_id, amount;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Best Practices Summary
# MAGIC %md
# MAGIC ## 🎯 Performance Best Practices
# MAGIC
# MAGIC ### **Query Design**
# MAGIC ✅ Filter early (predicate pushdown)
# MAGIC ✅ Select only needed columns
# MAGIC ✅ Use broadcast joins for small tables
# MAGIC ✅ Avoid unnecessary shuffles
# MAGIC ✅ Cache frequently used data
# MAGIC
# MAGIC ### **Data Layout**
# MAGIC ✅ **Partition** low-cardinality columns (date, country)
# MAGIC ✅ **Z-Order** high-cardinality columns (customer_id)
# MAGIC ✅ Run **OPTIMIZE** regularly
# MAGIC ✅ Enable **Auto-Optimize** for production
# MAGIC ✅ Keep file sizes 100MB-1GB
# MAGIC
# MAGIC ### **Table Maintenance**
# MAGIC ✅ Run OPTIMIZE weekly
# MAGIC ✅ Apply Z-Order on filter columns
# MAGIC ✅ Vacuum old files (after 7+ days)
# MAGIC ✅ Update table statistics
# MAGIC ✅ Monitor query performance
# MAGIC
# MAGIC ### **Common Patterns**
# MAGIC
# MAGIC **Time-Series Data:**
# MAGIC ```sql
# MAGIC -- Partition by date, Z-Order by entity
# MAGIC PARTITIONED BY (date)
# MAGIC OPTIMIZE sales ZORDER BY (customer_id);
# MAGIC ```
# MAGIC
# MAGIC **High-Volume Logs:**
# MAGIC ```sql
# MAGIC -- Partition by hour, Z-Order by trace_id
# MAGIC PARTITIONED BY (date, hour)
# MAGIC OPTIMIZE logs ZORDER BY (trace_id, session_id);
# MAGIC ```
# MAGIC
# MAGIC **Fact Tables:**
# MAGIC ```sql
# MAGIC -- No partition, Z-Order on dimension keys
# MAGIC OPTIMIZE orders ZORDER BY (customer_id, product_id);
# MAGIC ```
# MAGIC
# MAGIC ### **Monitoring**
# MAGIC ```sql
# MAGIC -- Check table details
# MAGIC DESCRIBE DETAIL sales;
# MAGIC
# MAGIC -- View file statistics
# MAGIC DESCRIBE HISTORY sales;
# MAGIC
# MAGIC -- Analyze query plans
# MAGIC EXPLAIN SELECT ... FROM sales WHERE ...;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Query Optimization** - Filter early, select specific columns
# MAGIC ✅ **Z-Ordering** - Co-locate related data for better skipping
# MAGIC ✅ **Partitioning** - Organize by low-cardinality columns
# MAGIC ✅ **OPTIMIZE** - Compact files regularly
# MAGIC ✅ **Auto-Optimize** - Automatic maintenance
# MAGIC ✅ **Data Skipping** - Automatic with Delta Lake
# MAGIC
# MAGIC ## 🎯 Key Concepts
# MAGIC
# MAGIC ### **Z-Order vs Partition**
# MAGIC * **Partition**: Low cardinality (date, country)
# MAGIC * **Z-Order**: High cardinality (customer_id, product_id)
# MAGIC * **Use both**: Partition by date, Z-Order by ID
# MAGIC
# MAGIC ### **Optimization Commands**
# MAGIC ```sql
# MAGIC -- Basic optimization
# MAGIC OPTIMIZE table_name;
# MAGIC
# MAGIC -- With Z-Ordering
# MAGIC OPTIMIZE table_name ZORDER BY (col1, col2);
# MAGIC
# MAGIC -- Enable auto-optimize
# MAGIC ALTER TABLE table_name SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### **Performance Hierarchy**
# MAGIC 1. **Data Layout** - Biggest impact (partition + Z-Order)
# MAGIC 2. **Query Design** - Filter/project early
# MAGIC 3. **File Size** - Keep 100MB-1GB
# MAGIC 4. **Statistics** - Keep up to date
# MAGIC 5. **Caching** - For hot data
# MAGIC
# MAGIC ## 💡 Remember
# MAGIC
# MAGIC * **Z-Order** for high-cardinality filter columns
# MAGIC * **Partition** for low-cardinality filter columns
# MAGIC * Run **OPTIMIZE** regularly (weekly)
# MAGIC * Enable **Auto-Optimize** for production
# MAGIC * Monitor query performance with EXPLAIN
# MAGIC * Keep files 100MB-1GB in size
# MAGIC * Update statistics after major changes
# MAGIC
# MAGIC ## ➡️ Next: Module 9.2 - Photon Engine & Cost Optimization
# MAGIC
# MAGIC 🎉 **Module 9.1 Complete!**
