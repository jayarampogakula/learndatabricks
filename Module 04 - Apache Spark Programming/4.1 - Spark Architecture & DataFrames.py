# Databricks notebook source
# DBTITLE 1,Module 4.1: Spark Architecture
# MAGIC %md
# MAGIC # ⚡ Module 4.1: Spark Architecture & DataFrames
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand Apache Spark architecture
# MAGIC * Master Spark DataFrames API
# MAGIC * Learn distributed computing concepts
# MAGIC * Build data transformations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏗️ Apache Spark Architecture
# MAGIC
# MAGIC ### **What is Spark?**
# MAGIC Unified analytics engine for large-scale data processing
# MAGIC
# MAGIC ### **Key Components**
# MAGIC * **Driver**: Coordinates execution
# MAGIC * **Executors**: Run tasks on workers
# MAGIC * **Tasks**: Process data partitions
# MAGIC * **Partitions**: Distributed data chunks
# MAGIC
# MAGIC ### **Execution Flow**
# MAGIC ```
# MAGIC Driver (notebook)
# MAGIC     ↓
# MAGIC SparkSession
# MAGIC     ↓
# MAGIC Executors → Process in parallel
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Transformations vs Actions
# MAGIC %md
# MAGIC ## 🔄 Transformations vs Actions
# MAGIC
# MAGIC ### **Transformations (Lazy)**
# MAGIC Build execution plan, don't execute yet
# MAGIC * `select()`, `filter()`, `groupBy()`, `join()`
# MAGIC * Return new DataFrame
# MAGIC
# MAGIC ### **Actions (Eager)**
# MAGIC Trigger execution
# MAGIC * `show()`, `count()`, `collect()`, `write()`
# MAGIC * Return results
# MAGIC
# MAGIC ### **Lazy Evaluation Example**
# MAGIC ```python
# MAGIC df = spark.read.parquet("/data")    # No execution
# MAGIC df2 = df.filter(col("age") > 18)    # No execution
# MAGIC df3 = df2.select("name")            # No execution
# MAGIC df3.show()                          # NOW executes!
# MAGIC ```
# MAGIC
# MAGIC 💡 **Spark optimizes the entire plan before execution**

# COMMAND ----------

# DBTITLE 1,Demo - Creating DataFrames
# ⚡ Demo: Creating Spark DataFrames

print("⚡ Creating DataFrames")
print("=" * 50)

# Method 1: From Python list
print("\n1️⃣ From Python list:")
data = [(1, "Alice", 85), (2, "Bob", 92)]
df = spark.createDataFrame(data, ["id", "name", "score"])
df.show()

# Method 2: From dictionary
print("\n2️⃣ From dictionary:")
data_dict = [{"id": 1, "product": "Laptop", "price": 999.99}]
df2 = spark.createDataFrame(data_dict)
df2.show()

# Method 3: Read from Delta
print("\n3️⃣ From Delta table:")
df.write.format("delta").mode("overwrite").save("/tmp/demo")
df3 = spark.read.format("delta").load("/tmp/demo")
print(f"✅ Read {df3.count()} rows from Delta")

print("\n💡 DataFrames are distributed & immutable!")

# COMMAND ----------

# DBTITLE 1,Demo - Transformations
# 🔧 Demo: DataFrame Transformations

from pyspark.sql.functions import col, sum, avg, count

print("🔧 DataFrame Operations")
print("=" * 50)

# Sample data
data = [
    (1, "Laptop", "Electronics", 999.99, 5),
    (2, "Mouse", "Electronics", 29.99, 15),
    (3, "Desk", "Furniture", 299.99, 3)
]
df = spark.createDataFrame(data, ["id", "product", "category", "price", "quantity"])

print("\n📊 Original Data:")
df.show()

# 1. SELECT
print("\n1️⃣ SELECT columns:")
df.select("product", "price").show()

# 2. FILTER
print("\n2️⃣ FILTER rows:")
df.filter(col("category") == "Electronics").show()

# 3. WITH COLUMN - add calculated column
print("\n3️⃣ WITH COLUMN (add revenue):")
df_revenue = df.withColumn("revenue", col("price") * col("quantity"))
df_revenue.show()

# 4. GROUP BY - aggregate
print("\n4️⃣ GROUP BY category:")
df_revenue.groupBy("category").agg(
    sum("revenue").alias("total_revenue"),
    count("*").alias("products")
).show()

print("\n✅ All operations return NEW DataFrames!")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Spark**: Distributed data processing engine
# MAGIC ✅ **DataFrames**: Distributed, immutable, schema-based
# MAGIC ✅ **Lazy Evaluation**: Build plan, execute on action
# MAGIC ✅ **Operations**: select, filter, groupBy, withColumn
# MAGIC
# MAGIC ## 🎯 Key Points
# MAGIC * Transformations are lazy (no execution)
# MAGIC * Actions trigger execution
# MAGIC * DataFrames are immutable
# MAGIC * Data processed in parallel
# MAGIC
# MAGIC ## 💡 Best Practices
# MAGIC * Filter early to reduce data
# MAGIC * Use select() to reduce columns
# MAGIC * Avoid collect() on large data
# MAGIC * Understand transformations vs actions
# MAGIC
# MAGIC ## ➡️ Next: Module 4.2 - Spark SQL & Window Functions
# MAGIC
# MAGIC 🎉 **Module 4.1 Complete!**
