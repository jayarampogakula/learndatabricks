# Databricks notebook source
# DBTITLE 1,Module 4.2: Spark SQL
# MAGIC %md
# MAGIC # 📊 Module 4.2: Spark SQL & Window Functions
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Master Spark SQL syntax
# MAGIC * Understand window functions
# MAGIC * Learn advanced analytics patterns
# MAGIC * Build complex aggregations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Spark SQL Overview
# MAGIC
# MAGIC ### **What is Spark SQL?**
# MAGIC SQL interface to query structured data in Spark
# MAGIC
# MAGIC ### **Two Ways to Use SQL**
# MAGIC
# MAGIC 1. **DataFrame API** (Python/Scala)
# MAGIC ```python
# MAGIC df.select("name").filter(col("age") > 18)
# MAGIC ```
# MAGIC
# MAGIC 2. **SQL Queries** (SQL syntax)
# MAGIC ```sql
# MAGIC SELECT name FROM users WHERE age > 18
# MAGIC ```
# MAGIC
# MAGIC ### **Key Features**
# MAGIC ✅ **Unified**: Same engine for SQL and DataFrames
# MAGIC ✅ **Optimized**: Catalyst optimizer
# MAGIC ✅ **Compatible**: ANSI SQL standard
# MAGIC ✅ **Integrated**: Works with Delta, Parquet, JSON

# COMMAND ----------

# DBTITLE 1,Demo - Spark SQL Basics
# 📊 Demo: Spark SQL Basics

print("📊 Spark SQL Demo")
print("=" * 50)

# Create sample data
data = [
    (1, "Alice", "Engineering", 85000),
    (2, "Bob", "Sales", 65000),
    (3, "Charlie", "Engineering", 95000),
    (4, "Diana", "Marketing", 70000),
    (5, "Eve", "Engineering", 88000)
]
df = spark.createDataFrame(data, ["id", "name", "department", "salary"])

# Register as temporary view for SQL queries
df.createOrReplaceTempView("employees")
print("✅ Created temp view 'employees'\n")

# Query 1: Simple SELECT
print("1️⃣ Basic SELECT:")
spark.sql("""
    SELECT name, department, salary 
    FROM employees
    WHERE salary > 70000
""").show()

# Query 2: Aggregation
print("\n2️⃣ Aggregation by department:")
spark.sql("""
    SELECT 
        department,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary,
        MAX(salary) as max_salary
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
""").show()

print("\n✅ SQL queries work on DataFrames via temp views!")

# COMMAND ----------

# DBTITLE 1,Window Functions Explained
# MAGIC %md
# MAGIC ## 🧮 Window Functions
# MAGIC
# MAGIC ### **What are Window Functions?**
# MAGIC Perform calculations **across rows** related to current row
# MAGIC
# MAGIC ### **Key Concepts**
# MAGIC * **Window**: Group of rows
# MAGIC * **Partition**: Divide into groups
# MAGIC * **Order**: Sort within window
# MAGIC * **Frame**: Rows to include in calculation
# MAGIC
# MAGIC ### **Common Window Functions**
# MAGIC
# MAGIC #### **Ranking**
# MAGIC * `ROW_NUMBER()` - Unique sequential number
# MAGIC * `RANK()` - Rank with gaps
# MAGIC * `DENSE_RANK()` - Rank without gaps
# MAGIC
# MAGIC #### **Aggregates**
# MAGIC * `SUM() OVER()` - Running total
# MAGIC * `AVG() OVER()` - Moving average
# MAGIC * `COUNT() OVER()` - Running count
# MAGIC
# MAGIC #### **Analytics**
# MAGIC * `LAG()` - Previous row value
# MAGIC * `LEAD()` - Next row value
# MAGIC * `FIRST_VALUE()` - First in window
# MAGIC * `LAST_VALUE()` - Last in window
# MAGIC
# MAGIC ### **Basic Syntax**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     name,
# MAGIC     salary,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rank
# MAGIC FROM employees
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo - Ranking Functions
# MAGIC %sql
# MAGIC -- 🏆 Demo: Window Ranking Functions
# MAGIC
# MAGIC -- Create sample sales data
# MAGIC CREATE OR REPLACE TEMP VIEW sales AS
# MAGIC SELECT * FROM VALUES
# MAGIC     ('Alice', 'West', 100000),
# MAGIC     ('Bob', 'East', 95000),
# MAGIC     ('Charlie', 'West', 120000),
# MAGIC     ('Diana', 'East', 110000),
# MAGIC     ('Eve', 'West', 105000)
# MAGIC AS sales(name, region, revenue);
# MAGIC
# MAGIC -- Ranking within each region
# MAGIC SELECT 
# MAGIC     name,
# MAGIC     region,
# MAGIC     revenue,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) as row_num,
# MAGIC     RANK() OVER (PARTITION BY region ORDER BY revenue DESC) as rank,
# MAGIC     DENSE_RANK() OVER (PARTITION BY region ORDER BY revenue DESC) as dense_rank
# MAGIC FROM sales
# MAGIC ORDER BY region, revenue DESC

# COMMAND ----------

# DBTITLE 1,Demo - Running Totals
# MAGIC %sql
# MAGIC -- 📈 Demo: Running Totals & Moving Averages
# MAGIC
# MAGIC -- Create time series data
# MAGIC CREATE OR REPLACE TEMP VIEW orders AS
# MAGIC SELECT * FROM VALUES
# MAGIC     ('2024-01-01', 1000),
# MAGIC     ('2024-01-02', 1500),
# MAGIC     ('2024-01-03', 1200),
# MAGIC     ('2024-01-04', 1800),
# MAGIC     ('2024-01-05', 1600)
# MAGIC AS orders(date, amount);
# MAGIC
# MAGIC -- Running total and moving average
# MAGIC SELECT 
# MAGIC     date,
# MAGIC     amount,
# MAGIC     SUM(amount) OVER (ORDER BY date) as running_total,
# MAGIC     AVG(amount) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg_3day
# MAGIC FROM orders
# MAGIC ORDER BY date

# COMMAND ----------

# DBTITLE 1,Demo - LAG & LEAD
# 🔍 Demo: LAG and LEAD Functions

from pyspark.sql.functions import col, lag, lead
from pyspark.sql.window import Window

print("🔍 LAG & LEAD Demo")
print("=" * 50)

# Create time series
data = [
    ("2024-01", 1000),
    ("2024-02", 1200),
    ("2024-03", 900),
    ("2024-04", 1500)
]
df = spark.createDataFrame(data, ["month", "revenue"])

# Define window
window_spec = Window.orderBy("month")

# Add LAG (previous) and LEAD (next)
result = df.withColumn("prev_month", lag("revenue", 1).over(window_spec)) \
           .withColumn("next_month", lead("revenue", 1).over(window_spec)) \
           .withColumn("growth", col("revenue") - col("prev_month"))

print("\n📊 Month-over-Month Analysis:")
result.show()

print("\n💡 LAG = previous row, LEAD = next row!")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Spark SQL**: Query DataFrames with SQL syntax
# MAGIC ✅ **Temp Views**: Register DataFrames for SQL queries
# MAGIC ✅ **Window Functions**: Analytics across related rows
# MAGIC ✅ **Ranking**: ROW_NUMBER, RANK, DENSE_RANK
# MAGIC ✅ **Analytics**: LAG, LEAD, running totals
# MAGIC
# MAGIC ## 🎯 Common Window Functions
# MAGIC
# MAGIC | Function | Purpose | Example |
# MAGIC |----------|---------|----------|
# MAGIC | ROW_NUMBER | Unique rank | Top N per group |
# MAGIC | RANK | Rank with gaps | Leaderboards |
# MAGIC | SUM() OVER | Running total | Cumulative sales |
# MAGIC | AVG() OVER | Moving average | Trends |
# MAGIC | LAG/LEAD | Previous/Next | Growth rates |
# MAGIC
# MAGIC ## 💡 When to Use Windows
# MAGIC * Ranking within groups
# MAGIC * Running totals/averages
# MAGIC * Period-over-period comparisons
# MAGIC * Top N per category
# MAGIC
# MAGIC ## 🎓 Module 04 Complete!
# MAGIC
# MAGIC You've learned:
# MAGIC * Spark architecture & DataFrames
# MAGIC * Spark SQL queries
# MAGIC * Window functions & analytics
# MAGIC
# MAGIC ## ➡️ Next: Module 05 - SQL Analytics & BI
# MAGIC
# MAGIC 🎉 **Congratulations on completing Module 04!**
