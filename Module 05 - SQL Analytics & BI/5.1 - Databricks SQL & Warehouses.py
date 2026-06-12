# Databricks notebook source
# DBTITLE 1,Module 5.1: Databricks SQL
# MAGIC %md
# MAGIC # 📊 Module 5.1: Databricks SQL & Warehouses
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand Databricks SQL environment
# MAGIC * Master SQL Warehouses
# MAGIC * Learn BI integration
# MAGIC * Build analytics queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 What is Databricks SQL?
# MAGIC
# MAGIC **Databricks SQL** is a data warehouse and BI platform built on the lakehouse architecture.
# MAGIC
# MAGIC ### **Key Features**
# MAGIC ✅ **SQL Warehouses** - Optimized compute for analytics
# MAGIC ✅ **Photon Engine** - 12x faster queries
# MAGIC ✅ **BI Integrations** - Tableau, Power BI, Looker
# MAGIC ✅ **Dashboards** - Built-in visualization
# MAGIC ✅ **Query Editor** - Interactive SQL development
# MAGIC ✅ **Unity Catalog** - Unified governance
# MAGIC
# MAGIC ### **Why Use Databricks SQL?**
# MAGIC * **Performance**: Photon-accelerated queries
# MAGIC * **Cost**: Serverless, pay-per-query
# MAGIC * **Simplicity**: Standard SQL syntax
# MAGIC * **Integration**: Connect any BI tool
# MAGIC * **Governance**: Unity Catalog security

# COMMAND ----------

# DBTITLE 1,SQL Warehouses Explained
# MAGIC %md
# MAGIC ## 🏭 SQL Warehouses
# MAGIC
# MAGIC ### **What are SQL Warehouses?**
# MAGIC Optimized compute clusters for SQL queries and BI workloads
# MAGIC
# MAGIC ### **Types of SQL Warehouses**
# MAGIC
# MAGIC #### **1. Serverless** ⭐ (Recommended)
# MAGIC * ✅ Instant startup (<5 seconds)
# MAGIC * ✅ Auto-scaling
# MAGIC * ✅ No cluster management
# MAGIC * ✅ Pay-per-query
# MAGIC * 👉 Best for: Most workloads
# MAGIC
# MAGIC #### **2. Pro**
# MAGIC * 🔧 Manual cluster configuration
# MAGIC * 📊 Predictable performance
# MAGIC * 👉 Best for: Consistent heavy workloads
# MAGIC
# MAGIC #### **3. Classic**
# MAGIC * 🗓️ Legacy option
# MAGIC * 👉 Use Serverless or Pro instead
# MAGIC
# MAGIC ### **Warehouse Sizes**
# MAGIC | Size | Clusters | Use Case |
# MAGIC |------|----------|----------|
# MAGIC | X-Small | 1 | Development, light queries |
# MAGIC | Small | 2 | Small teams |
# MAGIC | Medium | 4 | Production workloads |
# MAGIC | Large | 8 | Heavy analytics |
# MAGIC | X-Large | 16 | Enterprise scale |

# COMMAND ----------

# DBTITLE 1,Demo - SQL Queries
# 📊 Demo: Analytics Queries

from pyspark.sql.functions import col, sum, avg, count, month, year

print("📊 Databricks SQL Analytics Demo")
print("=" * 60)

# Create sample sales data
data = [
    ("2024-01-15", "Laptop", "Electronics", 999.99, 5),
    ("2024-01-20", "Mouse", "Electronics", 29.99, 15),
    ("2024-02-10", "Desk", "Furniture", 299.99, 3),
    ("2024-02-15", "Chair", "Furniture", 149.99, 8),
    ("2024-03-05", "Monitor", "Electronics", 399.99, 7)
]
df = spark.createDataFrame(data, ["date", "product", "category", "price", "quantity"])

# Save as Delta table for querying
df.write.format("delta").mode("overwrite").saveAsTable("sales_demo")
print("✅ Created sales_demo table\n")

# Query 1: Total revenue by category
print("1️⃣ Revenue by Category:")
spark.sql("""
    SELECT 
        category,
        COUNT(*) as product_count,
        SUM(price * quantity) as total_revenue,
        AVG(price) as avg_price
    FROM sales_demo
    GROUP BY category
    ORDER BY total_revenue DESC
""").show()

# Query 2: Monthly sales trend
print("\n2️⃣ Monthly Sales:")
spark.sql("""
    SELECT 
        DATE_TRUNC('month', date) as month,
        SUM(price * quantity) as monthly_revenue
    FROM sales_demo
    GROUP BY month
    ORDER BY month
""").show()

print("\n💡 These queries run on SQL Warehouses in production!")

# COMMAND ----------

# DBTITLE 1,Performance Optimization
# MAGIC %md
# MAGIC ## 🚀 Performance Optimization
# MAGIC
# MAGIC ### **Photon Engine**
# MAGIC Vectorized query engine (12x faster!)
# MAGIC * ✅ Automatic with Serverless warehouses
# MAGIC * ✅ Optimized for SQL workloads
# MAGIC * ✅ No code changes needed
# MAGIC
# MAGIC ### **Query Optimization Tips**
# MAGIC
# MAGIC 1. **Filter Early**
# MAGIC ```sql
# MAGIC -- Good: Filter before join
# MAGIC SELECT * FROM large_table 
# MAGIC WHERE date >= '2024-01-01'
# MAGIC JOIN small_table USING (id)
# MAGIC ```
# MAGIC
# MAGIC 2. **Use Partitions**
# MAGIC ```sql
# MAGIC -- Queries scan only needed partitions
# MAGIC SELECT * FROM sales WHERE date = '2024-01-15'
# MAGIC ```
# MAGIC
# MAGIC 3. **Avoid SELECT ***
# MAGIC ```sql
# MAGIC -- Better: Select only needed columns
# MAGIC SELECT id, name, revenue FROM sales
# MAGIC ```
# MAGIC
# MAGIC 4. **Use Delta Cache**
# MAGIC * Automatic with SQL Warehouses
# MAGIC * Caches frequently accessed data
# MAGIC
# MAGIC 5. **Enable Result Cache**
# MAGIC * Reuse results for identical queries
# MAGIC * Saves compute time and cost

# COMMAND ----------

# DBTITLE 1,BI Tool Integration
# MAGIC %md
# MAGIC ## 🔗 BI Tool Integration
# MAGIC
# MAGIC ### **Supported BI Tools**
# MAGIC * 📊 **Tableau** - Native connector
# MAGIC * 📊 **Power BI** - Native connector
# MAGIC * 📊 **Looker** - JDBC/ODBC
# MAGIC * 📊 **Qlik** - JDBC/ODBC
# MAGIC * 📊 **Excel** - ODBC
# MAGIC
# MAGIC ### **Connection Methods**
# MAGIC
# MAGIC #### **Partner Connect** (Easiest)
# MAGIC 1. Go to Partner Connect in Databricks
# MAGIC 2. Select your BI tool
# MAGIC 3. Auto-configures connection
# MAGIC 4. Start building dashboards
# MAGIC
# MAGIC #### **Manual Connection**
# MAGIC 1. Get SQL Warehouse connection details
# MAGIC 2. Use JDBC/ODBC driver
# MAGIC 3. Configure in BI tool
# MAGIC
# MAGIC ### **Connection String Example**
# MAGIC ```
# MAGIC Server: <workspace-url>
# MAGIC HTTP Path: /sql/1.0/warehouses/<warehouse-id>
# MAGIC Authentication: Personal Access Token
# MAGIC ```
# MAGIC
# MAGIC ### **Best Practices**
# MAGIC ✅ Use Serverless warehouses for BI
# MAGIC ✅ Enable auto-stop (15-120 min)
# MAGIC ✅ Use Unity Catalog for governance
# MAGIC ✅ Monitor query performance

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Databricks SQL** = Lakehouse-based data warehouse
# MAGIC ✅ **SQL Warehouses** = Optimized compute (Serverless recommended)
# MAGIC ✅ **Photon Engine** = 12x faster queries
# MAGIC ✅ **BI Integration** = Native connectors for Tableau, Power BI
# MAGIC ✅ **Unity Catalog** = Unified governance
# MAGIC
# MAGIC ## 🎯 Key Features
# MAGIC * Serverless SQL warehouses with instant startup
# MAGIC * Photon-accelerated query performance
# MAGIC * Standard SQL syntax (ANSI SQL)
# MAGIC * Connect any BI tool via JDBC/ODBC
# MAGIC * Built-in dashboards and visualizations
# MAGIC
# MAGIC ## 💡 When to Use Databricks SQL
# MAGIC ✅ **Analytics queries** on Delta tables
# MAGIC ✅ **BI dashboards** and reports
# MAGIC ✅ **Ad-hoc analysis** by analysts
# MAGIC ✅ **Data exploration** before ETL
# MAGIC
# MAGIC ❌ **Don't use for**:
# MAGIC * Heavy transformations (use notebooks/jobs)
# MAGIC * Real-time streaming (use Spark Structured Streaming)
# MAGIC
# MAGIC ## ➡️ Next: Module 5.2 - Dashboards & Visualizations
# MAGIC
# MAGIC 🎉 **Module 5.1 Complete!**
