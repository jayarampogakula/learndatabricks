# Databricks notebook source
# DBTITLE 1,📚 Module 1.4: Notebooks & Languages
# MAGIC %md
# MAGIC # 📚 Module 1.4: Notebooks & Languages
# MAGIC
# MAGIC ## Supported Languages
# MAGIC * 🐍 **Python** - Most popular, default
# MAGIC * 📊 **SQL** - Data analysis & queries
# MAGIC * 📈 **R** - Statistical analysis (Classic compute only)
# MAGIC * ⚙️ **Scala** - Spark native (Classic compute only)
# MAGIC
# MAGIC **Note**: On Serverless, Python, SQL, and Shell are supported.
# MAGIC
# MAGIC ## ✨ Magic Commands
# MAGIC * `%python` - Python code
# MAGIC * `%sql` - SQL queries
# MAGIC * `%md` - Markdown text
# MAGIC * `%sh` - Shell commands
# MAGIC * `%run` - Execute another notebook
# MAGIC
# MAGIC ## 🔄 Multi-Language Notebooks
# MAGIC You can mix languages in one notebook!

# COMMAND ----------

# DBTITLE 1,Demo - Python & SQL
# 🐍 Python: Create DataFrame
print("1️⃣ Creating data in Python...")

data = [(1, "Alice", 85), (2, "Bob", 92), (3, "Charlie", 78)]
df = spark.createDataFrame(data, ["id", "name", "score"])

# Create temp view for SQL
df.createOrReplaceTempView("students")
print("✅ Created temp view 'students'")
print("\n2️⃣ Next cell: Query with SQL!")

# COMMAND ----------

# DBTITLE 1,SQL Query
# MAGIC %sql
# MAGIC -- 📊 SQL Query on Python DataFrame
# MAGIC SELECT 
# MAGIC   name,
# MAGIC   score,
# MAGIC   CASE 
# MAGIC     WHEN score >= 90 THEN 'A'
# MAGIC     WHEN score >= 80 THEN 'B'
# MAGIC     ELSE 'C'
# MAGIC   END as grade
# MAGIC FROM students
# MAGIC ORDER BY score DESC

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Languages**: Python, SQL supported on Serverless
# MAGIC ✅ **Magic Commands**: Mix languages with %
# MAGIC ✅ **Data Sharing**: Python ↔ SQL via temp views
# MAGIC
# MAGIC ## ➡️ Next: Module 1.5 - DBFS & Storage
# MAGIC
# MAGIC 🎉 **Module 1.4 Complete!**
