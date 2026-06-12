# Databricks notebook source
# DBTITLE 1,Module 1.5: Storage
# MAGIC %md
# MAGIC # 💾 Module 1.5: DBFS & Storage
# MAGIC
# MAGIC ## Storage Options
# MAGIC
# MAGIC ### Unity Catalog Volumes ⭐
# MAGIC * Recommended modern storage
# MAGIC * Path: `/Volumes/catalog/schema/volume/`
# MAGIC * Governed access
# MAGIC
# MAGIC ### DBFS (Legacy)
# MAGIC * Path: `/dbfs/FileStore/`
# MAGIC * Mounted cloud storage
# MAGIC
# MAGIC ## File Formats
# MAGIC * **Delta**: ACID transactions ✅
# MAGIC * **Parquet**: Big data ✅
# MAGIC * **CSV**: Simple data
# MAGIC
# MAGIC 💡 Use Delta for tables!

# COMMAND ----------

# DBTITLE 1,Demo & Summary
# 📂 Demo: Write & Read Files

data = [(1, "Alice"), (2, "Bob")]
df = spark.createDataFrame(data, ["id", "name"])

# Write Delta format
df.write.mode("overwrite").format("delta").save("/tmp/demo_delta")
print("✅ Wrote Delta format")

# Read it back
result = spark.read.format("delta").load("/tmp/demo_delta")
print(f"📊 Rows: {result.count()}")

print("\n🎉 MODULE 01 COMPLETE!")
print("➡️  Next: Module 02 - Data Engineering")
