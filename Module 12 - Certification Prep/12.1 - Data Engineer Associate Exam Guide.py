# Databricks notebook source
# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Exam**: 45 questions, 90 min, 70% pass
# MAGIC ✅ **Domains**: Platform (24%), ELT (30%), Incremental (22%), Production (16%), Governance (8%)
# MAGIC ✅ **Study**: 5-week plan, hands-on practice
# MAGIC ✅ **Resources**: Academy, docs, this course
# MAGIC
# MAGIC ## 🎯 Key Topics
# MAGIC
# MAGIC | Topic | Must Know |
# MAGIC |-------|----------|
# MAGIC | **Delta** | OPTIMIZE, VACUUM, time travel |
# MAGIC | **Auto Loader** | cloudFiles format, schema location |
# MAGIC | **Unity Catalog** | 3-level namespace, GRANT |
# MAGIC | **Streaming** | checkpoint, availableNow |
# MAGIC | **Pipelines** | @dlt.table, expectations |
# MAGIC | **Jobs** | Job clusters, parameters |
# MAGIC
# MAGIC ## 📚 Quick Commands
# MAGIC
# MAGIC ```sql
# MAGIC -- Delta Lake
# MAGIC DESCRIBE HISTORY table;
# MAGIC OPTIMIZE table ZORDER BY (col);
# MAGIC VACUUM table RETAIN 168 HOURS;
# MAGIC SELECT * FROM table VERSION AS OF 10;
# MAGIC
# MAGIC -- Unity Catalog
# MAGIC GRANT SELECT ON TABLE t TO user;
# MAGIC SHOW GRANTS ON TABLE t;
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC # Auto Loader
# MAGIC .format("cloudFiles")
# MAGIC .option("cloudFiles.format", "json")
# MAGIC .option("cloudFiles.schemaLocation", "/path")
# MAGIC
# MAGIC # Streaming
# MAGIC .writeStream
# MAGIC .option("checkpointLocation", "/checkpoint")
# MAGIC .trigger(availableNow=True)
# MAGIC .table("target")
# MAGIC ```
# MAGIC
# MAGIC ## 💡 Final Tips
# MAGIC
# MAGIC ✅ Practice hands-on
# MAGIC ✅ Use Community Edition
# MAGIC ✅ Time yourself
# MAGIC ✅ Review mistakes
# MAGIC ✅ Stay calm
# MAGIC
# MAGIC ## 🎓 Certification Benefits
# MAGIC
# MAGIC * Career advancement
# MAGIC * Higher salary
# MAGIC * Industry recognition
# MAGIC * Deep knowledge
# MAGIC * Professional network
# MAGIC
# MAGIC ## ➡️ Next: Module 12.2 - Practice Questions
# MAGIC
# MAGIC 🎉 **Good luck on your certification!**

# COMMAND ----------

# DBTITLE 1,Sample Questions
# MAGIC %md
# MAGIC ## 📝 Sample Questions
# MAGIC
# MAGIC ### **Q1: Delta Lake**
# MAGIC Which command views table history?
# MAGIC
# MAGIC A. `SHOW HISTORY table`
# MAGIC B. `DESCRIBE HISTORY table`
# MAGIC C. `SELECT HISTORY FROM table`
# MAGIC D. `GET HISTORY table`
# MAGIC
# MAGIC **Answer: B**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q2: Auto Loader**
# MAGIC Which format for CSV files?
# MAGIC
# MAGIC ```python
# MAGIC .format("cloudFiles")
# MAGIC .option("cloudFiles.format", ____)
# MAGIC ```
# MAGIC
# MAGIC A. `"autoloader"`
# MAGIC B. `"csv"`
# MAGIC C. `"delta"`
# MAGIC D. `"cloudFiles"`
# MAGIC
# MAGIC **Answer: B**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q3: Unity Catalog**
# MAGIC Correct namespace order?
# MAGIC
# MAGIC A. schema → catalog → table
# MAGIC B. catalog → table → schema
# MAGIC C. catalog → schema → table
# MAGIC D. table → schema → catalog
# MAGIC
# MAGIC **Answer: C**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q4: Streaming**
# MAGIC Process all available data then stop?
# MAGIC
# MAGIC A. `.trigger(processingTime="5s")`
# MAGIC B. `.trigger(once=True)`
# MAGIC C. `.trigger(availableNow=True)`
# MAGIC D. `.trigger(continuous="1s")`
# MAGIC
# MAGIC **Answer: C**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q5: Optimization**
# MAGIC Co-locate related data?
# MAGIC
# MAGIC A. `VACUUM`
# MAGIC B. `OPTIMIZE`
# MAGIC C. `COMPACT`
# MAGIC D. `ZORDER`
# MAGIC
# MAGIC **Answer: D** (OPTIMIZE ... ZORDER BY)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q6: Jobs**
# MAGIC Most cost-effective for production?
# MAGIC
# MAGIC A. All-purpose cluster
# MAGIC B. High-concurrency
# MAGIC C. Job cluster
# MAGIC D. Shared cluster
# MAGIC
# MAGIC **Answer: C**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 👉 **More in Module 12.2!**

# COMMAND ----------

# DBTITLE 1,Exam Tips
# MAGIC %md
# MAGIC ## 💡 Exam Tips
# MAGIC
# MAGIC ### **Time Management**
# MAGIC
# MAGIC * 90 minutes / 45 questions = 2 min/question
# MAGIC * Flag difficult questions
# MAGIC * Review flagged at end
# MAGIC * Don't overthink
# MAGIC
# MAGIC ### **Question Strategy**
# MAGIC
# MAGIC ✅ Read carefully
# MAGIC ✅ Eliminate wrong answers
# MAGIC ✅ Watch for "NOT", "EXCEPT"
# MAGIC ✅ Look for keywords
# MAGIC
# MAGIC ### **Common Pitfalls**
# MAGIC
# MAGIC ❌ Don't confuse:
# MAGIC * `read` vs `readStream`
# MAGIC * `save` vs `saveAsTable`
# MAGIC * Managed vs external tables
# MAGIC * Job vs all-purpose clusters
# MAGIC
# MAGIC ### **Quick Reference**
# MAGIC
# MAGIC ```sql
# MAGIC -- Delta Lake
# MAGIC OPTIMIZE table ZORDER BY (col);
# MAGIC VACUUM table RETAIN 168 HOURS;
# MAGIC DESCRIBE HISTORY table;
# MAGIC
# MAGIC -- Unity Catalog
# MAGIC GRANT SELECT ON TABLE t TO user;
# MAGIC SHOW GRANTS ON TABLE t;
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC # Auto Loader
# MAGIC .format("cloudFiles")
# MAGIC .option("cloudFiles.format", "json")
# MAGIC
# MAGIC # Streaming
# MAGIC .writeStream
# MAGIC .option("checkpointLocation", "/path")
# MAGIC .trigger(availableNow=True)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Study Strategy
# MAGIC %md
# MAGIC ## 📚 5-Week Study Plan
# MAGIC
# MAGIC ### **Week 1-2: Foundation**
# MAGIC
# MAGIC ✅ Complete Modules 1-5
# MAGIC ✅ Databricks platform basics
# MAGIC ✅ Delta Lake fundamentals
# MAGIC ✅ Spark SQL & DataFrames
# MAGIC ✅ Unity Catalog basics
# MAGIC
# MAGIC ### **Week 3-4: Advanced**
# MAGIC
# MAGIC ✅ Complete Modules 6-10
# MAGIC ✅ Streaming & Auto Loader
# MAGIC ✅ Declarative Pipelines
# MAGIC ✅ Jobs & orchestration
# MAGIC ✅ Performance optimization
# MAGIC
# MAGIC ### **Week 5: Practice**
# MAGIC
# MAGIC ✅ Practice questions (Module 12.2)
# MAGIC ✅ Mock exams
# MAGIC ✅ Review weak areas
# MAGIC ✅ Hands-on labs
# MAGIC
# MAGIC ## 📚 Resources
# MAGIC
# MAGIC ### **Official**
# MAGIC * Databricks Academy (free)
# MAGIC * Documentation
# MAGIC * Exam guide
# MAGIC
# MAGIC ### **This Course**
# MAGIC * 28 notebooks
# MAGIC * 171+ cells
# MAGIC * Comprehensive coverage
# MAGIC
# MAGIC ### **Practice**
# MAGIC * Community Edition (free)
# MAGIC * Sample datasets
# MAGIC * Practice exams

# COMMAND ----------

# DBTITLE 1,Key Topics
# 🎯 Demo: Key Exam Topics

print("🎯 Databricks Data Engineer Associate - Key Topics")
print("=" * 60)

print("\n1️⃣ Delta Lake Operations:")
print("""\n-- Time travel
SELECT * FROM table VERSION AS OF 10;
SELECT * FROM table TIMESTAMP AS OF '2024-01-01';

-- Optimize with Z-ORDER
OPTIMIZE table ZORDER BY (col1, col2);

-- Vacuum (clean old files)
VACUUM table RETAIN 168 HOURS;

-- View history
DESCRIBE HISTORY table;
""")

print("\n2️⃣ Auto Loader:")
print("""\n# Ingest files automatically
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/schema/path")
    .load("/data/path")
)

(df.writeStream
    .option("checkpointLocation", "/checkpoint/path")
    .table("target_table")
)
""")

print("\n3️⃣ Unity Catalog:")
print("""\n-- 3-level namespace
catalog.schema.table

-- Grant permissions
GRANT SELECT ON TABLE catalog.schema.table TO user;

-- Show grants
SHOW GRANTS ON TABLE table;
""")

print("\n4️⃣ Structured Streaming:")
print("""\n# Streaming query
(spark.readStream
    .table("source")
    .writeStream
    .option("checkpointLocation", "/checkpoint")
    .trigger(availableNow=True)  # Process all available
    .table("target")
)
""")

print("\n5️⃣ Declarative Pipelines:")
print("""\nimport dlt

@dlt.table
@dlt.expect_or_drop("valid_amount", "amount > 0")
def clean_orders():
    return spark.readStream.table("raw_orders")
""")

print("\n✅ Study Focus Areas:")
print("   • Delta Lake commands (70% of questions)")
print("   • Auto Loader configuration")
print("   • Unity Catalog namespace")
print("   • Streaming checkpoints")
print("   • Job clusters vs all-purpose")

# COMMAND ----------

# DBTITLE 1,Exam Domains
# MAGIC %md
# MAGIC ## 📚 Exam Domains
# MAGIC
# MAGIC ### **Domain 1: Platform (24%)**
# MAGIC
# MAGIC * Lakehouse architecture
# MAGIC * Unity Catalog
# MAGIC * Delta Lake basics
# MAGIC * Compute resources
# MAGIC
# MAGIC ### **Domain 2: ELT (30%)**
# MAGIC
# MAGIC * Spark SQL & Python
# MAGIC * DataFrames
# MAGIC * Transformations
# MAGIC * Data quality
# MAGIC
# MAGIC ### **Domain 3: Incremental (22%)**
# MAGIC
# MAGIC * Structured Streaming
# MAGIC * Auto Loader
# MAGIC * CDC
# MAGIC * Declarative Pipelines
# MAGIC
# MAGIC ### **Domain 4: Production (16%)**
# MAGIC
# MAGIC * Jobs & workflows
# MAGIC * Monitoring
# MAGIC * Error handling
# MAGIC * Performance
# MAGIC
# MAGIC ### **Domain 5: Governance (8%)**
# MAGIC
# MAGIC * Security
# MAGIC * Access control
# MAGIC * Lineage
# MAGIC * Compliance

# COMMAND ----------

# DBTITLE 1,Exam Overview
# MAGIC %md
# MAGIC ## 🎯 Databricks Certified Data Engineer Associate
# MAGIC
# MAGIC **Official Databricks certification** validating foundational data engineering skills.
# MAGIC
# MAGIC ### **Exam Details**
# MAGIC
# MAGIC * **Duration**: 90 minutes
# MAGIC * **Questions**: 45 multiple-choice
# MAGIC * **Passing Score**: 70% (32/45)
# MAGIC * **Format**: Online proctored
# MAGIC * **Cost**: $200 USD
# MAGIC * **Validity**: 2 years
# MAGIC
# MAGIC ### **Who Should Take This Exam?**
# MAGIC
# MAGIC ✅ **Data Engineers** - Building pipelines
# MAGIC ✅ **ETL Developers** - Data transformation
# MAGIC ✅ **Analytics Engineers** - Data prep
# MAGIC ✅ **Software Engineers** - Big data work

# COMMAND ----------

# DBTITLE 1,Module 12.1 Introduction
# MAGIC %md
# MAGIC # 🎓 Module 12.1: Data Engineer Associate Exam Guide
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand exam structure
# MAGIC * Review key domains
# MAGIC * Learn preparation strategies
# MAGIC * Access study resources
