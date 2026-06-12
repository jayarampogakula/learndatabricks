# Databricks notebook source
# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **28+ Practice Questions** covering all domains
# MAGIC ✅ **Answer Explanations** for each question
# MAGIC ✅ **Mock Test Scorer** to track progress
# MAGIC ✅ **Domain Coverage** - Platform, ELT, Incremental, Production, Governance
# MAGIC
# MAGIC ## 📈 Practice Strategy
# MAGIC
# MAGIC ### **Phase 1: Learn**
# MAGIC 1. Study each domain (Module 12.1)
# MAGIC 2. Complete hands-on labs (Modules 1-11)
# MAGIC 3. Review key concepts
# MAGIC
# MAGIC ### **Phase 2: Practice**
# MAGIC 1. Answer questions without looking
# MAGIC 2. Check answers and explanations
# MAGIC 3. Note weak areas
# MAGIC
# MAGIC ### **Phase 3: Test**
# MAGIC 1. Take full 45-question mock test
# MAGIC 2. Time yourself (90 minutes)
# MAGIC 3. Score yourself
# MAGIC 4. Review mistakes
# MAGIC
# MAGIC ### **Phase 4: Repeat**
# MAGIC 1. Focus on weak domains
# MAGIC 2. Retake until 80%+ consistently
# MAGIC 3. Schedule real exam
# MAGIC
# MAGIC ## 🎯 Scoring Guide
# MAGIC
# MAGIC | Score | Status | Action |
# MAGIC |-------|--------|--------|
# MAGIC | **90-100%** | 🎉 Excellent | Ready to take exam! |
# MAGIC | **80-89%** | 🚀 Very Good | Review weak areas, then take exam |
# MAGIC | **70-79%** | ✅ Pass | More practice recommended |
# MAGIC | **<70%** | 📚 Needs Work | Review modules, retake |
# MAGIC
# MAGIC ## 💡 Final Tips
# MAGIC
# MAGIC ✅ **Retake questions** - Until 80%+ score
# MAGIC ✅ **Time yourself** - Practice under exam conditions
# MAGIC ✅ **Review explanations** - Understand why, not just what
# MAGIC ✅ **Identify patterns** - Common question types
# MAGIC ✅ **Stay calm** - You've prepared well!
# MAGIC
# MAGIC ## 📚 Key Topics to Review
# MAGIC
# MAGIC ### **Most Tested**
# MAGIC * Delta Lake commands (OPTIMIZE, VACUUM, time travel)
# MAGIC * Auto Loader configuration
# MAGIC * Unity Catalog namespace and permissions
# MAGIC * Structured Streaming checkpoints
# MAGIC * Job clusters vs all-purpose
# MAGIC
# MAGIC ### **Common Mistakes**
# MAGIC * Confusing `read` vs `readStream`
# MAGIC * Wrong checkpoint location syntax
# MAGIC * Unity Catalog 3-level namespace
# MAGIC * VACUUM retention period effects
# MAGIC * Trigger modes (availableNow vs once)
# MAGIC
# MAGIC ## 🎓 Ready for Certification?
# MAGIC
# MAGIC ### **You're ready when:**
# MAGIC ✅ Scoring 80%+ consistently
# MAGIC ✅ Can explain all answers
# MAGIC ✅ Comfortable with time limit
# MAGIC ✅ Know all Delta Lake commands
# MAGIC ✅ Understand Unity Catalog
# MAGIC ✅ Confident with streaming concepts
# MAGIC
# MAGIC ## 🎉 Course Complete!
# MAGIC
# MAGIC Congratulations on completing the Databricks Data Engineer Associate certification prep course!
# MAGIC
# MAGIC **Course Summary:**
# MAGIC * **12 Modules** - Comprehensive coverage
# MAGIC * **28 Notebooks** - Structured learning
# MAGIC * **180+ Cells** - Detailed content
# MAGIC * **Practice Questions** - Exam preparation
# MAGIC
# MAGIC ### **What's Next?**
# MAGIC 1. Review weak areas
# MAGIC 2. Practice more questions
# MAGIC 3. Schedule your exam
# MAGIC 4. Pass with confidence!
# MAGIC
# MAGIC ## 🚀 Good luck on your certification!
# MAGIC
# MAGIC 🎓 **You've got this!**

# COMMAND ----------

# DBTITLE 1,Mock Test Scorer
# 🎯 Practice Test Scorer

print("🎯 Databricks Data Engineer Associate - Practice Test Scorer")
print("=" * 60)

def calculate_score(correct_answers, total_questions=45):
    """
    Calculate exam score and pass/fail status
    """
    percentage = (correct_answers / total_questions) * 100
    passed = percentage >= 70
    
    print(f"\n📊 Your Results:")
    print(f"   Correct: {correct_answers}/{total_questions}")
    print(f"   Percentage: {percentage:.1f}%")
    print(f"   Passing: 70% (32/45)")
    print(f"\n   Status: {'\u2705 PASS' if passed else '❌ FAIL'}")
    
    if passed:
        if percentage >= 90:
            print("   🎉 Excellent! You're ready!")
        elif percentage >= 80:
            print("   🚀 Great job! Almost perfect!")
        else:
            print("   👍 Good! Review weak areas.")
    else:
        print("   📚 Keep studying. You can do it!")
    
    return percentage, passed

# Example usage
print("\n📝 Example Scoring:")
print("\n1. Perfect Score:")
calculate_score(45, 45)

print("\n2. Passing Score:")
calculate_score(32, 45)

print("\n3. Just Below Passing:")
calculate_score(31, 45)

print("\n\n📝 Track Your Progress:")
print("""
Attempt 1: __/45 = __%
Attempt 2: __/45 = __%
Attempt 3: __/45 = __%

Goal: 80%+ consistently
""")

print("\n💡 Tips:")
print("   • Review incorrect answers")
print("   • Focus on weak domains")
print("   • Retake until 80%+")
print("   • Time yourself: 90 minutes")

# COMMAND ----------

# DBTITLE 1,Delta Lake Questions
# MAGIC %md
# MAGIC ## 🔶 Delta Lake Deep Dive
# MAGIC
# MAGIC ### **Question 23**
# MAGIC View table at specific version?
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM table ____
# MAGIC ```
# MAGIC
# MAGIC A. `AT VERSION 10`
# MAGIC B. `VERSION AS OF 10`
# MAGIC C. `WITH VERSION 10`
# MAGIC D. `USING VERSION 10`
# MAGIC
# MAGIC **Answer: B** - `VERSION AS OF 10` for time travel
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 24**
# MAGIC Optimize table and co-locate data?
# MAGIC
# MAGIC ```sql
# MAGIC OPTIMIZE table ____
# MAGIC ```
# MAGIC
# MAGIC A. `ORDER BY (col)`
# MAGIC B. `ZORDER BY (col)`
# MAGIC C. `CLUSTER BY (col)`
# MAGIC D. `SORT BY (col)`
# MAGIC
# MAGIC **Answer: B** - `ZORDER BY` for multi-dimensional clustering
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 25**
# MAGIC Remove old data files?
# MAGIC
# MAGIC ```sql
# MAGIC ____ table RETAIN 168 HOURS;
# MAGIC ```
# MAGIC
# MAGIC A. `CLEAN`
# MAGIC B. `VACUUM`
# MAGIC C. `DELETE`
# MAGIC D. `PURGE`
# MAGIC
# MAGIC **Answer: B** - `VACUUM` removes old files
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 26**
# MAGIC What does RETAIN 168 HOURS mean?
# MAGIC
# MAGIC A. Keep only last 168 hours
# MAGIC B. Keep files newer than 168 hours
# MAGIC C. Delete files older than 168 hours
# MAGIC D. Both B and C
# MAGIC
# MAGIC **Answer: D** - Keeps files < 168 hrs, deletes older
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 27**
# MAGIC View table modification history?
# MAGIC
# MAGIC ```sql
# MAGIC ____ table;
# MAGIC ```
# MAGIC
# MAGIC A. `SHOW HISTORY`
# MAGIC B. `DESCRIBE HISTORY`
# MAGIC C. `TABLE HISTORY`
# MAGIC D. `AUDIT LOG`
# MAGIC
# MAGIC **Answer: B** - `DESCRIBE HISTORY table`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 28**
# MAGIC Restore table to previous version?
# MAGIC
# MAGIC ```sql
# MAGIC RESTORE TABLE table ____ 15;
# MAGIC ```
# MAGIC
# MAGIC A. `VERSION`
# MAGIC B. `TO VERSION`
# MAGIC C. `USING VERSION`
# MAGIC D. `AT VERSION`
# MAGIC
# MAGIC **Answer: B** - `RESTORE TABLE t TO VERSION AS OF 15`

# COMMAND ----------

# DBTITLE 1,Domain 4 & 5 Questions
# MAGIC %md
# MAGIC ## 🚀 Domain 4: Production (16%)
# MAGIC
# MAGIC ### **Question 17**
# MAGIC How to pass parameters to a notebook job?
# MAGIC
# MAGIC A. Environment variables
# MAGIC B. Widgets
# MAGIC C. Arguments
# MAGIC D. All of the above
# MAGIC
# MAGIC **Answer: D** - Can use widgets, job parameters, or env vars
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 18**
# MAGIC What happens if a job task fails?
# MAGIC
# MAGIC A. Entire job fails immediately
# MAGIC B. Depends on retry policy
# MAGIC C. Job continues with next task
# MAGIC D. Task marked as skipped
# MAGIC
# MAGIC **Answer: B** - Retry policy determines behavior
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 19**
# MAGIC Which cluster type for scheduled jobs?
# MAGIC
# MAGIC A. All-purpose (recommended)
# MAGIC B. Job cluster (recommended)
# MAGIC C. Both equally good
# MAGIC D. High-concurrency only
# MAGIC
# MAGIC **Answer: B** - Job clusters are recommended for cost efficiency
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔒 Domain 5: Governance (8%)
# MAGIC
# MAGIC ### **Question 20**
# MAGIC How to grant SELECT permission?
# MAGIC
# MAGIC ```sql
# MAGIC ____ SELECT ON TABLE t TO user;
# MAGIC ```
# MAGIC
# MAGIC A. `ALLOW`
# MAGIC B. `GRANT`
# MAGIC C. `GIVE`
# MAGIC D. `PERMIT`
# MAGIC
# MAGIC **Answer: B** - Use `GRANT SELECT ON TABLE ...`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 21**
# MAGIC What shows table access permissions?
# MAGIC
# MAGIC A. `DESCRIBE GRANTS ON TABLE t`
# MAGIC B. `SHOW GRANTS ON TABLE t`
# MAGIC C. `LIST GRANTS ON TABLE t`
# MAGIC D. `GET GRANTS ON TABLE t`
# MAGIC
# MAGIC **Answer: B** - `SHOW GRANTS` displays permissions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 22**
# MAGIC What tracks data lineage?
# MAGIC
# MAGIC A. Audit logs only
# MAGIC B. Table history only
# MAGIC C. Unity Catalog
# MAGIC D. Notebooks only
# MAGIC
# MAGIC **Answer: C** - Unity Catalog provides data lineage

# COMMAND ----------

# DBTITLE 1,Domain 3: Incremental Questions
# MAGIC %md
# MAGIC ## ⏱️ Domain 3: Incremental Processing (22%)
# MAGIC
# MAGIC ### **Question 11**
# MAGIC What format does Auto Loader use?
# MAGIC
# MAGIC ```python
# MAGIC df = spark.readStream.format(____)
# MAGIC ```
# MAGIC
# MAGIC A. `"autoloader"`
# MAGIC B. `"cloudFiles"`
# MAGIC C. `"incremental"`
# MAGIC D. `"streaming"`
# MAGIC
# MAGIC **Answer: B** - Auto Loader uses `"cloudFiles"` format
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 12**
# MAGIC Which option specifies file format in Auto Loader?
# MAGIC
# MAGIC A. `cloudFiles.type`
# MAGIC B. `cloudFiles.fileFormat`
# MAGIC C. `cloudFiles.format`
# MAGIC D. `format`
# MAGIC
# MAGIC **Answer: C** - Use `cloudFiles.format` (e.g., "json", "csv")
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 13**
# MAGIC What is checkpoint location for?
# MAGIC
# MAGIC A. Store schema only
# MAGIC B. Track processed data
# MAGIC C. Store output data
# MAGIC D. Cache results
# MAGIC
# MAGIC **Answer: B** - Checkpoint tracks which data has been processed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 14**
# MAGIC Which trigger processes all available data then stops?
# MAGIC
# MAGIC A. `.trigger(processingTime="1 second")`
# MAGIC B. `.trigger(once=True)`
# MAGIC C. `.trigger(availableNow=True)`
# MAGIC D. `.trigger(continuous="1 second")`
# MAGIC
# MAGIC **Answer: C** - `availableNow=True` processes all and stops
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 15**
# MAGIC What decorator creates a DLT table?
# MAGIC
# MAGIC A. `@table`
# MAGIC B. `@dlt.table`
# MAGIC C. `@stream_table`
# MAGIC D. `@delta.table`
# MAGIC
# MAGIC **Answer: B** - Use `@dlt.table` for Declarative Pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 16**
# MAGIC How to enforce data quality in DLT?
# MAGIC
# MAGIC A. `@dlt.quality()`
# MAGIC B. `@dlt.validate()`
# MAGIC C. `@dlt.expect()`
# MAGIC D. `@dlt.constraint()`
# MAGIC
# MAGIC **Answer: C** - Use `@dlt.expect()`, `@dlt.expect_or_drop()`, etc.

# COMMAND ----------

# DBTITLE 1,Domain 2: ELT Questions
# MAGIC %md
# MAGIC ## 🔧 Domain 2: ELT (30%)
# MAGIC
# MAGIC ### **Question 6**
# MAGIC Which reads data as a stream?
# MAGIC
# MAGIC ```python
# MAGIC df = spark.____("delta").table("source")
# MAGIC ```
# MAGIC
# MAGIC A. `read`
# MAGIC B. `readStream`
# MAGIC C. `stream`
# MAGIC D. `load`
# MAGIC
# MAGIC **Answer: B** - Use `readStream` for streaming reads
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 7**
# MAGIC How to overwrite existing table?
# MAGIC
# MAGIC ```python
# MAGIC df.write.format("delta").mode(____).saveAsTable("table")
# MAGIC ```
# MAGIC
# MAGIC A. `"append"`
# MAGIC B. `"overwrite"`
# MAGIC C. `"replace"`
# MAGIC D. `"update"`
# MAGIC
# MAGIC **Answer: B** - Use `.mode("overwrite")` to replace data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 8**
# MAGIC Which command shows DataFrame schema?
# MAGIC
# MAGIC A. `df.schema()`
# MAGIC B. `df.printSchema()`
# MAGIC C. `df.describe()`
# MAGIC D. `df.show()`
# MAGIC
# MAGIC **Answer: B** - `df.printSchema()` displays column types
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 9**
# MAGIC How to select distinct values?
# MAGIC
# MAGIC ```python
# MAGIC df.select("col").____
# MAGIC ```
# MAGIC
# MAGIC A. `unique()`
# MAGIC B. `distinct()`
# MAGIC C. `dedupe()`
# MAGIC D. `dropDuplicates()`
# MAGIC
# MAGIC **Answer: B or D** - Both `distinct()` and `dropDuplicates()` work
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 10**
# MAGIC Which joins all records from both tables?
# MAGIC
# MAGIC A. `inner`
# MAGIC B. `left`
# MAGIC C. `right`
# MAGIC D. `full`
# MAGIC
# MAGIC **Answer: D** - Full outer join includes all records

# COMMAND ----------

# DBTITLE 1,Domain 1: Platform Questions
# MAGIC %md
# MAGIC ## 🎯 Domain 1: Platform (24%)
# MAGIC
# MAGIC ### **Question 1**
# MAGIC What is the correct Unity Catalog namespace hierarchy?
# MAGIC
# MAGIC A. schema.catalog.table
# MAGIC B. catalog.schema.table
# MAGIC C. table.schema.catalog
# MAGIC D. catalog.table.schema
# MAGIC
# MAGIC **Answer: B** - Unity Catalog uses 3-level: catalog.schema.table
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 2**
# MAGIC Which cluster type is most cost-effective for production jobs?
# MAGIC
# MAGIC A. All-purpose cluster
# MAGIC B. High-concurrency cluster
# MAGIC C. Job cluster
# MAGIC D. Single-node cluster
# MAGIC
# MAGIC **Answer: C** - Job clusters start/stop with job, minimizing costs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 3**
# MAGIC What does Delta Lake provide?
# MAGIC
# MAGIC A. ACID transactions only
# MAGIC B. Schema evolution only
# MAGIC C. Time travel only
# MAGIC D. All of the above
# MAGIC
# MAGIC **Answer: D** - Delta Lake provides ACID, schema evolution, time travel, and more
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 4**
# MAGIC Which SQL warehouse type is serverless?
# MAGIC
# MAGIC A. Classic SQL warehouse
# MAGIC B. Pro SQL warehouse
# MAGIC C. Serverless SQL warehouse
# MAGIC D. All SQL warehouses
# MAGIC
# MAGIC **Answer: C** - Serverless SQL warehouses don't require cluster management
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 5**
# MAGIC What is a managed table in Unity Catalog?
# MAGIC
# MAGIC A. Table where Databricks manages data and metadata
# MAGIC B. Table where user manages data location
# MAGIC C. Temporary table
# MAGIC D. View over external data
# MAGIC
# MAGIC **Answer: A** - Managed tables: Databricks manages both data and metadata

# COMMAND ----------

# DBTITLE 1,Module 12.2 Introduction
# MAGIC %md
# MAGIC # 📝 Module 12.2: Practice Questions & Mock Tests
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Practice exam-style questions
# MAGIC * Test knowledge across all domains
# MAGIC * Identify weak areas
# MAGIC * Build exam confidence
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 How to Use This Notebook
# MAGIC
# MAGIC ### **Practice Questions**
# MAGIC * 30 questions across 5 domains
# MAGIC * Similar to actual exam format
# MAGIC * Detailed explanations
# MAGIC
# MAGIC ### **Mock Test**
# MAGIC * 45-question full practice exam
# MAGIC * 90-minute time limit
# MAGIC * Covers all domains
# MAGIC
# MAGIC ### **Scoring**
# MAGIC * Passing: 70% (32/45)
# MAGIC * Track your progress
# MAGIC * Review incorrect answers
# MAGIC
# MAGIC ### **Tips**
# MAGIC ✅ Time yourself
# MAGIC ✅ Don't look at answers first
# MAGIC ✅ Review explanations
# MAGIC ✅ Retake until 80%+
