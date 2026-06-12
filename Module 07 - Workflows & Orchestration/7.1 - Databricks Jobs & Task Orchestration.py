# Databricks notebook source
# DBTITLE 1,Module 7.1: Databricks Jobs
# MAGIC %md
# MAGIC # 🔄 Module 7.1: Databricks Jobs & Task Orchestration
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand Databricks Jobs
# MAGIC * Create and configure jobs
# MAGIC * Orchestrate multi-task workflows
# MAGIC * Schedule and monitor jobs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 What are Databricks Jobs?
# MAGIC
# MAGIC **Databricks Jobs** are the orchestration layer for running data and ML workflows.
# MAGIC
# MAGIC ### **Key Features**
# MAGIC
# MAGIC ✅ **Task Orchestration** - Chain multiple tasks
# MAGIC ✅ **Scheduling** - Cron or continuous
# MAGIC ✅ **Dependencies** - Define task order
# MAGIC ✅ **Monitoring** - Track runs and failures
# MAGIC ✅ **Alerts** - Email/webhook notifications
# MAGIC ✅ **Job Clusters** - Cost-effective compute
# MAGIC
# MAGIC ### **Why Use Jobs?**
# MAGIC * **Automation** - Schedule recurring workflows
# MAGIC * **Reliability** - Retries and error handling
# MAGIC * **Scalability** - Parallel task execution
# MAGIC * **Observability** - Centralized monitoring
# MAGIC * **Cost Control** - Job clusters vs all-purpose

# COMMAND ----------

# DBTITLE 1,Job Components
# MAGIC %md
# MAGIC ## 🧩 Job Components
# MAGIC
# MAGIC ### **Task Types**
# MAGIC
# MAGIC 1. **Notebook Task** - Run a notebook
# MAGIC 2. **Python Script** - Execute .py file
# MAGIC 3. **JAR** - Run Scala/Java code
# MAGIC 4. **Spark Submit** - Submit Spark application
# MAGIC 5. **Pipeline (DLT)** - Run Delta Live Tables pipeline
# MAGIC 6. **SQL Query/Dashboard** - Execute SQL
# MAGIC 7. **dbt** - Run dbt models
# MAGIC
# MAGIC ### **Task Configuration**
# MAGIC
# MAGIC ```python
# MAGIC {
# MAGIC   "task_key": "data_ingestion",
# MAGIC   "notebook_task": {
# MAGIC     "notebook_path": "/Users/me/ingest_data",
# MAGIC     "base_parameters": {"date": "2024-01-15"}
# MAGIC   },
# MAGIC   "new_cluster": {...},  # Job cluster
# MAGIC   "timeout_seconds": 3600,
# MAGIC   "max_retries": 2
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### **Cluster Options**
# MAGIC
# MAGIC * **Job Cluster** - Created per run (cheaper)
# MAGIC * **All-Purpose Cluster** - Reuse existing (faster start)
# MAGIC * **Serverless** - Instant startup (easiest)

# COMMAND ----------

# DBTITLE 1,Creating Jobs
# MAGIC %md
# MAGIC ## 🛠️ Creating Jobs
# MAGIC
# MAGIC ### **Method 1: UI (Workflows)**
# MAGIC
# MAGIC 1. Click **Workflows** in sidebar
# MAGIC 2. Click **Create Job**
# MAGIC 3. Configure tasks
# MAGIC 4. Set schedule
# MAGIC 5. Click **Create**
# MAGIC
# MAGIC ### **Method 2: API**
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service import jobs
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC job = w.jobs.create(
# MAGIC     name="Daily ETL Pipeline",
# MAGIC     tasks=[
# MAGIC         jobs.Task(
# MAGIC             task_key="extract",
# MAGIC             notebook_task=jobs.NotebookTask(
# MAGIC                 notebook_path="/ETL/extract"
# MAGIC             ),
# MAGIC             new_cluster=jobs.ClusterSpec(...)
# MAGIC         )
# MAGIC     ],
# MAGIC     schedule=jobs.CronSchedule(
# MAGIC         quartz_cron_expression="0 0 2 * * ?",  # 2 AM daily
# MAGIC         timezone_id="America/Los_Angeles"
# MAGIC     )
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### **Method 3: Databricks CLI**
# MAGIC
# MAGIC ```bash
# MAGIC databricks jobs create --json-file job_config.json
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Task Dependencies
# MAGIC %md
# MAGIC ## 🔗 Task Dependencies
# MAGIC
# MAGIC ### **Linear Pipeline**
# MAGIC ```
# MAGIC Extract → Transform → Load
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC tasks = [
# MAGIC     {"task_key": "extract", ...},
# MAGIC     {"task_key": "transform", 
# MAGIC      "depends_on": [{"task_key": "extract"}]},
# MAGIC     {"task_key": "load", 
# MAGIC      "depends_on": [{"task_key": "transform"}]}
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC ### **Parallel Execution**
# MAGIC ```
# MAGIC         → Process_US
# MAGIC Extract → Process_EU  → Aggregate
# MAGIC         → Process_APAC
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC tasks = [
# MAGIC     {"task_key": "extract", ...},
# MAGIC     {"task_key": "process_us", 
# MAGIC      "depends_on": [{"task_key": "extract"}]},
# MAGIC     {"task_key": "process_eu", 
# MAGIC      "depends_on": [{"task_key": "extract"}]},
# MAGIC     {"task_key": "process_apac", 
# MAGIC      "depends_on": [{"task_key": "extract"}]},
# MAGIC     {"task_key": "aggregate", 
# MAGIC      "depends_on": [
# MAGIC          {"task_key": "process_us"},
# MAGIC          {"task_key": "process_eu"},
# MAGIC          {"task_key": "process_apac"}
# MAGIC      ]}
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC 💡 **Parallel tasks run simultaneously for faster completion**

# COMMAND ----------

# DBTITLE 1,Scheduling
# MAGIC %md
# MAGIC ## ⏰ Scheduling Jobs
# MAGIC
# MAGIC ### **Trigger Types**
# MAGIC
# MAGIC #### **1. Scheduled (Cron)**
# MAGIC ```python
# MAGIC # Daily at 2 AM
# MAGIC "0 0 2 * * ?"
# MAGIC
# MAGIC # Every 6 hours
# MAGIC "0 0 */6 * * ?"
# MAGIC
# MAGIC # Monday-Friday at 9 AM
# MAGIC "0 0 9 ? * MON-FRI"
# MAGIC
# MAGIC # First day of month at midnight
# MAGIC "0 0 0 1 * ?"
# MAGIC ```
# MAGIC
# MAGIC #### **2. Continuous**
# MAGIC * Runs immediately after completion
# MAGIC * Use for streaming workloads
# MAGIC * Specify max concurrent runs
# MAGIC
# MAGIC #### **3. File Arrival**
# MAGIC * Trigger on new files
# MAGIC * Monitor cloud storage
# MAGIC * Integrated with Auto Loader
# MAGIC
# MAGIC #### **4. Manual**
# MAGIC * Run on-demand
# MAGIC * Trigger via API
# MAGIC * For ad-hoc execution
# MAGIC
# MAGIC ### **Cron Expression Format**
# MAGIC ```
# MAGIC ┌───────── second (0-59)
# MAGIC │ ┌─────── minute (0-59)
# MAGIC │ │ ┌───── hour (0-23)
# MAGIC │ │ │ ┌─── day of month (1-31)
# MAGIC │ │ │ │ ┌─ month (1-12)
# MAGIC │ │ │ │ │ ┌─ day of week (0-6)
# MAGIC │ │ │ │ │ │
# MAGIC 0 0 2 * * ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Monitoring & Alerts
# MAGIC %md
# MAGIC ## 📊 Monitoring & Alerts
# MAGIC
# MAGIC ### **Job Run States**
# MAGIC
# MAGIC * **PENDING** - Waiting to start
# MAGIC * **RUNNING** - Currently executing
# MAGIC * **TERMINATING** - Shutting down
# MAGIC * **SUCCESS** - Completed successfully
# MAGIC * **FAILED** - Task failed
# MAGIC * **CANCELED** - Manually stopped
# MAGIC
# MAGIC ### **Alerts Configuration**
# MAGIC
# MAGIC ```python
# MAGIC alerts = [
# MAGIC     {
# MAGIC         "on_failure": [
# MAGIC             {"email_notifications": {"to": ["team@company.com"]}}
# MAGIC         ]
# MAGIC     },
# MAGIC     {
# MAGIC         "on_success": [
# MAGIC             {"webhook_notifications": {"url": "https://..."}}
# MAGIC         ]
# MAGIC     }
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC ### **Monitoring Best Practices**
# MAGIC
# MAGIC ✅ **Set up alerts** for failures
# MAGIC ✅ **Monitor run duration** for anomalies
# MAGIC ✅ **Track task success rates**
# MAGIC ✅ **Review logs** for errors
# MAGIC ✅ **Use tags** for organization
# MAGIC
# MAGIC ### **Job Metrics**
# MAGIC
# MAGIC * Run duration
# MAGIC * Task success/failure rate
# MAGIC * Cluster utilization
# MAGIC * Cost per run

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Databricks Jobs** - Orchestration for data/ML workflows
# MAGIC ✅ **Task Types** - Notebooks, Python, SQL, Pipelines
# MAGIC ✅ **Dependencies** - Linear and parallel execution
# MAGIC ✅ **Scheduling** - Cron, continuous, file arrival
# MAGIC ✅ **Monitoring** - Track runs, set alerts
# MAGIC ✅ **Job Clusters** - Cost-effective compute
# MAGIC
# MAGIC ## 🎯 Best Practices
# MAGIC
# MAGIC ### **Job Design**
# MAGIC * Break work into small, focused tasks
# MAGIC * Use dependencies for proper ordering
# MAGIC * Leverage parallelism where possible
# MAGIC * Set appropriate timeouts and retries
# MAGIC
# MAGIC ### **Cost Optimization**
# MAGIC * Use **job clusters** (not all-purpose)
# MAGIC * Use **Spot instances** for fault-tolerant jobs
# MAGIC * Set **auto-termination** appropriately
# MAGIC * Right-size cluster configurations
# MAGIC
# MAGIC ### **Reliability**
# MAGIC * Configure retries for transient failures
# MAGIC * Set up alerts for critical jobs
# MAGIC * Monitor job run history
# MAGIC * Test jobs before production
# MAGIC
# MAGIC ## 💡 When to Use Jobs
# MAGIC
# MAGIC ✅ **Scheduled ETL** - Daily data pipelines
# MAGIC ✅ **ML Training** - Recurring model updates
# MAGIC ✅ **Report Generation** - Automated reports
# MAGIC ✅ **Data Quality** - Validation checks
# MAGIC ✅ **Multi-step Workflows** - Complex orchestration
# MAGIC
# MAGIC ## ➡️ Next: Module 7.2 - Declarative Automation Bundles
# MAGIC
# MAGIC 🎉 **Module 7.1 Complete!**
