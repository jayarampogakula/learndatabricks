# Databricks notebook source
# DBTITLE 1,Module 9.2: Photon & Cost
# MAGIC %md
# MAGIC # 🚀 Module 9.2: Photon Engine & Cost Optimization
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand Photon Engine
# MAGIC * Optimize compute costs
# MAGIC * Choose right cluster types
# MAGIC * Implement cost monitoring
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 What is Photon?
# MAGIC
# MAGIC **Photon** is Databricks' next-generation query engine written in C++ for vectorized execution.
# MAGIC
# MAGIC ### **Key Features**
# MAGIC
# MAGIC ✅ **Faster Queries** - 2-10x speedup
# MAGIC ✅ **Lower Costs** - Same work, less time
# MAGIC ✅ **C++ Native** - Vectorized execution
# MAGIC ✅ **No Code Changes** - Drop-in replacement
# MAGIC ✅ **Automatic** - On by default for SQL
# MAGIC
# MAGIC ### **Performance Improvements**
# MAGIC * **Data Ingestion** - 2-4x faster
# MAGIC * **ETL Workloads** - 2-3x faster
# MAGIC * **Analytics Queries** - 3-10x faster
# MAGIC * **Joins & Aggregations** - Significant speedup
# MAGIC
# MAGIC ### **Why Photon?**
# MAGIC * **Traditional Spark** - JVM-based, row-oriented
# MAGIC * **Photon** - C++, vectorized, CPU-optimized
# MAGIC * **Result** - Better hardware utilization

# COMMAND ----------

# DBTITLE 1,Photon Architecture
# MAGIC %md
# MAGIC ## 🏛️ Photon Architecture
# MAGIC
# MAGIC ### **How Photon Works**
# MAGIC
# MAGIC ```
# MAGIC Query → Catalyst Optimizer → Photon Engine → Results
# MAGIC           │
# MAGIC           └──> Vectorized execution
# MAGIC                SIMD instructions
# MAGIC                Cache-friendly
# MAGIC ```
# MAGIC
# MAGIC ### **Vectorized Execution**
# MAGIC
# MAGIC **Traditional (Row-by-Row):**
# MAGIC ```
# MAGIC for each row:
# MAGIC     process row[0]
# MAGIC     process row[1]
# MAGIC     process row[2]
# MAGIC ```
# MAGIC
# MAGIC **Photon (Vectorized):**
# MAGIC ```
# MAGIC for each batch of 1000 rows:
# MAGIC     process all rows in parallel
# MAGIC     use SIMD instructions
# MAGIC ```
# MAGIC
# MAGIC ### **When Photon Helps Most**
# MAGIC
# MAGIC ✅ **Scans** - Reading Parquet/Delta files
# MAGIC ✅ **Filters** - WHERE clauses
# MAGIC ✅ **Aggregations** - GROUP BY, SUM, COUNT
# MAGIC ✅ **Joins** - Especially hash joins
# MAGIC ✅ **String Operations** - LIKE, regex
# MAGIC ✅ **Arithmetic** - Mathematical operations
# MAGIC
# MAGIC ### **Enabling Photon**
# MAGIC
# MAGIC **SQL Warehouses:**
# MAGIC * Photon enabled by default
# MAGIC * No configuration needed
# MAGIC
# MAGIC **All-Purpose Clusters:**
# MAGIC ```python
# MAGIC # Cluster configuration
# MAGIC {
# MAGIC     "runtime_engine": "PHOTON",
# MAGIC     "spark_version": "13.3.x-photon-scala2.12"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Check if Photon is Active:**
# MAGIC ```python
# MAGIC # Check cluster config
# MAGIC print(spark.conf.get("spark.databricks.photon.enabled"))
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Compute Types & Costs
# MAGIC %md
# MAGIC ## 💰 Compute Types & Cost Optimization
# MAGIC
# MAGIC ### **Compute Options**
# MAGIC
# MAGIC #### **1. SQL Warehouses (Recommended)**
# MAGIC * **Best for**: SQL queries, BI dashboards
# MAGIC * **Photon**: Enabled by default
# MAGIC * **Auto-scaling**: Built-in
# MAGIC * **Serverless**: Instant startup
# MAGIC * **Cost**: Most predictable
# MAGIC
# MAGIC #### **2. All-Purpose Clusters**
# MAGIC * **Best for**: Notebooks, development
# MAGIC * **Photon**: Optional (add "-photon" to runtime)
# MAGIC * **Flexibility**: Full control
# MAGIC * **Cost**: Higher for long-running
# MAGIC
# MAGIC #### **3. Job Clusters**
# MAGIC * **Best for**: Scheduled jobs
# MAGIC * **Lifecycle**: Created per job run
# MAGIC * **Cost**: Most economical
# MAGIC * **Auto-terminate**: After job completion
# MAGIC
# MAGIC ### **Cost Comparison**
# MAGIC
# MAGIC | Compute Type | Use Case | Relative Cost | Auto-Terminate |
# MAGIC |--------------|----------|---------------|----------------|
# MAGIC | **SQL Warehouse** | Interactive SQL | $$ | Auto |
# MAGIC | **All-Purpose** | Development | $$$ | Manual |
# MAGIC | **Job Cluster** | Production Jobs | $ | Automatic |
# MAGIC | **Serverless** | Ad-hoc queries | $$ | Instant |
# MAGIC
# MAGIC ### **Right-Sizing Clusters**
# MAGIC
# MAGIC ✅ **Start small** - Scale up if needed
# MAGIC ✅ **Monitor utilization** - CPU, memory usage
# MAGIC ✅ **Use autoscaling** - Match workload demand
# MAGIC ✅ **Job clusters** - For scheduled work
# MAGIC ✅ **Spot instances** - 60-90% savings (fault-tolerant)

# COMMAND ----------

# DBTITLE 1,Cost Optimization Strategies
# MAGIC %md
# MAGIC ## 🎯 Cost Optimization Strategies
# MAGIC
# MAGIC ### **1. Cluster Selection**
# MAGIC
# MAGIC ✅ **Use SQL Warehouses** for SQL/BI
# MAGIC ```sql
# MAGIC -- Automatically uses Photon
# MAGIC -- Auto-scaling built-in
# MAGIC -- Pay only for active time
# MAGIC ```
# MAGIC
# MAGIC ✅ **Use Job Clusters** for automation
# MAGIC ```python
# MAGIC # Created per job, auto-terminates
# MAGIC # Most cost-effective for scheduled work
# MAGIC ```
# MAGIC
# MAGIC ✅ **Right-size clusters**
# MAGIC ```python
# MAGIC # Don't over-provision
# MAGIC # Monitor: Ganglia metrics, Spark UI
# MAGIC ```
# MAGIC
# MAGIC ### **2. Spot Instances**
# MAGIC Up to 90% cost savings
# MAGIC
# MAGIC ```python
# MAGIC # Cluster config
# MAGIC {
# MAGIC     "aws_attributes": {
# MAGIC         "first_on_demand": 1,  # Driver always on-demand
# MAGIC         "availability": "SPOT_WITH_FALLBACK"
# MAGIC     }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **When to use Spot:**
# MAGIC ✅ Fault-tolerant jobs
# MAGIC ✅ Delta Lake (built-in retry)
# MAGIC ✅ Long-running ETL
# MAGIC
# MAGIC **When NOT to use Spot:**
# MAGIC ❌ Interactive workloads
# MAGIC ❌ Time-sensitive jobs
# MAGIC ❌ Small/short jobs
# MAGIC
# MAGIC ### **3. Auto-Termination**
# MAGIC
# MAGIC ```python
# MAGIC # All-purpose cluster
# MAGIC {
# MAGIC     "autotermination_minutes": 15  # Terminate after 15min idle
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### **4. Cluster Pools**
# MAGIC Pre-warmed VMs for faster startup
# MAGIC
# MAGIC ```python
# MAGIC # Create pool
# MAGIC {
# MAGIC     "instance_pool_id": "...",
# MAGIC     "min_idle_instances": 0,
# MAGIC     "max_capacity": 10
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### **5. Photon Acceleration**
# MAGIC ✅ **Enable Photon** - 2-10x faster = lower costs
# MAGIC ✅ **Serverless** - Photon + auto-scaling
# MAGIC ✅ **SQL Warehouses** - Photon by default

# COMMAND ----------

# DBTITLE 1,Monitoring Costs
# MAGIC %md
# MAGIC ## 📊 Cost Monitoring
# MAGIC
# MAGIC ### **System Tables**
# MAGIC Query historical usage
# MAGIC
# MAGIC ```sql
# MAGIC -- Cluster usage by user
# MAGIC SELECT 
# MAGIC     user_name,
# MAGIC     SUM(usage_quantity) as total_dbus,
# MAGIC     SUM(usage_quantity * list_price) as estimated_cost
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
# MAGIC GROUP BY user_name
# MAGIC ORDER BY total_dbus DESC;
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- Usage by workspace
# MAGIC SELECT 
# MAGIC     workspace_id,
# MAGIC     sku_name,
# MAGIC     SUM(usage_quantity) as dbus,
# MAGIC     usage_date
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY workspace_id, sku_name, usage_date
# MAGIC ORDER BY usage_date DESC, dbus DESC;
# MAGIC ```
# MAGIC
# MAGIC ### **Budget Alerts**
# MAGIC
# MAGIC 1. **Account Console** → Settings → Budget Alerts
# MAGIC 2. Set monthly budget threshold
# MAGIC 3. Configure email notifications
# MAGIC
# MAGIC ### **Tagging**
# MAGIC Organize costs by team/project
# MAGIC
# MAGIC ```python
# MAGIC # Cluster tags
# MAGIC {
# MAGIC     "custom_tags": {
# MAGIC         "team": "data-engineering",
# MAGIC         "project": "sales-pipeline",
# MAGIC         "environment": "production"
# MAGIC     }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### **Cost Attribution**
# MAGIC
# MAGIC ```sql
# MAGIC -- Cost by tag
# MAGIC SELECT 
# MAGIC     custom_tags['project'] as project,
# MAGIC     SUM(usage_quantity) as total_dbus
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
# MAGIC GROUP BY custom_tags['project']
# MAGIC ORDER BY total_dbus DESC;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Best Practices
# MAGIC %md
# MAGIC ## 🎯 Cost Optimization Best Practices
# MAGIC
# MAGIC ### **Compute Selection**
# MAGIC
# MAGIC ✅ **SQL Warehouses** for SQL/BI (Photon default)
# MAGIC ✅ **Job Clusters** for scheduled workflows
# MAGIC ✅ **All-Purpose** only for interactive development
# MAGIC ✅ **Serverless** for ad-hoc, unpredictable workloads
# MAGIC
# MAGIC ### **Cluster Configuration**
# MAGIC
# MAGIC ✅ **Enable Photon** - 2-10x faster = lower costs
# MAGIC ✅ **Enable auto-scaling** - Match demand
# MAGIC ✅ **Set auto-termination** - 15-30 minutes
# MAGIC ✅ **Use Spot instances** - For fault-tolerant jobs
# MAGIC ✅ **Right-size** - Start small, scale if needed
# MAGIC
# MAGIC ### **Query Optimization**
# MAGIC
# MAGIC ✅ **Filter early** - Reduce data scanned
# MAGIC ✅ **Select specific columns** - Not SELECT *
# MAGIC ✅ **Use partitioning** - Skip irrelevant data
# MAGIC ✅ **Apply Z-Ordering** - Better data layout
# MAGIC ✅ **Cache hot data** - Reduce recomputation
# MAGIC
# MAGIC ### **Data Management**
# MAGIC
# MAGIC ✅ **OPTIMIZE regularly** - Compact small files
# MAGIC ✅ **VACUUM old files** - Reduce storage costs
# MAGIC ✅ **Use Delta Lake** - Better compression
# MAGIC ✅ **Archive cold data** - Move to cheaper storage
# MAGIC
# MAGIC ### **Monitoring**
# MAGIC
# MAGIC ✅ **Query system tables** - Track usage
# MAGIC ✅ **Set budget alerts** - Avoid surprises
# MAGIC ✅ **Tag resources** - Cost attribution
# MAGIC ✅ **Review regularly** - Monthly cost review
# MAGIC ✅ **Identify waste** - Idle clusters, unused data
# MAGIC
# MAGIC ### **Development Practices**
# MAGIC
# MAGIC ✅ **Test on samples** - Not full datasets
# MAGIC ✅ **Limit development clusters** - One per user
# MAGIC ✅ **Use notebooks** - Share compute
# MAGIC ✅ **Schedule off-hours** - Lower demand periods
# MAGIC ✅ **Clean up** - Delete unused clusters/tables

# COMMAND ----------

# DBTITLE 1,Demo - Cost Analysis
# 📊 Demo: Cost Analysis

print("💰 Cost Optimization Demo")
print("=" * 60)

print("\n📊 Sample Cost Analysis Queries:")
print("\n1. Usage by Cluster Type:")
print("""\nSELECT 
    sku_name,
    SUM(usage_quantity) as total_dbus,
    COUNT(DISTINCT usage_date) as days_used
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY sku_name
ORDER BY total_dbus DESC;
""")

print("\n2. Top Cost Users:")
print("""\nSELECT 
    user_name,
    SUM(usage_quantity) as total_dbus,
    COUNT(DISTINCT cluster_id) as num_clusters
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY user_name
ORDER BY total_dbus DESC
LIMIT 10;
""")

print("\n3. Daily Cost Trend:")
print("""\nSELECT 
    usage_date,
    SUM(usage_quantity) as daily_dbus
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY usage_date
ORDER BY usage_date;
""")

print("\n💡 Cost Optimization Tips:")
print("   1. Use SQL Warehouses with Photon for SQL workloads")
print("   2. Enable auto-termination (15-30 min)")
print("   3. Use job clusters for scheduled work")
print("   4. Enable Spot instances for fault-tolerant jobs")
print("   5. Right-size clusters - start small")
print("   6. Set budget alerts")
print("   7. Tag resources for cost attribution")
print("   8. OPTIMIZE tables regularly")
print("   9. Review usage monthly")
print("   10. Delete unused resources")

print("\n✅ Key Metric: DBU (Databricks Unit)")
print("   • Unit of compute consumption")
print("   • Pricing varies by workload type")
print("   • Monitor with system.billing.usage")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Photon** - 2-10x faster queries, lower costs
# MAGIC ✅ **SQL Warehouses** - Best for SQL/BI, Photon default
# MAGIC ✅ **Job Clusters** - Most economical for scheduled work
# MAGIC ✅ **Spot Instances** - 60-90% savings for fault-tolerant jobs
# MAGIC ✅ **Auto-Termination** - Stop idle clusters
# MAGIC ✅ **System Tables** - Monitor usage and costs
# MAGIC ✅ **Budget Alerts** - Avoid cost surprises
# MAGIC
# MAGIC ## 🎯 Key Concepts
# MAGIC
# MAGIC ### **Photon Benefits**
# MAGIC * **2-10x faster** queries
# MAGIC * **C++ vectorized** execution
# MAGIC * **No code changes** required
# MAGIC * **Default on** SQL Warehouses
# MAGIC * **Lower costs** through efficiency
# MAGIC
# MAGIC ### **Cost Hierarchy** (Cheapest to Most Expensive)
# MAGIC ```
# MAGIC 1. Job Cluster (created per run)
# MAGIC 2. SQL Warehouse with Serverless
# MAGIC 3. All-Purpose Cluster
# MAGIC ```
# MAGIC
# MAGIC ### **Optimization Checklist**
# MAGIC ☑️ Enable Photon
# MAGIC ☑️ Use SQL Warehouses for SQL
# MAGIC ☑️ Use Job Clusters for automation
# MAGIC ☑️ Enable auto-termination
# MAGIC ☑️ Consider Spot instances
# MAGIC ☑️ Right-size clusters
# MAGIC ☑️ Set budget alerts
# MAGIC ☑️ Tag resources
# MAGIC ☑️ Monitor usage
# MAGIC ☑️ Clean up unused resources
# MAGIC
# MAGIC ## 💡 Remember
# MAGIC
# MAGIC * **Photon is free** - Just faster execution
# MAGIC * **SQL Warehouses** have Photon by default
# MAGIC * **Job clusters** are cheapest for scheduled work
# MAGIC * **Spot instances** save 60-90% (use for fault-tolerant)
# MAGIC * **Auto-terminate** clusters after 15-30 min idle
# MAGIC * **Monitor** with system.billing.usage
# MAGIC * **Tag** everything for cost attribution
# MAGIC * **OPTIMIZE** tables to reduce scan costs
# MAGIC * **Test on samples** during development
# MAGIC
# MAGIC ## 🎓 Module 09 Complete!
# MAGIC
# MAGIC You've mastered:
# MAGIC * Query optimization and Z-Ordering
# MAGIC * Partitioning strategies
# MAGIC * Photon Engine architecture
# MAGIC * Cost optimization techniques
# MAGIC * Resource monitoring
# MAGIC
# MAGIC ## ➡️ Next: Module 10 - Streaming & Real-Time
# MAGIC
# MAGIC 🎉 **Congratulations on completing Module 09!**
