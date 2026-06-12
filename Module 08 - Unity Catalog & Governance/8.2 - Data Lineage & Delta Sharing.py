# Databricks notebook source
# DBTITLE 1,Module 8.2: Lineage & Sharing
# MAGIC %md
# MAGIC # 🔍 Module 8.2: Data Lineage & Delta Sharing
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand data lineage
# MAGIC * Track data dependencies
# MAGIC * Share data securely with Delta Sharing
# MAGIC * Implement cross-organization collaboration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 What is Data Lineage?
# MAGIC
# MAGIC **Data Lineage** tracks the flow of data through transformations from source to destination.
# MAGIC
# MAGIC ### **Key Features**
# MAGIC
# MAGIC ✅ **Automatic Tracking** - No manual configuration
# MAGIC ✅ **Column-Level** - Track individual columns
# MAGIC ✅ **Table-Level** - Track table dependencies
# MAGIC ✅ **Notebook/Job Tracking** - See which code created data
# MAGIC ✅ **Visual Graph** - Interactive lineage visualization
# MAGIC
# MAGIC ### **Why Track Lineage?**
# MAGIC * **Impact Analysis** - Understand downstream effects
# MAGIC * **Root Cause** - Trace data quality issues
# MAGIC * **Compliance** - Document data flow
# MAGIC * **Discovery** - Find related datasets
# MAGIC * **Trust** - Understand data provenance

# COMMAND ----------

# DBTITLE 1,Data Lineage Types
# MAGIC %md
# MAGIC ## 📏 Data Lineage Types
# MAGIC
# MAGIC ### **Table-Level Lineage**
# MAGIC Shows relationships between tables
# MAGIC
# MAGIC ```
# MAGIC raw_orders → cleaned_orders → sales_summary
# MAGIC      │                │
# MAGIC      └────────────────┘
# MAGIC             ↓
# MAGIC        order_metrics
# MAGIC ```
# MAGIC
# MAGIC ### **Column-Level Lineage**
# MAGIC Tracks individual column transformations
# MAGIC
# MAGIC ```
# MAGIC raw_orders.order_date → sales_summary.sales_date
# MAGIC raw_orders.amount → sales_summary.total_revenue
# MAGIC ```
# MAGIC
# MAGIC ### **Notebook/Job Lineage**
# MAGIC Shows which code created/modified data
# MAGIC
# MAGIC ```
# MAGIC Notebook: ETL_Pipeline.ipynb
# MAGIC   │
# MAGIC   └──> Created: main.sales.cleaned_orders
# MAGIC        │
# MAGIC        └──> Used by: Job "Daily Sales Report"
# MAGIC ```
# MAGIC
# MAGIC ### **Viewing Lineage**
# MAGIC
# MAGIC 1. **Data Explorer** - Click table → Lineage tab
# MAGIC 2. **Catalog Explorer** - Visual graph view
# MAGIC 3. **System Tables** - Query lineage programmatically
# MAGIC
# MAGIC ```sql
# MAGIC -- Query lineage from system tables
# MAGIC SELECT * FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name = 'main.sales.summary';
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo - Exploring Lineage
# 🔍 Demo: Data Lineage Query

print("🔍 Data Lineage Exploration")
print("=" * 60)

# Note: This requires access to system.access schema
print("\n📊 Querying Table Lineage")
print("\nExample query structure:")
print("""\nSELECT 
    source_table_full_name,
    target_table_full_name,
    source_type,
    entity_type
FROM system.access.table_lineage
WHERE target_table_full_name LIKE 'main.%'
LIMIT 10;
""")

print("\n📊 Column-Level Lineage")
print("\nExample query:")
print("""\nSELECT 
    source_table_full_name,
    source_column_name,
    target_table_full_name,
    target_column_name
FROM system.access.column_lineage
WHERE target_table_full_name = 'main.sales.summary'
LIMIT 10;
""")

print("\n💡 Lineage Use Cases:")
print("   1. Impact Analysis - What breaks if I change this table?")
print("   2. Root Cause - Where did bad data come from?")
print("   3. Discovery - What tables use this data?")
print("   4. Compliance - Document data flow for auditors")
print("   5. Optimization - Find unused tables")

print("\n📝 Best viewed in Data Explorer UI:")
print("   Data → Select Table → Lineage Tab")

# COMMAND ----------

# DBTITLE 1,Delta Sharing Introduction
# MAGIC %md
# MAGIC ## 🤝 Delta Sharing
# MAGIC
# MAGIC **Delta Sharing** is an open protocol for secure data sharing across organizations.
# MAGIC
# MAGIC ### **Key Features**
# MAGIC
# MAGIC ✅ **Open Standard** - Works with any platform
# MAGIC ✅ **No Data Copy** - Direct access to source
# MAGIC ✅ **Fine-Grained Access** - Table/partition level
# MAGIC ✅ **Audit Trail** - Track all access
# MAGIC ✅ **Live Data** - Always current
# MAGIC ✅ **Cross-Cloud** - AWS, Azure, GCP
# MAGIC
# MAGIC ### **Why Delta Sharing?**
# MAGIC * **Security** - No data copies to secure
# MAGIC * **Performance** - No ETL delays
# MAGIC * **Cost** - No duplicate storage
# MAGIC * **Governance** - Centralized control
# MAGIC * **Simplicity** - Recipients need no Databricks
# MAGIC
# MAGIC ### **Use Cases**
# MAGIC * Share data with partners
# MAGIC * Distribute to subsidiaries
# MAGIC * Provide to customers
# MAGIC * Cross-team collaboration
# MAGIC * Data marketplace

# COMMAND ----------

# DBTITLE 1,Delta Sharing Components
# MAGIC %md
# MAGIC ## 📦 Delta Sharing Components
# MAGIC
# MAGIC ### **Provider (Data Owner)**
# MAGIC 1. **Share** - Collection of tables to share
# MAGIC 2. **Recipient** - Entity receiving access
# MAGIC 3. **Provider** - Data owner and host
# MAGIC
# MAGIC ### **Architecture**
# MAGIC ```
# MAGIC Provider (Databricks)
# MAGIC   │
# MAGIC   ├── Share: sales_data
# MAGIC   │     ├── Table: customers
# MAGIC   │     └── Table: orders
# MAGIC   │
# MAGIC   └── Recipient: partner_company
# MAGIC         ├── Access: sales_data
# MAGIC         └── Credentials: secure_token
# MAGIC
# MAGIC Recipient (Any Platform)
# MAGIC   │
# MAGIC   └── Reads data using Delta Sharing protocol
# MAGIC ```
# MAGIC
# MAGIC ### **Setting Up Sharing (SQL)**
# MAGIC
# MAGIC **1. Create Share**
# MAGIC ```sql
# MAGIC -- Create a share
# MAGIC CREATE SHARE sales_data;
# MAGIC
# MAGIC -- Add tables to share
# MAGIC ALTER SHARE sales_data 
# MAGIC ADD TABLE main.sales.customers;
# MAGIC
# MAGIC ALTER SHARE sales_data 
# MAGIC ADD TABLE main.sales.orders;
# MAGIC ```
# MAGIC
# MAGIC **2. Create Recipient**
# MAGIC ```sql
# MAGIC -- Create recipient
# MAGIC CREATE RECIPIENT partner_company;
# MAGIC
# MAGIC -- Grant access to share
# MAGIC GRANT SELECT ON SHARE sales_data TO RECIPIENT partner_company;
# MAGIC ```
# MAGIC
# MAGIC **3. Get Activation Link**
# MAGIC ```sql
# MAGIC -- Generate credential
# MAGIC DESCRIBE RECIPIENT partner_company;
# MAGIC -- Returns activation link for recipient
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Consuming Shared Data
# MAGIC %md
# MAGIC ## 📥 Consuming Shared Data
# MAGIC
# MAGIC ### **Databricks Recipient**
# MAGIC
# MAGIC **1. Create Foreign Catalog**
# MAGIC ```sql
# MAGIC -- Create catalog from share
# MAGIC CREATE CATALOG partner_data
# MAGIC USING SHARE provider.sales_data;
# MAGIC
# MAGIC -- Query shared data
# MAGIC SELECT * FROM partner_data.sales.customers LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC **2. Use Like Local Tables**
# MAGIC ```sql
# MAGIC -- Join with local data
# MAGIC SELECT 
# MAGIC     c.customer_name,
# MAGIC     COUNT(o.order_id) as order_count
# MAGIC FROM partner_data.sales.customers c
# MAGIC JOIN main.local.orders o ON c.customer_id = o.customer_id
# MAGIC GROUP BY c.customer_name;
# MAGIC ```
# MAGIC
# MAGIC ### **Non-Databricks Recipient**
# MAGIC
# MAGIC **Python (pandas)**
# MAGIC ```python
# MAGIC import delta_sharing
# MAGIC
# MAGIC # Create client
# MAGIC client = delta_sharing.SharingClient(profile_file)
# MAGIC
# MAGIC # List tables
# MAGIC tables = client.list_all_tables()
# MAGIC
# MAGIC # Load as pandas DataFrame
# MAGIC df = delta_sharing.load_as_pandas(
# MAGIC     f"{profile_file}#share_name.schema.table"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Apache Spark**
# MAGIC ```python
# MAGIC # Read shared table
# MAGIC df = spark.read.format("deltaSharing") \
# MAGIC     .load(f"{profile_file}#share.schema.table")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Delta Sharing Best Practices
# MAGIC %md
# MAGIC ## 🎯 Delta Sharing Best Practices
# MAGIC
# MAGIC ### **Provider Best Practices**
# MAGIC
# MAGIC ✅ **Organize shares logically** - By recipient or purpose
# MAGIC ✅ **Document tables** - Clear descriptions
# MAGIC ✅ **Version shared tables** - Track changes
# MAGIC ✅ **Monitor usage** - Review audit logs
# MAGIC ✅ **Set retention** - Define data lifetime
# MAGIC ✅ **Test access** - Verify before distributing
# MAGIC
# MAGIC ### **Security**
# MAGIC
# MAGIC ✅ **Least privilege** - Share minimum necessary
# MAGIC ✅ **Audit access** - Monitor who accesses what
# MAGIC ✅ **Rotate credentials** - Regular updates
# MAGIC ✅ **Revoke when needed** - Remove inactive recipients
# MAGIC ✅ **Use row filters** - Limit data exposure
# MAGIC
# MAGIC ```sql
# MAGIC -- Add row filter to shared table
# MAGIC ALTER SHARE sales_data
# MAGIC ADD TABLE main.sales.orders
# MAGIC PARTITION (date >= '2024-01-01');
# MAGIC ```
# MAGIC
# MAGIC ### **Performance**
# MAGIC
# MAGIC ✅ **Partition tables** - For efficient filtering
# MAGIC ✅ **Optimize regularly** - Better query performance
# MAGIC ✅ **Share aggregates** - Not raw data when possible
# MAGIC ✅ **Use Z-ordering** - For common filters
# MAGIC
# MAGIC ### **Governance**
# MAGIC
# MAGIC ✅ **Track recipients** - Know who has access
# MAGIC ✅ **Document purpose** - Why shared
# MAGIC ✅ **Set expiration** - Time-limited access
# MAGIC ✅ **Regular reviews** - Audit shares quarterly
# MAGIC ✅ **Change management** - Notify of schema changes

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Data Lineage** - Track data flow and dependencies
# MAGIC ✅ **Table-Level** - Understand table relationships
# MAGIC ✅ **Column-Level** - Track individual transformations
# MAGIC ✅ **System Tables** - Query lineage programmatically
# MAGIC ✅ **Delta Sharing** - Secure cross-org data sharing
# MAGIC ✅ **No Data Copy** - Direct access to source
# MAGIC ✅ **Open Protocol** - Works with any platform
# MAGIC
# MAGIC ## 🎯 Key Concepts
# MAGIC
# MAGIC ### **Data Lineage Benefits**
# MAGIC 1. **Impact Analysis** - Understand change effects
# MAGIC 2. **Root Cause Analysis** - Trace data issues
# MAGIC 3. **Compliance** - Document data flow
# MAGIC 4. **Discovery** - Find related datasets
# MAGIC 5. **Optimization** - Identify unused tables
# MAGIC
# MAGIC ### **Delta Sharing Workflow**
# MAGIC ```
# MAGIC Provider:
# MAGIC 1. Create Share
# MAGIC 2. Add Tables to Share  
# MAGIC 3. Create Recipient
# MAGIC 4. Grant Access
# MAGIC 5. Provide Activation Link
# MAGIC
# MAGIC Recipient:
# MAGIC 1. Activate Credentials
# MAGIC 2. Create Foreign Catalog (or use client)
# MAGIC 3. Query Shared Data
# MAGIC ```
# MAGIC
# MAGIC ### **Governance Integration**
# MAGIC * **Audit Logs** - Track all access
# MAGIC * **Row Filters** - Limit data exposure
# MAGIC * **Time-Limited** - Expiring access
# MAGIC * **Version Control** - Track changes
# MAGIC * **Documentation** - Clear descriptions
# MAGIC
# MAGIC ## 💡 Remember
# MAGIC
# MAGIC * **Lineage is automatic** - No configuration needed
# MAGIC * **Use Data Explorer** - Best visualization
# MAGIC * **Query system tables** - Programmatic access
# MAGIC * **Delta Sharing = Open** - Not proprietary
# MAGIC * **No data copies** - Security and cost benefits
# MAGIC * **Test before sharing** - Verify access
# MAGIC * **Monitor usage** - Review audit logs
# MAGIC * **Document shared data** - Clear descriptions
# MAGIC
# MAGIC ## 🎓 Module 08 Complete!
# MAGIC
# MAGIC You've mastered:
# MAGIC * Unity Catalog architecture
# MAGIC * Access control and governance
# MAGIC * Data lineage tracking
# MAGIC * Delta Sharing for collaboration
# MAGIC
# MAGIC ## ➡️ Next: Module 09 - Performance Optimization
# MAGIC
# MAGIC 🎉 **Congratulations on completing Module 08!**
