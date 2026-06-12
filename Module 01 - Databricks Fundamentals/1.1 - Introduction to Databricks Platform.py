# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,📚 Module 1.1: Introduction to Databricks Platform
# MAGIC %md
# MAGIC # 🎓 Module 1.1: Introduction to Databricks Platform
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC By the end of this module, you will understand:
# MAGIC * What Databricks is and why it exists
# MAGIC * The Lakehouse architecture concept
# MAGIC * Key components of the Databricks platform
# MAGIC * Common use cases and benefits
# MAGIC * Databricks in the cloud ecosystem
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 What is Databricks?
# MAGIC
# MAGIC **Databricks** is a unified data analytics platform built on Apache Spark that combines the best of data warehouses and data lakes into a **Lakehouse architecture**.
# MAGIC
# MAGIC ### Key Definition
# MAGIC > Databricks is a cloud-based platform for big data analytics and AI that provides a collaborative workspace for data engineers, data scientists, and business analysts.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏗️ The Lakehouse Architecture
# MAGIC
# MAGIC Traditional data architectures have limitations:
# MAGIC * **Data Lakes**: Good for storing raw data, but lack structure and ACID transactions
# MAGIC * **Data Warehouses**: Structured and reliable, but expensive and not flexible
# MAGIC
# MAGIC **Lakehouse** combines both:
# MAGIC ✅ Low-cost storage (like data lakes)  
# MAGIC ✅ ACID transactions (like data warehouses)  
# MAGIC ✅ Schema enforcement AND schema evolution  
# MAGIC ✅ Direct access for BI tools  
# MAGIC ✅ Support for streaming and batch  
# MAGIC ✅ ML and data science workloads  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Evolution of Data Architectures
# MAGIC
# MAGIC ```
# MAGIC Generation 1: Data Warehouses (2000s)
# MAGIC ├── Structured data only
# MAGIC ├── Expensive storage
# MAGIC └── Limited scalability
# MAGIC
# MAGIC Generation 2: Data Lakes (2010s)
# MAGIC ├── Store all data types
# MAGIC ├── Cheap storage
# MAGIC └── Lack governance & performance
# MAGIC
# MAGIC Generation 3: Lakehouse (2020s)
# MAGIC ├── Best of both worlds
# MAGIC ├── ACID transactions on data lakes
# MAGIC ├── Delta Lake foundation
# MAGIC └── Unified analytics
# MAGIC ```

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Summary & Next Steps
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC In this module, you learned:
# MAGIC
# MAGIC ✅ **What Databricks is**: A unified data analytics platform  
# MAGIC ✅ **Lakehouse Architecture**: Combines data lake flexibility with data warehouse reliability  
# MAGIC ✅ **Key Components**: Workspace, Compute, Delta Lake, Unity Catalog, MLflow  
# MAGIC ✅ **Use Cases**: ETL, BI, ML, Real-time analytics, Governance  
# MAGIC ✅ **Benefits**: Performance, simplicity, cost efficiency, reliability  
# MAGIC ✅ **Multi-Cloud**: AWS, Azure, GCP support  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Key Takeaways
# MAGIC
# MAGIC 1. **Databricks = Apache Spark + Delta Lake + ML + Governance**
# MAGIC 2. **Lakehouse** is the modern data architecture standard
# MAGIC 3. **One platform** for data engineers, data scientists, and analysts
# MAGIC 4. **Cloud-native** with no vendor lock-in
# MAGIC 5. **Production-ready** with enterprise security and scalability
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Resources
# MAGIC
# MAGIC * [Databricks Official Documentation](https://docs.databricks.com)
# MAGIC * [What is a Lakehouse?](https://www.databricks.com/product/data-lakehouse)
# MAGIC * [Delta Lake Documentation](https://docs.delta.io)
# MAGIC * [Databricks Academy](https://www.databricks.com/learn/training)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ➡️ Next Module
# MAGIC
# MAGIC **Module 1.2: Workspace Navigation & UI**  
# MAGIC Learn how to navigate the Databricks workspace and use the interface effectively.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 🎉 **Congratulations!** You've completed Module 1.1

# COMMAND ----------

# DBTITLE 1,Key Benefits & Cloud Support
# MAGIC %md
# MAGIC ## ✨ Key Benefits of Databricks
# MAGIC
# MAGIC ### **Performance**
# MAGIC * **Photon Engine**: 12x faster query performance
# MAGIC * **Liquid Clustering**: Automatic data organization
# MAGIC * **Predictive Optimization**: Auto-tuning without manual intervention
# MAGIC * **Result Caching**: Instant query results
# MAGIC
# MAGIC ### **Simplicity**
# MAGIC * **Unified Platform**: One place for all data workloads
# MAGIC * **Serverless Compute**: No infrastructure management
# MAGIC * **AutoML**: Automated machine learning
# MAGIC * **Built-in Collaboration**: Notebooks, comments, version control
# MAGIC
# MAGIC ### **Cost Efficiency**
# MAGIC * **Autoscaling**: Pay only for what you use
# MAGIC * **Spot Instance Support**: Up to 90% cost savings
# MAGIC * **Query Optimizations**: Reduced compute time
# MAGIC * **Storage Optimization**: Delta Lake compression
# MAGIC
# MAGIC ### **Reliability**
# MAGIC * **ACID Transactions**: Data consistency guaranteed
# MAGIC * **Time Travel**: Query historical data versions
# MAGIC * **Schema Evolution**: Handle changing data structures
# MAGIC * **Fault Tolerance**: Built-in resilience
# MAGIC
# MAGIC ### **Security & Governance**
# MAGIC * **Unity Catalog**: Centralized governance
# MAGIC * **Fine-Grained Access Control**: Row/column level security
# MAGIC * **Audit Logging**: Complete activity tracking
# MAGIC * **Compliance**: SOC2, HIPAA, GDPR ready
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ☁️ Multi-Cloud Support
# MAGIC
# MAGIC Databricks runs on all major cloud providers:
# MAGIC
# MAGIC ### **AWS (Amazon Web Services)**
# MAGIC * S3 integration
# MAGIC * Glue catalog support
# MAGIC * Native Kinesis/MSK support
# MAGIC
# MAGIC ### **Azure (Microsoft Azure)**
# MAGIC * ADLS Gen2 integration
# MAGIC * Azure Active Directory
# MAGIC * Event Hubs support
# MAGIC
# MAGIC ### **GCP (Google Cloud Platform)**
# MAGIC * GCS integration
# MAGIC * BigQuery federation
# MAGIC * Pub/Sub support
# MAGIC
# MAGIC 🌐 **Same platform, same features across all clouds!**

# COMMAND ----------

# DBTITLE 1,Quick Demo - Your First Databricks Code
# 🚀 Your First Databricks Code!
# Let's verify Spark is available and working

# SparkSession is pre-configured in Databricks
print(f"✅ Spark Version: {spark.version}")
print(f"✅ Application Name: {spark.sparkContext.appName}")
print(f"✅ Spark UI: Available in the notebook UI")

# Create a simple DataFrame
data = [
    ("Data Engineering", "ETL & Pipelines"),
    ("Data Science", "ML & Analytics"),
    ("Data Analytics", "BI & Reporting")
]

df = spark.createDataFrame(data, ["Domain", "Focus"])

print("\n📊 Sample DataFrame:")
df.show()

# Display for better visualization
print("\n🔥 Using display() for rich output:")
display(df)

# COMMAND ----------

# DBTITLE 1,Common Use Cases
# MAGIC %md
# MAGIC ## 🎯 Common Use Cases
# MAGIC
# MAGIC ### 1. **ETL & Data Engineering**
# MAGIC * Ingest data from various sources
# MAGIC * Transform and cleanse data
# MAGIC * Build medallion architecture (Bronze → Silver → Gold)
# MAGIC * Incremental processing with Auto Loader
# MAGIC
# MAGIC ### 2. **Business Intelligence & Analytics**
# MAGIC * Interactive SQL queries
# MAGIC * Real-time dashboards
# MAGIC * Ad-hoc analysis
# MAGIC * Self-service analytics for business users
# MAGIC
# MAGIC ### 3. **Machine Learning & AI**
# MAGIC * Feature engineering at scale
# MAGIC * Model training with distributed compute
# MAGIC * Experiment tracking with MLflow
# MAGIC * Model deployment and serving
# MAGIC * GenAI and LLM applications
# MAGIC
# MAGIC ### 4. **Real-Time Analytics**
# MAGIC * Streaming data ingestion
# MAGIC * Real-time transformations
# MAGIC * Event processing
# MAGIC * IoT data analytics
# MAGIC
# MAGIC ### 5. **Data Science**
# MAGIC * Exploratory data analysis (EDA)
# MAGIC * Statistical modeling
# MAGIC * Collaborative notebooks
# MAGIC * Reproducible research
# MAGIC
# MAGIC ### 6. **Data Governance**
# MAGIC * Centralized access control
# MAGIC * Data lineage tracking
# MAGIC * Audit logging
# MAGIC * PII detection and masking

# COMMAND ----------

# DBTITLE 1,Key Platform Components
# MAGIC %md
# MAGIC ## 🛠️ Key Components of Databricks Platform
# MAGIC
# MAGIC ### 1. **Workspace**
# MAGIC * Collaborative environment for notebooks, dashboards, and queries
# MAGIC * Folders and access control
# MAGIC * Git integration
# MAGIC
# MAGIC ### 2. **Compute (Clusters)**
# MAGIC * **All-Purpose Clusters**: Interactive development and ad-hoc analysis
# MAGIC * **Job Clusters**: Automated, production workloads
# MAGIC * **SQL Warehouses**: Optimized for BI and SQL queries
# MAGIC * **Serverless Compute**: Auto-scaling, no cluster management
# MAGIC
# MAGIC ### 3. **Data Management**
# MAGIC * **Delta Lake**: ACID transactions on data lakes
# MAGIC * **Unity Catalog**: Centralized governance and metadata
# MAGIC * **DBFS**: Databricks File System
# MAGIC * **Volumes**: Modern file storage in Unity Catalog
# MAGIC
# MAGIC ### 4. **Development Tools**
# MAGIC * **Notebooks**: Interactive Python, SQL, Scala, R
# MAGIC * **SQL Editor**: For analysts and BI users
# MAGIC * **Repos**: Git integration for version control
# MAGIC * **Databricks CLI**: Command-line interface
# MAGIC
# MAGIC ### 5. **Machine Learning**
# MAGIC * **MLflow**: Experiment tracking and model management
# MAGIC * **AutoML**: Automated machine learning
# MAGIC * **Feature Store**: Centralized feature management
# MAGIC * **Model Serving**: Real-time inference endpoints
# MAGIC
# MAGIC ### 6. **Workflows & Orchestration**
# MAGIC * **Jobs**: Schedule and orchestrate workflows
# MAGIC * **Spark Declarative Pipelines (SDP)**: Streaming ETL
# MAGIC * **Task orchestration**: Multi-task workflows
