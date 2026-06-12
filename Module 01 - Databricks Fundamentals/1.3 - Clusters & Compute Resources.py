# Databricks notebook source
# DBTITLE 1,⚡ Module 1.3: Clusters & Compute
# MAGIC %md
# MAGIC # ⚡ Module 1.3: Clusters & Compute Resources
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand different compute types
# MAGIC * Learn cluster configurations
# MAGIC * Know when to use each compute option
# MAGIC * Understand autoscaling and cost optimization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🖥️ Compute Types in Databricks
# MAGIC
# MAGIC ### **1. Serverless Compute** ⭐ (Recommended)
# MAGIC * ✅ **No cluster management**
# MAGIC * ✅ **Instant startup**
# MAGIC * ✅ **Auto-scales automatically**
# MAGIC * ✅ **Pay per second**
# MAGIC * 👉 **Best for**: Notebooks, SQL queries, immediate workloads
# MAGIC
# MAGIC ### **2. All-Purpose Clusters**
# MAGIC * 👥 **Interactive development**
# MAGIC * 🔄 **Shared among users**
# MAGIC * ⏸️ **Auto-terminates** after inactivity
# MAGIC * 💰 **Higher cost**
# MAGIC * 👉 **Best for**: Exploration, development
# MAGIC
# MAGIC ### **3. Job Clusters**
# MAGIC * 🤖 **Automated workloads**
# MAGIC * 🛡️ **Isolated execution**
# MAGIC * 💰 **Cost-effective**
# MAGIC * ⏹️ **Auto-terminates** after job
# MAGIC * 👉 **Best for**: Production ETL
# MAGIC
# MAGIC ### **4. SQL Warehouses**
# MAGIC * 📊 **SQL-optimized**
# MAGIC * 🚀 **Photon engine** - 12x faster
# MAGIC * 👥 **Serverless or Classic**
# MAGIC * 👉 **Best for**: BI dashboards, analytics
