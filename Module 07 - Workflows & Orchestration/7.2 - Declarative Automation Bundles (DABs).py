# Databricks notebook source
# DBTITLE 1,Module 7.2: DABs
# MAGIC %md
# MAGIC # 📦 Module 7.2: Declarative Automation Bundles (DABs)
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand Declarative Automation Bundles
# MAGIC * Create DAB projects
# MAGIC * Deploy with CI/CD
# MAGIC * Manage multiple environments
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📦 What are Declarative Automation Bundles?
# MAGIC
# MAGIC **Declarative Automation Bundles (DABs)** enable version-controlled, programmatic deployment of Databricks resources.
# MAGIC
# MAGIC ### **Key Features**
# MAGIC
# MAGIC ✅ **Infrastructure as Code** - YAML configuration
# MAGIC ✅ **Version Control** - Git integration
# MAGIC ✅ **Multi-Environment** - Dev, Staging, Prod
# MAGIC ✅ **CI/CD Ready** - Automated deployment
# MAGIC ✅ **Resource Management** - Jobs, pipelines, models
# MAGIC
# MAGIC ### **Why Use DABs?**
# MAGIC * **Reproducibility** - Consistent deployments
# MAGIC * **Collaboration** - Team development
# MAGIC * **Testing** - Validate before production
# MAGIC * **Rollback** - Git-based versioning
# MAGIC * **Best Practices** - Production-ready patterns

# COMMAND ----------

# DBTITLE 1,Bundle Structure
# MAGIC %md
# MAGIC ## 📁 Bundle Structure
# MAGIC
# MAGIC ### **Project Layout**
# MAGIC ```
# MAGIC my-project/
# MAGIC ├── databricks.yml          # Main config
# MAGIC ├── resources/
# MAGIC │   ├── jobs.yml            # Job definitions
# MAGIC │   └── pipelines.yml       # Pipeline definitions
# MAGIC ├── src/
# MAGIC │   ├── etl/
# MAGIC │   │   ├── extract.py
# MAGIC │   │   ├── transform.py
# MAGIC │   │   └── load.py
# MAGIC │   └── notebooks/
# MAGIC │       └── analysis.py
# MAGIC └── tests/
# MAGIC     └── test_etl.py
# MAGIC ```
# MAGIC
# MAGIC ### **databricks.yml**
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: my_project
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     workspace:
# MAGIC       host: https://abc.cloud.databricks.com
# MAGIC     
# MAGIC   prod:
# MAGIC     workspace:
# MAGIC       host: https://xyz.cloud.databricks.com
# MAGIC ```
# MAGIC
# MAGIC ### **Supported Resources**
# MAGIC * Jobs
# MAGIC * Pipelines (DLT)
# MAGIC * ML Models
# MAGIC * Notebooks
# MAGIC * Experiments

# COMMAND ----------

# DBTITLE 1,Creating a Bundle
# MAGIC %md
# MAGIC ## 🛠️ Creating a Bundle
# MAGIC
# MAGIC ### **Step 1: Initialize**
# MAGIC ```bash
# MAGIC # Create new bundle from template
# MAGIC databricks bundle init
# MAGIC
# MAGIC # Choose template:
# MAGIC # - default-python
# MAGIC # - default-sql
# MAGIC # - mlops-stacks
# MAGIC ```
# MAGIC
# MAGIC ### **Step 2: Define Resources**
# MAGIC
# MAGIC **resources/jobs.yml:**
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     daily_etl:
# MAGIC       name: Daily ETL Pipeline
# MAGIC       
# MAGIC       tasks:
# MAGIC         - task_key: extract
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../src/notebooks/extract
# MAGIC           
# MAGIC           new_cluster:
# MAGIC             num_workers: 2
# MAGIC             spark_version: 13.3.x-scala2.12
# MAGIC             node_type_id: i3.xlarge
# MAGIC         
# MAGIC         - task_key: transform
# MAGIC           depends_on:
# MAGIC             - task_key: extract
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../src/notebooks/transform
# MAGIC       
# MAGIC       schedule:
# MAGIC         quartz_cron_expression: "0 0 2 * * ?"
# MAGIC         timezone_id: "America/Los_Angeles"
# MAGIC ```
# MAGIC
# MAGIC ### **Step 3: Validate**
# MAGIC ```bash
# MAGIC databricks bundle validate
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Deployment
# MAGIC %md
# MAGIC ## 🚀 Deployment
# MAGIC
# MAGIC ### **Deploy to Environment**
# MAGIC
# MAGIC ```bash
# MAGIC # Deploy to dev
# MAGIC databricks bundle deploy -t dev
# MAGIC
# MAGIC # Deploy to prod
# MAGIC databricks bundle deploy -t prod
# MAGIC ```
# MAGIC
# MAGIC ### **What Happens During Deploy**
# MAGIC
# MAGIC 1. **Validate** - Check configuration
# MAGIC 2. **Upload** - Sync files to workspace
# MAGIC 3. **Create/Update** - Apply resource changes
# MAGIC 4. **Report** - Show deployment summary
# MAGIC
# MAGIC ### **Run Resources**
# MAGIC
# MAGIC ```bash
# MAGIC # Run a job
# MAGIC databricks bundle run daily_etl -t prod
# MAGIC
# MAGIC # Run with parameters
# MAGIC databricks bundle run daily_etl -t prod \
# MAGIC   --params '{"date": "2024-01-15"}'
# MAGIC ```
# MAGIC
# MAGIC ### **Destroy Resources**
# MAGIC
# MAGIC ```bash
# MAGIC # Remove all resources (careful!)
# MAGIC databricks bundle destroy -t dev
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Multi-Environment
# MAGIC %md
# MAGIC ## 🌍 Multi-Environment Setup
# MAGIC
# MAGIC ### **Environment Configuration**
# MAGIC
# MAGIC ```yaml
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development
# MAGIC     workspace:
# MAGIC       host: https://dev.databricks.com
# MAGIC     variables:
# MAGIC       catalog: dev_catalog
# MAGIC       schema: dev_schema
# MAGIC   
# MAGIC   staging:
# MAGIC     mode: production
# MAGIC     workspace:
# MAGIC       host: https://staging.databricks.com
# MAGIC     variables:
# MAGIC       catalog: staging_catalog
# MAGIC       schema: staging_schema
# MAGIC   
# MAGIC   prod:
# MAGIC     mode: production
# MAGIC     workspace:
# MAGIC       host: https://prod.databricks.com
# MAGIC     variables:
# MAGIC       catalog: prod_catalog
# MAGIC       schema: prod_schema
# MAGIC ```
# MAGIC
# MAGIC ### **Using Variables**
# MAGIC
# MAGIC **In Job Config:**
# MAGIC ```yaml
# MAGIC parameters:
# MAGIC   catalog: ${var.catalog}
# MAGIC   schema: ${var.schema}
# MAGIC ```
# MAGIC
# MAGIC **In Notebook:**
# MAGIC ```python
# MAGIC catalog = dbutils.widgets.get("catalog")
# MAGIC schema = dbutils.widgets.get("schema")
# MAGIC
# MAGIC spark.sql(f"USE {catalog}.{schema}")
# MAGIC ```
# MAGIC
# MAGIC ### **Environment Differences**
# MAGIC * **Dev** - Small clusters, fast iteration
# MAGIC * **Staging** - Production-like testing
# MAGIC * **Prod** - Full scale, monitoring

# COMMAND ----------

# DBTITLE 1,CI/CD Integration
# MAGIC %md
# MAGIC ## 🔄 CI/CD Integration
# MAGIC
# MAGIC ### **GitHub Actions Example**
# MAGIC
# MAGIC ```yaml
# MAGIC name: Deploy Bundle
# MAGIC
# MAGIC on:
# MAGIC   push:
# MAGIC     branches: [main]
# MAGIC
# MAGIC jobs:
# MAGIC   deploy:
# MAGIC     runs-on: ubuntu-latest
# MAGIC     
# MAGIC     steps:
# MAGIC       - uses: actions/checkout@v3
# MAGIC       
# MAGIC       - name: Setup Databricks CLI
# MAGIC         run: |
# MAGIC           curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
# MAGIC       
# MAGIC       - name: Validate Bundle
# MAGIC         run: databricks bundle validate -t prod
# MAGIC         env:
# MAGIC           DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
# MAGIC           DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
# MAGIC       
# MAGIC       - name: Deploy Bundle
# MAGIC         run: databricks bundle deploy -t prod
# MAGIC         env:
# MAGIC           DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
# MAGIC           DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
# MAGIC ```
# MAGIC
# MAGIC ### **Deployment Strategy**
# MAGIC
# MAGIC 1. **PR to Dev** → Auto-deploy to dev
# MAGIC 2. **PR Review** → Manual testing
# MAGIC 3. **Merge to Main** → Auto-deploy to staging
# MAGIC 4. **Release Tag** → Manual deploy to prod
# MAGIC
# MAGIC ### **Best Practices**
# MAGIC ✅ Run tests before deployment
# MAGIC ✅ Use separate tokens per environment
# MAGIC ✅ Enable deployment approvals for prod
# MAGIC ✅ Tag releases in Git

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **DABs** - Infrastructure as code for Databricks
# MAGIC ✅ **YAML Configuration** - Declarative resource definitions
# MAGIC ✅ **Multi-Environment** - Dev, Staging, Prod
# MAGIC ✅ **CI/CD Ready** - Automated deployments
# MAGIC ✅ **Version Control** - Git-based workflow
# MAGIC ✅ **Databricks CLI** - Deploy, run, destroy
# MAGIC
# MAGIC ## 🎯 Best Practices
# MAGIC
# MAGIC ### **Project Organization**
# MAGIC * Separate code from config
# MAGIC * Use clear directory structure
# MAGIC * Version control everything
# MAGIC * Document environment setup
# MAGIC
# MAGIC ### **Configuration Management**
# MAGIC * Use variables for environment differences
# MAGIC * Keep secrets in CI/CD vault
# MAGIC * Validate before deploying
# MAGIC * Test in lower environments first
# MAGIC
# MAGIC ### **Deployment**
# MAGIC * Automate with CI/CD
# MAGIC * Use staging environment
# MAGIC * Enable manual prod approval
# MAGIC * Monitor deployments
# MAGIC * Plan rollback strategy
# MAGIC
# MAGIC ## 💡 When to Use DABs
# MAGIC
# MAGIC ✅ **Production Workflows** - Reliable deployments
# MAGIC ✅ **Team Projects** - Collaboration
# MAGIC ✅ **Multi-Environment** - Dev/Staging/Prod
# MAGIC ✅ **CI/CD Pipelines** - Automated testing
# MAGIC ✅ **Complex Projects** - Many resources
# MAGIC
# MAGIC ## 📚 Resources
# MAGIC
# MAGIC * [DABs Documentation](https://docs.databricks.com/dev-tools/bundles/)
# MAGIC * [Bundle Templates](https://github.com/databricks/bundle-templates)
# MAGIC * [Example Projects](https://github.com/databricks/bundle-examples)
# MAGIC
# MAGIC ## 🎓 Module 07 Complete!
# MAGIC
# MAGIC You've mastered:
# MAGIC * Databricks Jobs and orchestration
# MAGIC * Task dependencies and scheduling
# MAGIC * Declarative Automation Bundles
# MAGIC * CI/CD deployment patterns
# MAGIC
# MAGIC ## ➡️ Next: Module 08 - Unity Catalog & Governance
# MAGIC
# MAGIC 🎉 **Congratulations on completing Module 07!**
