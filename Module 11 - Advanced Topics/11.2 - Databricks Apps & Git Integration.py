# Databricks notebook source
# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Databricks Apps** - Build interactive web applications
# MAGIC ✅ **Multiple Frameworks** - Gradio, Streamlit, Dash, Flask, FastAPI
# MAGIC ✅ **Git Integration** - Version control with Repos
# MAGIC ✅ **CI/CD** - Automated testing and deployment
# MAGIC ✅ **Deployment** - Serverless, scalable apps
# MAGIC ✅ **Monitoring** - Logs, metrics, health checks
# MAGIC
# MAGIC ## 🎯 Key Concepts
# MAGIC
# MAGIC ### **Databricks App Structure**
# MAGIC
# MAGIC ```
# MAGIC my-app/
# MAGIC ├── app.py           # Main code
# MAGIC ├── app.yaml         # Configuration
# MAGIC ├── requirements.txt # Dependencies
# MAGIC └── README.md        # Docs
# MAGIC ```
# MAGIC
# MAGIC ### **Git Workflow**
# MAGIC
# MAGIC ```
# MAGIC 1. Clone → 2. Branch → 3. Develop → 4. Commit → 5. Push → 6. PR → 7. Merge
# MAGIC ```
# MAGIC
# MAGIC ### **Deployment Commands**
# MAGIC
# MAGIC ```bash
# MAGIC # Create and deploy app
# MAGIC databricks apps create --name my-app --source-code-path /path
# MAGIC databricks apps deploy --app-name my-app
# MAGIC databricks apps start --app-name my-app
# MAGIC
# MAGIC # Manage app
# MAGIC databricks apps get --app-name my-app
# MAGIC databricks apps logs --app-name my-app
# MAGIC databricks apps stop --app-name my-app
# MAGIC ```
# MAGIC
# MAGIC ### **CI/CD Pipeline**
# MAGIC
# MAGIC ```yaml
# MAGIC stages:
# MAGIC   - test      # Run unit tests
# MAGIC   - deploy    # Deploy to Databricks
# MAGIC   - monitor   # Check health
# MAGIC ```
# MAGIC
# MAGIC ## 🎯 Best Practices
# MAGIC
# MAGIC ### **App Development**
# MAGIC
# MAGIC ✅ **Start simple** - Build MVP first
# MAGIC ✅ **Test locally** - Debug before deploying
# MAGIC ✅ **Use logging** - Track app behavior
# MAGIC ✅ **Handle errors** - Graceful error handling
# MAGIC ✅ **Optimize performance** - Profile and optimize
# MAGIC ✅ **Secure credentials** - Use secrets
# MAGIC
# MAGIC ### **Git & Version Control**
# MAGIC
# MAGIC ✅ **Commit frequently** - Small, focused commits
# MAGIC ✅ **Clear messages** - Descriptive commit messages
# MAGIC ✅ **Feature branches** - One branch per feature
# MAGIC ✅ **Code reviews** - Review before merging
# MAGIC ✅ **Tag releases** - Version your releases
# MAGIC ✅ **Keep main stable** - Don't commit directly to main
# MAGIC
# MAGIC ### **CI/CD**
# MAGIC
# MAGIC ✅ **Automate tests** - Run on every commit
# MAGIC ✅ **Environment parity** - Dev/staging/prod similar
# MAGIC ✅ **Fail fast** - Catch errors early
# MAGIC ✅ **Monitor deployments** - Track success/failures
# MAGIC ✅ **Rollback plan** - Be ready to revert
# MAGIC ✅ **Secrets management** - Secure credentials
# MAGIC
# MAGIC ### **Deployment**
# MAGIC
# MAGIC ✅ **Health checks** - Monitor app status
# MAGIC ✅ **Logging** - Comprehensive logging
# MAGIC ✅ **Metrics** - Track usage and performance
# MAGIC ✅ **Autoscaling** - Handle traffic spikes
# MAGIC ✅ **Authentication** - Control access
# MAGIC ✅ **Documentation** - Clear usage docs
# MAGIC
# MAGIC ## 💡 Remember
# MAGIC
# MAGIC * **Apps** - Interactive web apps on Databricks
# MAGIC * **Frameworks** - Gradio, Streamlit, Dash, Flask, FastAPI
# MAGIC * **Git** - Version control and collaboration
# MAGIC * **Repos** - Databricks Git integration
# MAGIC * **CI/CD** - Automate testing and deployment
# MAGIC * **Monitoring** - Logs, metrics, health checks
# MAGIC * **Security** - Auth, secrets, permissions
# MAGIC
# MAGIC ## 🎓 Module 11.2 Complete!
# MAGIC
# MAGIC You've learned:
# MAGIC * Building Databricks Apps with popular frameworks
# MAGIC * Git integration for version control
# MAGIC * CI/CD pipelines for automated deployment
# MAGIC * App deployment and monitoring best practices
# MAGIC
# MAGIC ## ➡️ Next: Module 12 - Serverless & Advanced Features
# MAGIC
# MAGIC 🎉 **Congratulations on completing Module 11!**

# COMMAND ----------

# DBTITLE 1,App Deployment
# MAGIC %md
# MAGIC ## 🚀 App Deployment & Management
# MAGIC
# MAGIC ### **Deploying a Databricks App**
# MAGIC
# MAGIC #### **1. Via Databricks CLI**
# MAGIC
# MAGIC ```bash
# MAGIC # Create app
# MAGIC databricks apps create \\
# MAGIC   --name my-app \\
# MAGIC   --source-code-path /Repos/myuser/myrepo/app
# MAGIC
# MAGIC # Deploy app
# MAGIC databricks apps deploy \\
# MAGIC   --app-name my-app \\
# MAGIC   --source-code-path /Repos/myuser/myrepo/app
# MAGIC
# MAGIC # Start app
# MAGIC databricks apps start --app-name my-app
# MAGIC
# MAGIC # Get app status
# MAGIC databricks apps get --app-name my-app
# MAGIC
# MAGIC # View app logs
# MAGIC databricks apps logs --app-name my-app
# MAGIC
# MAGIC # Stop app
# MAGIC databricks apps stop --app-name my-app
# MAGIC ```
# MAGIC
# MAGIC #### **2. Via UI**
# MAGIC
# MAGIC 1. Navigate to **Apps** in workspace
# MAGIC 2. Click **Create App**
# MAGIC 3. Select source code location
# MAGIC 4. Configure settings (name, resources)
# MAGIC 5. Click **Deploy**
# MAGIC
# MAGIC ### **App Configuration**
# MAGIC
# MAGIC ```yaml
# MAGIC # app.yaml
# MAGIC name: customer-dashboard
# MAGIC runtime:
# MAGIC   python_version: "3.10"
# MAGIC   
# MAGIC resources:
# MAGIC   instance_type: medium  # small, medium, large
# MAGIC   autoscaling:
# MAGIC     min_instances: 1
# MAGIC     max_instances: 5
# MAGIC     target_cpu_utilization: 70
# MAGIC
# MAGIC environment:
# MAGIC   variables:
# MAGIC     CATALOG: main
# MAGIC     SCHEMA: analytics
# MAGIC     LOG_LEVEL: INFO
# MAGIC
# MAGIC auth:
# MAGIC   enabled: true
# MAGIC   allowed_users:
# MAGIC     - user1@company.com
# MAGIC     - user2@company.com
# MAGIC   allowed_groups:
# MAGIC     - data-team
# MAGIC     - analysts
# MAGIC ```
# MAGIC
# MAGIC ### **Monitoring & Logging**
# MAGIC
# MAGIC ```python
# MAGIC import logging
# MAGIC
# MAGIC # Configure logging
# MAGIC logging.basicConfig(
# MAGIC     level=logging.INFO,
# MAGIC     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# MAGIC )
# MAGIC
# MAGIC logger = logging.getLogger(__name__)
# MAGIC
# MAGIC def process_request(request):
# MAGIC     logger.info(f"Processing request: {request}")
# MAGIC     try:
# MAGIC         result = do_work(request)
# MAGIC         logger.info(f"Request completed successfully")
# MAGIC         return result
# MAGIC     except Exception as e:
# MAGIC         logger.error(f"Request failed: {str(e)}")
# MAGIC         raise
# MAGIC ```
# MAGIC
# MAGIC ### **Health Checks**
# MAGIC
# MAGIC ```python
# MAGIC from fastapi import FastAPI
# MAGIC
# MAGIC app = FastAPI()
# MAGIC
# MAGIC @app.get("/health")
# MAGIC def health_check():
# MAGIC     """Health check endpoint"""
# MAGIC     return {
# MAGIC         "status": "healthy",
# MAGIC         "version": "1.0.0",
# MAGIC         "timestamp": datetime.now().isoformat()
# MAGIC     }
# MAGIC
# MAGIC @app.get("/metrics")
# MAGIC def metrics():
# MAGIC     """Metrics endpoint"""
# MAGIC     return {
# MAGIC         "requests_total": request_counter,
# MAGIC         "errors_total": error_counter,
# MAGIC         "avg_response_time_ms": avg_response_time
# MAGIC     }
# MAGIC ```
# MAGIC
# MAGIC ### **Troubleshooting**
# MAGIC
# MAGIC ✅ **Check logs** - View application logs
# MAGIC ✅ **Verify permissions** - Ensure catalog/table access
# MAGIC ✅ **Test locally** - Run app locally first
# MAGIC ✅ **Check dependencies** - Verify requirements.txt
# MAGIC ✅ **Monitor resources** - CPU/memory usage
# MAGIC ✅ **Review errors** - Check error messages

# COMMAND ----------

# DBTITLE 1,CI/CD with Git
# MAGIC %md
# MAGIC ## 🚀 CI/CD with Git
# MAGIC
# MAGIC **Continuous Integration/Continuous Deployment** automates testing and deployment.
# MAGIC
# MAGIC ### **GitHub Actions Example**
# MAGIC
# MAGIC ```yaml
# MAGIC name: Deploy to Databricks
# MAGIC
# MAGIC on:
# MAGIC   push:
# MAGIC     branches: [main]
# MAGIC   pull_request:
# MAGIC     branches: [main]
# MAGIC
# MAGIC jobs:
# MAGIC   deploy:
# MAGIC     runs-on: ubuntu-latest
# MAGIC     
# MAGIC     steps:
# MAGIC     - name: Checkout code
# MAGIC       uses: actions/checkout@v3
# MAGIC     
# MAGIC     - name: Set up Python
# MAGIC       uses: actions/setup-python@v4
# MAGIC       with:
# MAGIC         python-version: '3.10'
# MAGIC     
# MAGIC     - name: Install dependencies
# MAGIC       run: |
# MAGIC         pip install databricks-cli pytest
# MAGIC         pip install -r requirements.txt
# MAGIC     
# MAGIC     - name: Run tests
# MAGIC       run: |
# MAGIC         pytest tests/
# MAGIC     
# MAGIC     - name: Deploy to Databricks
# MAGIC       env:
# MAGIC         DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
# MAGIC         DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
# MAGIC       run: |
# MAGIC         databricks repos update \\
# MAGIC           --repo-id ${{ secrets.REPO_ID }} \\
# MAGIC           --branch main
# MAGIC ```
# MAGIC
# MAGIC ### **GitLab CI/CD Example**
# MAGIC
# MAGIC ```yaml
# MAGIC stages:
# MAGIC   - test
# MAGIC   - deploy
# MAGIC
# MAGIC test:
# MAGIC   stage: test
# MAGIC   script:
# MAGIC     - pip install pytest
# MAGIC     - pytest tests/
# MAGIC
# MAGIC deploy:
# MAGIC   stage: deploy
# MAGIC   only:
# MAGIC     - main
# MAGIC   script:
# MAGIC     - pip install databricks-cli
# MAGIC     - databricks repos update --repo-id $REPO_ID --branch main
# MAGIC ```
# MAGIC
# MAGIC ### **Testing Strategy**
# MAGIC
# MAGIC ```python
# MAGIC # tests/test_app.py
# MAGIC import pytest
# MAGIC from app import analyze_table
# MAGIC
# MAGIC def test_analyze_table():
# MAGIC     """Test table analysis function"""
# MAGIC     result = analyze_table('main', 'default', 'test_table')
# MAGIC     assert result is not None
# MAGIC     assert 'row_count' in result
# MAGIC
# MAGIC def test_invalid_table():
# MAGIC     """Test error handling"""
# MAGIC     with pytest.raises(Exception):
# MAGIC         analyze_table('invalid', 'invalid', 'invalid')
# MAGIC ```
# MAGIC
# MAGIC ### **Deployment Stages**
# MAGIC
# MAGIC ```
# MAGIC Development → Staging → Production
# MAGIC
# MAGIC * Development: feature branches
# MAGIC * Staging: develop branch
# MAGIC * Production: main branch
# MAGIC ```
# MAGIC
# MAGIC ### **Environment Variables**
# MAGIC
# MAGIC ```yaml
# MAGIC # app.yaml
# MAGIC environments:
# MAGIC   development:
# MAGIC     variables:
# MAGIC       DB_HOST: dev-db.company.com
# MAGIC       LOG_LEVEL: DEBUG
# MAGIC   
# MAGIC   staging:
# MAGIC     variables:
# MAGIC       DB_HOST: staging-db.company.com
# MAGIC       LOG_LEVEL: INFO
# MAGIC   
# MAGIC   production:
# MAGIC     variables:
# MAGIC       DB_HOST: prod-db.company.com
# MAGIC       LOG_LEVEL: WARNING
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Git Workflow Demo
# 🔀 Demo: Git Workflow

print("🔀 Databricks Git Workflow")
print("=" * 60)

print("\n📚 Git Commands (using Databricks CLI):")
print("""\n# 1. Clone Repository
databricks repos create \\
  --url https://github.com/myorg/myrepo.git \\
  --provider github \\
  --path /Repos/myuser/myrepo

# 2. Check Current Branch
databricks repos get --repo-id <repo-id>

# 3. Create and Switch to Feature Branch
databricks repos update \\
  --repo-id <repo-id> \\
  --branch feature/new-feature

# 4. Commit Changes (via UI or API)
# Changes are auto-committed when you save notebooks

# 5. List All Repos
databricks repos list

# 6. Pull Latest Changes
databricks repos update \\
  --repo-id <repo-id> \\
  --branch main
""")

print("\n🔍 Using Git in Notebooks:")
print("""\nimport os
import subprocess

def git_status():
    \"\"\"Check Git status\"\"\"    
    result = subprocess.run(
        ['git', 'status'],
        capture_output=True,
        text=True
    )
    return result.stdout

def git_commit(message):
    \"\"\"Commit changes\"\"\"    
    subprocess.run(['git', 'add', '.'])
    subprocess.run(['git', 'commit', '-m', message])
    
def git_push():
    \"\"\"Push to remote\"\"\"    
    subprocess.run(['git', 'push'])

# Example usage
print(git_status())
git_commit("Update analysis notebook")
git_push()
""")

print("\n🌱 Branching Strategy:")
print("""\n* main - Production code
* develop - Development branch
* feature/* - New features
* bugfix/* - Bug fixes
* hotfix/* - Urgent production fixes
* release/* - Release preparation
""")

print("\n📝 .gitignore for Databricks:")
print("""\n# Databricks notebook metadata
.ipynb_checkpoints/

# Python
__pycache__/
*.pyc
*.pyo
.pytest_cache/

# Virtual environments
venv/
.env

# IDE
.vscode/
.idea/

# Logs
*.log

# Data files (don't commit large data)
*.csv
*.parquet
data/
""")

print("\n✅ Best Practices:")
print("   • Commit frequently with clear messages")
print("   • Use feature branches for development")
print("   • Keep main branch stable")
print("   • Review code before merging")
print("   • Use .gitignore to exclude unnecessary files")
print("   • Tag releases for tracking")

# COMMAND ----------

# DBTITLE 1,Git Integration
# MAGIC %md
# MAGIC ## 🔀 Git Integration
# MAGIC
# MAGIC **Databricks Git integration** (Repos) enables version control for notebooks, apps, and code.
# MAGIC
# MAGIC ### **Why Use Git with Databricks?**
# MAGIC
# MAGIC ✅ **Version Control** - Track all changes
# MAGIC ✅ **Collaboration** - Multiple developers
# MAGIC ✅ **Code Review** - Pull request workflow
# MAGIC ✅ **CI/CD** - Automated deployments
# MAGIC ✅ **Backup** - Code safety
# MAGIC ✅ **Branching** - Feature development
# MAGIC
# MAGIC ### **Supported Git Providers**
# MAGIC
# MAGIC * **GitHub**
# MAGIC * **GitLab**
# MAGIC * **Bitbucket**
# MAGIC * **Azure DevOps**
# MAGIC * **AWS CodeCommit**
# MAGIC
# MAGIC ### **Git Workflow**
# MAGIC
# MAGIC ```
# MAGIC 1. Clone repository to Databricks
# MAGIC    ↓
# MAGIC 2. Create feature branch
# MAGIC    ↓
# MAGIC 3. Make changes in notebooks/apps
# MAGIC    ↓
# MAGIC 4. Commit changes
# MAGIC    ↓
# MAGIC 5. Push to remote
# MAGIC    ↓
# MAGIC 6. Create pull request
# MAGIC    ↓
# MAGIC 7. Code review
# MAGIC    ↓
# MAGIC 8. Merge to main
# MAGIC    ↓
# MAGIC 9. Deploy to production
# MAGIC ```
# MAGIC
# MAGIC ### **Databricks Repos UI**
# MAGIC
# MAGIC * **Clone** - Import repository
# MAGIC * **Branch** - Switch/create branches
# MAGIC * **Commit** - Save changes
# MAGIC * **Push** - Upload to remote
# MAGIC * **Pull** - Download updates
# MAGIC * **Diff** - View changes
# MAGIC * **Merge** - Combine branches

# COMMAND ----------

# DBTITLE 1,Simple App Example
# 📦 Demo: Simple Databricks App

print("📦 Simple Databricks App Example")
print("=" * 60)

print("\n📚 Gradio App Example:")
print("""\nimport gradio as gr
import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

def analyze_table(catalog, schema, table):
    \"\"\"Analyze Unity Catalog table\"\"\"    
    # Read table
    df = spark.table(f"{catalog}.{schema}.{table}")
    
    # Get statistics
    stats = {
        "Row Count": df.count(),
        "Column Count": len(df.columns),
        "Columns": ", ".join(df.columns)
    }
    
    # Sample data
    sample = df.limit(10).toPandas()
    
    return stats, sample

# Create Gradio interface
with gr.Blocks() as app:
    gr.Markdown("# 📋 Table Analyzer")
    
    with gr.Row():
        catalog_input = gr.Textbox(label="Catalog", value="main")
        schema_input = gr.Textbox(label="Schema", value="default")
        table_input = gr.Textbox(label="Table")
    
    analyze_btn = gr.Button("Analyze")
    
    stats_output = gr.JSON(label="Statistics")
    data_output = gr.Dataframe(label="Sample Data")
    
    analyze_btn.click(
        fn=analyze_table,
        inputs=[catalog_input, schema_input, table_input],
        outputs=[stats_output, data_output]
    )

if __name__ == "__main__":
    app.launch()
""")

print("\n🚀 Streamlit App Example:")
print("""\nimport streamlit as st
import pandas as pd
from pyspark.sql import SparkSession

st.title("📊 Data Dashboard")

# Sidebar
st.sidebar.header("Settings")
catalog = st.sidebar.text_input("Catalog", "main")
schema = st.sidebar.text_input("Schema", "default")
table = st.sidebar.text_input("Table")

if st.sidebar.button("Load Data"):
    spark = SparkSession.builder.getOrCreate()
    df = spark.table(f"{catalog}.{schema}.{table}")
    
    # Display metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Rows", df.count())
    col2.metric("Columns", len(df.columns))
    col3.metric("Size", f"{df.count() * len(df.columns)} cells")
    
    # Display data
    st.dataframe(df.limit(100).toPandas())
    
    # Chart
    if 'amount' in df.columns:
        st.bar_chart(df.groupBy('category').sum('amount').toPandas())
""")

print("\n✅ App Frameworks:")
print("   • Gradio - Simple ML interfaces")
print("   • Streamlit - Data dashboards")
print("   • Dash - Analytical apps")
print("   • Flask - Custom web apps")
print("   • FastAPI - REST APIs")

# COMMAND ----------

# DBTITLE 1,App Structure
# MAGIC %md
# MAGIC ## 📝 Databricks App Structure
# MAGIC
# MAGIC ### **Basic App Files**
# MAGIC
# MAGIC ```
# MAGIC my-app/
# MAGIC ├── app.py           # Main application code
# MAGIC ├── app.yaml         # Configuration
# MAGIC ├── requirements.txt # Python dependencies
# MAGIC └── README.md        # Documentation
# MAGIC ```
# MAGIC
# MAGIC ### **app.py Example (Gradio)**
# MAGIC
# MAGIC ```python
# MAGIC import gradio as gr
# MAGIC from databricks import sql
# MAGIC
# MAGIC def query_data(table_name):
# MAGIC     """Query Unity Catalog table"""
# MAGIC     with sql.connect(
# MAGIC         server_hostname="...",
# MAGIC         http_path="..."
# MAGIC     ) as connection:
# MAGIC         cursor = connection.cursor()
# MAGIC         cursor.execute(f"SELECT * FROM {table_name} LIMIT 10")
# MAGIC         return cursor.fetchall()
# MAGIC
# MAGIC # Create Gradio interface
# MAGIC interface = gr.Interface(
# MAGIC     fn=query_data,
# MAGIC     inputs=gr.Textbox(label="Table Name"),
# MAGIC     outputs=gr.Dataframe(label="Results"),
# MAGIC     title="Databricks Table Viewer"
# MAGIC )
# MAGIC
# MAGIC if __name__ == "__main__":
# MAGIC     interface.launch()
# MAGIC ```
# MAGIC
# MAGIC ### **app.yaml Configuration**
# MAGIC
# MAGIC ```yaml
# MAGIC name: my-databricks-app
# MAGIC runtime:
# MAGIC   python_version: "3.10"
# MAGIC resources:
# MAGIC   instance_type: medium
# MAGIC   autoscaling:
# MAGIC     min_instances: 1
# MAGIC     max_instances: 5
# MAGIC environment:
# MAGIC   variables:
# MAGIC     APP_ENV: production
# MAGIC ```
# MAGIC
# MAGIC ### **requirements.txt**
# MAGIC
# MAGIC ```txt
# MAGIC gradio==4.0.0
# MAGIC pandas==2.0.0
# MAGIC databricks-sql-connector==2.9.0
# MAGIC mlflow==2.8.0
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Module 11.2 Introduction
# MAGIC %md
# MAGIC # 📦 Module 11.2: Databricks Apps & Git Integration
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Build Databricks Apps
# MAGIC * Deploy interactive applications
# MAGIC * Integrate with Git repositories
# MAGIC * Implement CI/CD workflows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📦 What are Databricks Apps?
# MAGIC
# MAGIC **Databricks Apps** let you build and deploy interactive web applications directly on Databricks.
# MAGIC
# MAGIC ### **Supported Frameworks**
# MAGIC
# MAGIC * **Gradio** - Simple ML demos and interfaces
# MAGIC * **Streamlit** - Data apps and dashboards
# MAGIC * **Dash** - Analytical web applications
# MAGIC * **Flask** - Custom web applications
# MAGIC * **FastAPI** - API endpoints
# MAGIC
# MAGIC ### **Key Features**
# MAGIC
# MAGIC ✅ **Serverless** - No infrastructure management
# MAGIC ✅ **Integrated** - Access Unity Catalog, MLflow, etc.
# MAGIC ✅ **Secure** - Built-in authentication
# MAGIC ✅ **Scalable** - Auto-scaling compute
# MAGIC ✅ **Version Control** - Git integration
# MAGIC
# MAGIC ### **Use Cases**
# MAGIC
# MAGIC * **ML Demos** - Interactive model interfaces
# MAGIC * **Data Dashboards** - Business intelligence apps
# MAGIC * **Admin Tools** - Internal management portals
# MAGIC * **API Services** - REST API endpoints
# MAGIC * **Data Quality** - Monitoring dashboards
