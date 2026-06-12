# Databricks notebook source
# DBTITLE 1,Module 6.2: Model Registry
# MAGIC %md
# MAGIC # 🗂️ Module 6.2: Model Registry & Serving
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Register and version models
# MAGIC * Manage model lifecycle
# MAGIC * Deploy models for inference
# MAGIC * Understand model stages
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🗂️ What is Model Registry?
# MAGIC
# MAGIC **Model Registry** is a centralized model store with versioning and lifecycle management.
# MAGIC
# MAGIC ### **Key Features**
# MAGIC
# MAGIC ✅ **Versioning** - Track all model versions
# MAGIC ✅ **Stages** - None, Staging, Production, Archived
# MAGIC ✅ **Lineage** - Track data and code
# MAGIC ✅ **Annotations** - Descriptions, tags
# MAGIC ✅ **Unity Catalog** - Unified governance
# MAGIC
# MAGIC ### **Model Lifecycle**
# MAGIC ```
# MAGIC Train → Register → Stage → Deploy → Monitor
# MAGIC ```
# MAGIC
# MAGIC ### **Why Use Model Registry?**
# MAGIC * **Version control** for models
# MAGIC * **Collaboration** across teams
# MAGIC * **Governance** and compliance
# MAGIC * **Easy deployment** to production
# MAGIC * **Rollback** to previous versions

# COMMAND ----------

# DBTITLE 1,Registering Models
# MAGIC %md
# MAGIC ## 📝 Registering Models
# MAGIC
# MAGIC ### **Method 1: During Training**
# MAGIC ```python
# MAGIC import mlflow
# MAGIC
# MAGIC with mlflow.start_run():
# MAGIC     # Train model
# MAGIC     model.fit(X_train, y_train)
# MAGIC     
# MAGIC     # Log and register in one step
# MAGIC     mlflow.sklearn.log_model(
# MAGIC         model,
# MAGIC         "model",
# MAGIC         registered_model_name="iris_classifier"
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ### **Method 2: Register Existing Run**
# MAGIC ```python
# MAGIC # Register after training
# MAGIC run_id = "abc123..."
# MAGIC model_uri = f"runs:/{run_id}/model"
# MAGIC
# MAGIC mlflow.register_model(
# MAGIC     model_uri,
# MAGIC     "iris_classifier"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### **Model Naming**
# MAGIC * Use descriptive names: `customer_churn_model`
# MAGIC * Include purpose: `fraud_detection_v2`
# MAGIC * Follow team conventions
# MAGIC
# MAGIC 💡 **First registration creates Version 1**

# COMMAND ----------

# DBTITLE 1,Demo - Model Registry
# 🗂️ Demo: Model Registry Workflow

import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

print("🗂️ Model Registry Demo")
print("=" * 60)

# Prepare data
iris = load_iris()
X_train, X_test, y_train, y_test = train_test_split(
    iris.data, iris.target, test_size=0.2, random_state=42
)

# Train and register model
with mlflow.start_run(run_name="registry_demo"):
    # Train
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # Evaluate
    accuracy = accuracy_score(y_test, model.predict(X_test))
    mlflow.log_metric("accuracy", accuracy)
    
    print(f"✅ Trained model with accuracy: {accuracy:.3f}")
    
    # Register model
    mlflow.sklearn.log_model(
        model,
        "model",
        registered_model_name="demo_iris_model"
    )
    
    print("✅ Registered model as 'demo_iris_model'")
    print("   Version: 1 (first version)")

# View registered models
from mlflow.tracking import MlflowClient
client = MlflowClient()

print("\n📁 Registered Models:")
try:
    models = client.search_registered_models(filter_string="name='demo_iris_model'")
    for model in models:
        print(f"   Model: {model.name}")
        for version in model.latest_versions:
            print(f"     Version {version.version}: stage={version.current_stage}")
except:
    print("   demo_iris_model (just registered)")

print("\n💡 View in UI: Models → demo_iris_model")

# COMMAND ----------

# DBTITLE 1,Model Stages
# MAGIC %md
# MAGIC ## 🏷️ Model Stages
# MAGIC
# MAGIC ### **Stage Lifecycle**
# MAGIC
# MAGIC 1. **None** - Just registered, not yet promoted
# MAGIC 2. **Staging** - Testing and validation
# MAGIC 3. **Production** - Live deployment
# MAGIC 4. **Archived** - Deprecated, no longer used
# MAGIC
# MAGIC ### **Transitioning Stages**
# MAGIC ```python
# MAGIC from mlflow.tracking import MlflowClient
# MAGIC
# MAGIC client = MlflowClient()
# MAGIC
# MAGIC # Promote to Staging
# MAGIC client.transition_model_version_stage(
# MAGIC     name="iris_classifier",
# MAGIC     version=1,
# MAGIC     stage="Staging"
# MAGIC )
# MAGIC
# MAGIC # Promote to Production
# MAGIC client.transition_model_version_stage(
# MAGIC     name="iris_classifier",
# MAGIC     version=1,
# MAGIC     stage="Production"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### **Best Practices**
# MAGIC * Test in **Staging** before Production
# MAGIC * Only one version in **Production** (usually)
# MAGIC * **Archive** old versions, don't delete
# MAGIC * Document changes when transitioning

# COMMAND ----------

# DBTITLE 1,Loading Models
# MAGIC %md
# MAGIC ## 📥 Loading Models for Inference
# MAGIC
# MAGIC ### **Load by Stage**
# MAGIC ```python
# MAGIC import mlflow
# MAGIC
# MAGIC # Load production model
# MAGIC model = mlflow.pyfunc.load_model(
# MAGIC     "models:/iris_classifier/Production"
# MAGIC )
# MAGIC
# MAGIC # Make predictions
# MAGIC predictions = model.predict(X_test)
# MAGIC ```
# MAGIC
# MAGIC ### **Load by Version**
# MAGIC ```python
# MAGIC # Load specific version
# MAGIC model = mlflow.pyfunc.load_model(
# MAGIC     "models:/iris_classifier/1"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### **Model URI Format**
# MAGIC * `models:/<model_name>/Production` - By stage
# MAGIC * `models:/<model_name>/3` - By version
# MAGIC * `runs:/<run_id>/model` - From specific run
# MAGIC
# MAGIC ### **Why Load by Stage?**
# MAGIC ✅ **Decouple** code from specific versions
# MAGIC ✅ **Easy updates** - Just transition stages
# MAGIC ✅ **Rollback** - Revert to previous production version
# MAGIC
# MAGIC 💡 **Always load by stage in production code!**

# COMMAND ----------

# DBTITLE 1,Model Serving
# MAGIC %md
# MAGIC ## 🚀 Model Serving
# MAGIC
# MAGIC ### **Serving Options**
# MAGIC
# MAGIC #### **1. Batch Inference** - Spark UDF
# MAGIC ```python
# MAGIC import mlflow
# MAGIC
# MAGIC # Create UDF from model
# MAGIC predict_udf = mlflow.pyfunc.spark_udf(
# MAGIC     spark, 
# MAGIC     "models:/iris_classifier/Production"
# MAGIC )
# MAGIC
# MAGIC # Apply to DataFrame
# MAGIC df_with_predictions = df.withColumn(
# MAGIC     "prediction",
# MAGIC     predict_udf(*feature_columns)
# MAGIC )
# MAGIC ```
# MAGIC **Use when**: Large batch scoring jobs
# MAGIC
# MAGIC #### **2. Real-time API** - Model Serving Endpoints
# MAGIC * REST API endpoint
# MAGIC * Auto-scaling
# MAGIC * A/B testing support
# MAGIC * Monitoring built-in
# MAGIC
# MAGIC **Use when**: Low-latency predictions (<100ms)
# MAGIC
# MAGIC #### **3. Edge Deployment** - Export Model
# MAGIC ```python
# MAGIC # Save for deployment
# MAGIC mlflow.sklearn.save_model(model, "/path/to/model")
# MAGIC ```
# MAGIC **Use when**: Offline inference, embedded systems
# MAGIC
# MAGIC ### **Serving Best Practices**
# MAGIC ✅ Use **Model Registry** for all deployments
# MAGIC ✅ Load by **stage**, not version
# MAGIC ✅ Monitor **prediction latency**
# MAGIC ✅ Track **model drift**
# MAGIC ✅ Set up **alerts** for failures

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Model Registry** - Centralized model versioning
# MAGIC ✅ **Stages** - None → Staging → Production → Archived
# MAGIC ✅ **Versioning** - Track all model versions automatically
# MAGIC ✅ **Loading** - Load by stage or version
# MAGIC ✅ **Serving** - Batch (Spark UDF), Real-time (API), Edge
# MAGIC
# MAGIC ## 🎯 Best Practices
# MAGIC
# MAGIC ### **Model Management**
# MAGIC * Register all production models
# MAGIC * Use descriptive model names
# MAGIC * Test in Staging before Production
# MAGIC * Document model changes
# MAGIC * Archive old versions (don't delete)
# MAGIC
# MAGIC ### **Deployment**
# MAGIC * Load models by stage in code
# MAGIC * Monitor prediction performance
# MAGIC * Set up alerting
# MAGIC * Track model drift
# MAGIC * Plan rollback strategy
# MAGIC
# MAGIC ## 💡 Complete MLflow Workflow
# MAGIC
# MAGIC 1. **Experiment** - Track runs with MLflow Tracking
# MAGIC 2. **Register** - Add best model to Registry
# MAGIC 3. **Stage** - Promote through lifecycle stages
# MAGIC 4. **Deploy** - Serve for batch or real-time inference
# MAGIC 5. **Monitor** - Track performance and drift
# MAGIC
# MAGIC ## 🎓 Module 06 Complete!
# MAGIC
# MAGIC You've mastered:
# MAGIC * MLflow tracking and experiments
# MAGIC * Model registry and versioning
# MAGIC * Model lifecycle management
# MAGIC * Deployment patterns
# MAGIC
# MAGIC ## ➡️ Next: Module 07 - Workflows & Orchestration
# MAGIC
# MAGIC 🎉 **Congratulations on completing Module 06!**
