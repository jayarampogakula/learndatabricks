# Databricks notebook source
# DBTITLE 1,Module 6.1: MLflow Introduction
# MAGIC %md
# MAGIC # 🤖 Module 6.1: MLflow Tracking & Experiments
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand MLflow and its components
# MAGIC * Track ML experiments
# MAGIC * Compare model runs
# MAGIC * Use autologging
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🤖 What is MLflow?
# MAGIC
# MAGIC **MLflow** is an open-source platform for managing the ML lifecycle.
# MAGIC
# MAGIC ### **Key Components**
# MAGIC
# MAGIC 1. **Tracking** - Log experiments, parameters, metrics
# MAGIC 2. **Models** - Manage and deploy models
# MAGIC 3. **Registry** - Model versioning and staging
# MAGIC 4. **Projects** - Package ML code
# MAGIC
# MAGIC ### **Why MLflow on Databricks?**
# MAGIC ✅ **Fully managed** - No setup required
# MAGIC ✅ **Auto-logging** - Automatic tracking
# MAGIC ✅ **Unity Catalog** - Unified governance
# MAGIC ✅ **Scalable** - Enterprise-grade
# MAGIC ✅ **Integrated** - Works seamlessly

# COMMAND ----------

# DBTITLE 1,MLflow Tracking Basics
# MAGIC %md
# MAGIC ## 📊 MLflow Tracking
# MAGIC
# MAGIC ### **What to Track**
# MAGIC
# MAGIC * **Parameters** - Hyperparameters (learning_rate, n_estimators)
# MAGIC * **Metrics** - Performance (accuracy, RMSE, F1)
# MAGIC * **Artifacts** - Models, plots, data files
# MAGIC * **Tags** - Metadata for organization
# MAGIC
# MAGIC ### **Basic Pattern**
# MAGIC ```python
# MAGIC import mlflow
# MAGIC
# MAGIC with mlflow.start_run():
# MAGIC     # Log parameters
# MAGIC     mlflow.log_param("learning_rate", 0.01)
# MAGIC     
# MAGIC     # Train model
# MAGIC     model = train_model(...)
# MAGIC     
# MAGIC     # Log metrics
# MAGIC     mlflow.log_metric("accuracy", 0.95)
# MAGIC     
# MAGIC     # Log model
# MAGIC     mlflow.sklearn.log_model(model, "model")
# MAGIC ```
# MAGIC
# MAGIC ### **MLflow UI**
# MAGIC * View all experiments in Experiments tab
# MAGIC * Compare runs side-by-side
# MAGIC * Visualize metrics over time

# COMMAND ----------

# DBTITLE 1,Demo - Basic Tracking
# 🧪 Demo: Basic MLflow Tracking

import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

print("🤖 MLflow Tracking Demo")
print("=" * 60)

# Load data
iris = load_iris()
X_train, X_test, y_train, y_test = train_test_split(
    iris.data, iris.target, test_size=0.2, random_state=42
)

print("✅ Loaded iris dataset")
print(f"   Training samples: {len(X_train)}")
print(f"   Test samples: {len(X_test)}")

# Start MLflow run
with mlflow.start_run(run_name="iris_classifier"):
    # Log parameters
    n_estimators = 100
    max_depth = 5
    mlflow.log_param("n_estimators", n_estimators)
    mlflow.log_param("max_depth", max_depth)
    
    print("\n📝 Logged parameters")
    
    # Train model
    model = RandomForestClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        random_state=42
    )
    model.fit(X_train, y_train)
    
    # Evaluate
    predictions = model.predict(X_test)
    accuracy = accuracy_score(y_test, predictions)
    
    # Log metrics
    mlflow.log_metric("accuracy", accuracy)
    
    # Log model
    mlflow.sklearn.log_model(model, "model")
    
    print(f"\n✅ Training complete!")
    print(f"   Accuracy: {accuracy:.3f}")
    print(f"   Parameters: n_estimators={n_estimators}, max_depth={max_depth}")
    print("\n💡 View this run in the MLflow UI (Experiments tab)!")

# COMMAND ----------

# DBTITLE 1,Autologging
# MAGIC %md
# MAGIC ## ⚡ MLflow Autologging
# MAGIC
# MAGIC MLflow can **automatically log** parameters, metrics, and models!
# MAGIC
# MAGIC ### **Supported Frameworks**
# MAGIC * **scikit-learn** - Classification, regression, clustering
# MAGIC * **TensorFlow/Keras** - Deep learning
# MAGIC * **PyTorch** - Deep learning
# MAGIC * **XGBoost** - Gradient boosting
# MAGIC * **LightGBM** - Gradient boosting
# MAGIC * **Spark MLlib** - Distributed ML
# MAGIC
# MAGIC ### **How to Use**
# MAGIC ```python
# MAGIC import mlflow
# MAGIC
# MAGIC # Enable autologging (one line!)
# MAGIC mlflow.sklearn.autolog()
# MAGIC
# MAGIC # Train model - everything logged automatically!
# MAGIC model = RandomForestClassifier()
# MAGIC model.fit(X_train, y_train)
# MAGIC ```
# MAGIC
# MAGIC ### **What Gets Logged**
# MAGIC ✅ All model parameters
# MAGIC ✅ Training metrics
# MAGIC ✅ Model artifact
# MAGIC ✅ Feature importance
# MAGIC ✅ Training dataset signature
# MAGIC
# MAGIC 💡 **Autologging is the easiest way to get started!**

# COMMAND ----------

# DBTITLE 1,Demo - Comparing Runs
# 📊 Demo: Comparing Multiple Runs

import mlflow
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

print("📊 Hyperparameter Tuning with MLflow")
print("=" * 60)

# Try different hyperparameters
results = []

for n_estimators in [50, 100, 200]:
    for max_depth in [3, 5, 10]:
        with mlflow.start_run(run_name=f"n{n_estimators}_d{max_depth}"):
            # Log parameters
            mlflow.log_param("n_estimators", n_estimators)
            mlflow.log_param("max_depth", max_depth)
            
            # Train
            model = RandomForestClassifier(
                n_estimators=n_estimators,
                max_depth=max_depth,
                random_state=42
            )
            model.fit(X_train, y_train)
            
            # Evaluate
            accuracy = accuracy_score(y_test, model.predict(X_test))
            mlflow.log_metric("accuracy", accuracy)
            
            results.append((n_estimators, max_depth, accuracy))
            print(f"n={n_estimators:3d}, d={max_depth:2d}: accuracy={accuracy:.3f}")

# Find best
best = max(results, key=lambda x: x[2])
print(f"\n✅ Best configuration:")
print(f"   n_estimators={best[0]}, max_depth={best[1]}")
print(f"   accuracy={best[2]:.3f}")
print("\n💡 Compare all 9 runs in MLflow UI!")
print("   Click 'Experiments' → View runs in table")
print("   Sort by accuracy to find best model")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **MLflow Tracking** - Log experiments automatically
# MAGIC ✅ **Parameters** - Hyperparameters and configs
# MAGIC ✅ **Metrics** - Model performance scores
# MAGIC ✅ **Artifacts** - Models, plots, files
# MAGIC ✅ **Autologging** - One-line tracking for common frameworks
# MAGIC ✅ **Experiments** - Compare multiple runs
# MAGIC
# MAGIC ## 🎯 Best Practices
# MAGIC
# MAGIC * **Use descriptive run names** - Easy to identify later
# MAGIC * **Log all important parameters** - Reproducibility
# MAGIC * **Track metrics consistently** - Enable comparison
# MAGIC * **Use autologging when possible** - Less code, more tracking
# MAGIC * **Add tags for organization** - Group related runs
# MAGIC * **Compare runs in MLflow UI** - Find best models
# MAGIC
# MAGIC ## 💡 Key Takeaways
# MAGIC
# MAGIC * MLflow makes experiment tracking easy
# MAGIC * Track everything: parameters, metrics, models
# MAGIC * Autologging works with popular frameworks
# MAGIC * Compare runs to find best hyperparameters
# MAGIC * MLflow UI provides visualization and comparison
# MAGIC
# MAGIC ## ➡️ Next: Module 6.2 - Model Registry & Serving
# MAGIC
# MAGIC 🎉 **Module 6.1 Complete!**
