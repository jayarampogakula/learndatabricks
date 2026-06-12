# Databricks notebook source
# DBTITLE 1,Module 5.2: Dashboards
# MAGIC %md
# MAGIC # 📊 Module 5.2: Dashboards & Visualizations
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Build interactive dashboards
# MAGIC * Create visualizations in notebooks
# MAGIC * Share analytics with stakeholders
# MAGIC * Design effective dashboards
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Visualization Options in Databricks
# MAGIC
# MAGIC ### **1. Lakeview Dashboards** ⭐ (Recommended)
# MAGIC * Modern dashboard platform
# MAGIC * Drag-and-drop interface
# MAGIC * Real-time updates
# MAGIC * Share with stakeholders
# MAGIC * 👉 Best for: Business dashboards
# MAGIC
# MAGIC ### **2. Notebook Visualizations**
# MAGIC * Built-in DataFrame display
# MAGIC * Quick exploratory charts
# MAGIC * Matplotlib/Plotly support
# MAGIC * 👉 Best for: Data exploration
# MAGIC
# MAGIC ### **3. External BI Tools**
# MAGIC * Tableau, Power BI, Looker
# MAGIC * Enterprise dashboards
# MAGIC * Advanced features
# MAGIC * 👉 Best for: Enterprise BI

# COMMAND ----------

# DBTITLE 1,Notebook Visualizations
# MAGIC %md
# MAGIC ## 📊 Built-in DataFrame Visualizations
# MAGIC
# MAGIC ### **Display Types**
# MAGIC
# MAGIC #### **1. Table** (Default)
# MAGIC ```python
# MAGIC df.show()          # Console output
# MAGIC display(df)        # Interactive table
# MAGIC ```
# MAGIC
# MAGIC #### **2. Bar Chart**
# MAGIC * Compare categories
# MAGIC * Show distributions
# MAGIC * Time series data
# MAGIC
# MAGIC #### **3. Line Chart**
# MAGIC * Trends over time
# MAGIC * Multiple series comparison
# MAGIC * Continuous data
# MAGIC
# MAGIC #### **4. Pie Chart**
# MAGIC * Proportions
# MAGIC * Percentages
# MAGIC * Category breakdown
# MAGIC
# MAGIC #### **5. Scatter Plot**
# MAGIC * Correlations
# MAGIC * Relationships
# MAGIC * Outliers
# MAGIC
# MAGIC ### **Using display()**
# MAGIC ```python
# MAGIC # Interactive visualization
# MAGIC df = spark.sql("SELECT * FROM sales")
# MAGIC display(df)
# MAGIC # Click "Visualizations" to create charts
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo - DataFrame Viz
# 📊 Demo: DataFrame Visualizations

from pyspark.sql.functions import col, sum, count

print("📊 DataFrame Visualization Demo")
print("=" * 60)

# Create sample sales data
data = [
    ("Electronics", "Q1", 50000),
    ("Electronics", "Q2", 65000),
    ("Electronics", "Q3", 70000),
    ("Electronics", "Q4", 85000),
    ("Furniture", "Q1", 30000),
    ("Furniture", "Q2", 35000),
    ("Furniture", "Q3", 38000),
    ("Furniture", "Q4", 42000)
]
df = spark.createDataFrame(data, ["category", "quarter", "revenue"])

print("\n📈 Sales by Category and Quarter:")
print("⭐ Use display() for interactive visualizations!\n")

# Display with interactive viz option
display(df)

print("\n💡 Click 'Visualizations' above to:")
print("   1. Create bar chart (category vs revenue)")
print("   2. Create line chart (quarter vs revenue)")
print("   3. Create pie chart (category distribution)")

# COMMAND ----------

# DBTITLE 1,Custom Visualizations
# MAGIC %md
# MAGIC ## 🎨 Custom Visualizations with Libraries
# MAGIC
# MAGIC ### **Matplotlib** (Python)
# MAGIC ```python
# MAGIC import matplotlib.pyplot as plt
# MAGIC
# MAGIC # Create plot
# MAGIC plt.figure(figsize=(10, 6))
# MAGIC plt.bar(categories, values)
# MAGIC plt.title("Sales by Category")
# MAGIC plt.xlabel("Category")
# MAGIC plt.ylabel("Revenue")
# MAGIC display(plt)  # Display in notebook
# MAGIC ```
# MAGIC
# MAGIC ### **Plotly** (Interactive)
# MAGIC ```python
# MAGIC import plotly.express as px
# MAGIC
# MAGIC # Interactive chart
# MAGIC fig = px.line(df_pandas, x="date", y="revenue", 
# MAGIC               title="Revenue Trend")
# MAGIC fig.show()
# MAGIC ```
# MAGIC
# MAGIC ### **Seaborn** (Statistical)
# MAGIC ```python
# MAGIC import seaborn as sns
# MAGIC
# MAGIC sns.boxplot(data=df_pandas, x="category", y="price")
# MAGIC plt.title("Price Distribution")
# MAGIC display(plt)
# MAGIC ```
# MAGIC
# MAGIC 💡 **Tip**: Convert Spark DF to Pandas for plotting
# MAGIC ```python
# MAGIC df_pandas = df.toPandas()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Dashboard Best Practices
# MAGIC %md
# MAGIC ## ✅ Dashboard Design Best Practices
# MAGIC
# MAGIC ### **1. Know Your Audience**
# MAGIC * **Executives**: High-level KPIs
# MAGIC * **Analysts**: Detailed metrics
# MAGIC * **Operations**: Real-time monitoring
# MAGIC
# MAGIC ### **2. Design Principles**
# MAGIC
# MAGIC #### **Layout**
# MAGIC ✅ Most important metrics at top
# MAGIC ✅ Logical flow (left to right, top to bottom)
# MAGIC ✅ Group related metrics
# MAGIC ✅ Use white space
# MAGIC
# MAGIC #### **Charts**
# MAGIC ✅ Choose right chart type
# MAGIC * Bar: Comparisons
# MAGIC * Line: Trends
# MAGIC * Pie: Proportions
# MAGIC * Table: Detailed data
# MAGIC
# MAGIC ✅ Use consistent colors
# MAGIC ✅ Clear titles and labels
# MAGIC ✅ Avoid 3D charts
# MAGIC
# MAGIC ### **3. Performance**
# MAGIC ✅ Filter data early
# MAGIC ✅ Use aggregated tables
# MAGIC ✅ Set refresh schedules
# MAGIC ✅ Cache frequently used queries
# MAGIC
# MAGIC ### **4. Interactivity**
# MAGIC ✅ Add filters (date, category, region)
# MAGIC ✅ Enable drill-down
# MAGIC ✅ Link related dashboards
# MAGIC ✅ Export options

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Lakeview Dashboards** = Modern BI platform
# MAGIC ✅ **Notebook Viz** = Quick exploration with display()
# MAGIC ✅ **Custom Charts** = Matplotlib, Plotly, Seaborn
# MAGIC ✅ **BI Tools** = Tableau, Power BI integration
# MAGIC ✅ **Design** = Know audience, choose right charts
# MAGIC
# MAGIC ## 🎯 Visualization Options
# MAGIC
# MAGIC | Use Case | Tool | Why |
# MAGIC |----------|------|-----|
# MAGIC | Business dashboard | Lakeview | Shareable, interactive |
# MAGIC | Quick exploration | display() | Built-in, easy |
# MAGIC | Custom plots | Matplotlib/Plotly | Full control |
# MAGIC | Enterprise BI | Tableau/Power BI | Advanced features |
# MAGIC
# MAGIC ## 💡 Key Takeaways
# MAGIC * Use **display()** for quick DataFrame visualization
# MAGIC * Build **Lakeview dashboards** for stakeholders
# MAGIC * Choose **chart type** based on data and message
# MAGIC * Design for your **audience**
# MAGIC * Optimize for **performance**
# MAGIC
# MAGIC ## 🎓 Module 05 Complete!
# MAGIC
# MAGIC You've mastered:
# MAGIC * Databricks SQL & SQL Warehouses
# MAGIC * Dashboard creation and visualization
# MAGIC * BI tool integration
# MAGIC * Design best practices
# MAGIC
# MAGIC ## ➡️ Next: Module 06 - Machine Learning & MLflow
# MAGIC
# MAGIC 🎉 **Congratulations on completing Module 05!**
