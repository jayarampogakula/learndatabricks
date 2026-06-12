# Databricks notebook source
# DBTITLE 1,🧭 Module 1.2: Workspace Navigation & UI
# MAGIC %md
# MAGIC # 🧭 Module 1.2: Workspace Navigation & UI
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC By the end of this module, you will:
# MAGIC * Understand the Databricks workspace layout
# MAGIC * Navigate the main UI components
# MAGIC * Create and organize folders and notebooks
# MAGIC * Use the sidebar and navigation features
# MAGIC * Understand user settings and preferences
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏠 Workspace Overview
# MAGIC
# MAGIC The **Databricks Workspace** is your collaborative environment for:
# MAGIC * 📓 Creating and managing notebooks
# MAGIC * 📁 Organizing folders and assets
# MAGIC * 👥 Collaborating with team members
# MAGIC * ⚙️ Managing compute resources
# MAGIC * 📈 Building dashboards and queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Main UI Components
# MAGIC
# MAGIC ### **1. Left Sidebar (Primary Navigation)**
# MAGIC The sidebar provides access to all major features:
# MAGIC
# MAGIC ```
# MAGIC 🏠 Workspace       - Your files and notebooks
# MAGIC 📡 Catalog         - Data assets (Unity Catalog)
# MAGIC 🔍 Compute         - Clusters and SQL warehouses
# MAGIC 📊 SQL             - Query editor and dashboards
# MAGIC 🧪 Workflows       - Jobs and orchestration
# MAGIC 🤖 Machine Learning - MLflow and models
# MAGIC 🔄 Data Engineering - Pipelines (SDP)
# MAGIC 📊 Dashboards      - BI visualizations
# MAGIC 👥 Settings        - Workspace configuration
# MAGIC ```
# MAGIC
# MAGIC ### **2. Top Navigation Bar**
# MAGIC * 🔔 **Notifications**: System alerts and messages
# MAGIC * 🧩 **Assistant**: AI-powered help (Genie)
# MAGIC * ⚙️ **User Menu**: Profile, settings, documentation
# MAGIC * 🔍 **Search**: Global search across all assets
# MAGIC
# MAGIC ### **3. Main Content Area**
# MAGIC * Where notebooks, queries, and dashboards are displayed
# MAGIC * Context-specific based on selected sidebar item
# MAGIC
# MAGIC ### **4. Right Sidebar (Contextual)**
# MAGIC * **Compute selector**: Choose/attach clusters
# MAGIC * **Collaborators**: See who's viewing
# MAGIC * **Comments**: Add inline comments
# MAGIC * **Table of contents**: Navigate long notebooks

# COMMAND ----------

# DBTITLE 1,Working with Folders & Notebooks
# MAGIC %md
# MAGIC ## 📁 Workspace Folder Structure
# MAGIC
# MAGIC ### **Default Folders**
# MAGIC ```
# MAGIC /Workspace
# MAGIC └── /Users
# MAGIC     └── /your-email@company.com     ← Your personal home folder
# MAGIC         ├── my-project/
# MAGIC         ├── notebooks/
# MAGIC         └── experiments/
# MAGIC └── /Shared
# MAGIC     └── team-projects/              ← Shared team folders
# MAGIC ```
# MAGIC
# MAGIC ### **Best Practices for Organization**
# MAGIC ✅ Create project-based folder structures  
# MAGIC ✅ Use descriptive names  
# MAGIC ✅ Separate dev/prod environments  
# MAGIC ✅ Use `/Shared` for team collaboration  
# MAGIC ✅ Leverage Git repos for version control  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📓 Creating & Managing Notebooks
# MAGIC
# MAGIC ### **Create a New Notebook**
# MAGIC 1. Click on **Workspace** in sidebar
# MAGIC 2. Navigate to desired folder
# MAGIC 3. Click **Create** → **Notebook**
# MAGIC 4. Choose:
# MAGIC    * Name
# MAGIC    * Default language (Python, SQL, Scala, R)
# MAGIC    * Cluster (optional)
# MAGIC
# MAGIC ### **Notebook File Types**
# MAGIC * `.ipynb` - Standard Jupyter notebook format
# MAGIC * `.py` - Python files
# MAGIC * `.sql` - SQL scripts
# MAGIC * `.scala` - Scala files
# MAGIC * `.r` - R scripts
# MAGIC
# MAGIC ### **Notebook Operations**
# MAGIC * **Clone**: Create a copy
# MAGIC * **Export**: Download as `.ipynb`, `.html`, `.py`
# MAGIC * **Import**: Upload notebooks from local machine
# MAGIC * **Move**: Reorganize into different folders
# MAGIC * **Share**: Grant access to other users
# MAGIC * **Version Control**: Connect to Git
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⌨️ Keyboard Shortcuts
# MAGIC
# MAGIC Master these shortcuts for productivity:
# MAGIC
# MAGIC | Action | Shortcut |
# MAGIC |--------|----------|
# MAGIC | Run cell | `Shift + Enter` |
# MAGIC | Run all cells | `Ctrl + Shift + Enter` |
# MAGIC | Add cell below | `B` |
# MAGIC | Add cell above | `A` |
# MAGIC | Delete cell | `D, D` (press D twice) |
# MAGIC | Change to code | `Y` |
# MAGIC | Change to markdown | `M` |
# MAGIC | Command palette | `Ctrl + Shift + P` |
# MAGIC | Find/Replace | `Ctrl + F` |
# MAGIC | Save notebook | `Ctrl + S` |
# MAGIC
# MAGIC 💡 **Tip**: Press `H` to see all keyboard shortcuts!

# COMMAND ----------


