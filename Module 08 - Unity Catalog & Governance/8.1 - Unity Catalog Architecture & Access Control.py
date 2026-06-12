# Databricks notebook source
# DBTITLE 1,Module 8.1: Unity Catalog
# MAGIC %md
# MAGIC # 🏛️ Module 8.1: Unity Catalog Architecture & Access Control
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand Unity Catalog architecture
# MAGIC * Master three-level namespace
# MAGIC * Implement access control
# MAGIC * Apply data governance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ What is Unity Catalog?
# MAGIC
# MAGIC **Unity Catalog** is Databricks' unified governance solution for data and AI assets.
# MAGIC
# MAGIC ### **Key Features**
# MAGIC
# MAGIC ✅ **Unified Governance** - Single source of truth
# MAGIC ✅ **Fine-Grained Access** - Table, column, row-level
# MAGIC ✅ **Data Lineage** - Track data flow
# MAGIC ✅ **Audit Logs** - Complete access history
# MAGIC ✅ **Delta Sharing** - Secure data sharing
# MAGIC ✅ **Cross-Cloud** - Works on AWS, Azure, GCP
# MAGIC
# MAGIC ### **Why Unity Catalog?**
# MAGIC * **Centralized** security and governance
# MAGIC * **Simplified** permission management
# MAGIC * **Compliant** with regulations
# MAGIC * **Discoverable** data assets
# MAGIC * **Shareable** across organizations

# COMMAND ----------

# DBTITLE 1,Three-Level Namespace
# MAGIC %md
# MAGIC ## 📚 Three-Level Namespace
# MAGIC
# MAGIC Unity Catalog uses a **three-level namespace** for organizing data.
# MAGIC
# MAGIC ### **Hierarchy**
# MAGIC ```
# MAGIC Catalog
# MAGIC   └── Schema (Database)
# MAGIC         └── Table/View/Volume/Function
# MAGIC ```
# MAGIC
# MAGIC ### **Fully Qualified Names**
# MAGIC ```sql
# MAGIC -- Format: catalog.schema.object
# MAGIC SELECT * FROM main.sales.customers
# MAGIC
# MAGIC -- Components:
# MAGIC -- ├── Catalog: main
# MAGIC -- ├── Schema: sales
# MAGIC -- └── Table: customers
# MAGIC ```
# MAGIC
# MAGIC ### **Catalog**
# MAGIC * Top-level container
# MAGIC * Organizes by business domain
# MAGIC * Examples: `main`, `dev`, `prod`, `analytics`
# MAGIC
# MAGIC ### **Schema (Database)**
# MAGIC * Logical grouping within catalog
# MAGIC * Organize by project or subject
# MAGIC * Examples: `sales`, `marketing`, `finance`
# MAGIC
# MAGIC ### **Objects**
# MAGIC * **Tables** - Structured data
# MAGIC * **Views** - Virtual tables
# MAGIC * **Volumes** - Unstructured files
# MAGIC * **Functions** - SQL/Python UDFs
# MAGIC * **Models** - ML models
# MAGIC
# MAGIC ### **Benefits**
# MAGIC ✅ **Clear organization**
# MAGIC ✅ **Namespace isolation**
# MAGIC ✅ **Granular permissions**
# MAGIC ✅ **Multi-environment support**

# COMMAND ----------

# DBTITLE 1,Demo - Exploring Unity Catalog
# 🏛️ Demo: Exploring Unity Catalog

print("🏛️ Unity Catalog Exploration")
print("=" * 60)

# List catalogs
print("\n📚 Available Catalogs:")
catalogs = spark.sql("SHOW CATALOGS").collect()
for cat in catalogs:
    print(f"   • {cat.catalog}")

# List schemas in current catalog
print("\n📂 Schemas in current catalog:")
schemas = spark.sql("SHOW SCHEMAS").collect()
for schema in schemas[:5]:  # Show first 5
    print(f"   • {schema.databaseName}")

# Show current context
print("\n🎯 Current Context:")
current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
current_schema = spark.sql("SELECT current_schema()").collect()[0][0]
print(f"   Catalog: {current_catalog}")
print(f"   Schema: {current_schema}")

# Switch catalog/schema
print("\n🔄 Switching context:")
spark.sql("USE CATALOG main")
print("   ✅ Using catalog: main")

# Create example objects (if permitted)
print("\n💡 Three-level namespace examples:")
print("   • main.default.customers")
print("   • prod.analytics.revenue")
print("   • dev.sandbox.experiments")

# COMMAND ----------

# DBTITLE 1,Access Control
# MAGIC %md
# MAGIC ## 🔐 Access Control
# MAGIC
# MAGIC ### **Privilege Types**
# MAGIC
# MAGIC #### **Catalog Privileges**
# MAGIC * `USE CATALOG` - Access catalog
# MAGIC * `CREATE SCHEMA` - Create schemas
# MAGIC * `USE SCHEMA` - Access schemas
# MAGIC
# MAGIC #### **Schema Privileges**
# MAGIC * `USE SCHEMA` - Access schema
# MAGIC * `CREATE TABLE` - Create tables
# MAGIC * `CREATE FUNCTION` - Create functions
# MAGIC * `CREATE VOLUME` - Create volumes
# MAGIC
# MAGIC #### **Table Privileges**
# MAGIC * `SELECT` - Read data
# MAGIC * `MODIFY` - Insert/Update/Delete
# MAGIC * `READ_METADATA` - View schema
# MAGIC * `ALL PRIVILEGES` - Full access
# MAGIC
# MAGIC ### **Granting Permissions**
# MAGIC ```sql
# MAGIC -- Grant catalog access
# MAGIC GRANT USE CATALOG ON CATALOG main TO `data_analysts`;
# MAGIC
# MAGIC -- Grant schema access
# MAGIC GRANT USE SCHEMA ON SCHEMA main.sales TO `data_analysts`;
# MAGIC
# MAGIC -- Grant table SELECT
# MAGIC GRANT SELECT ON TABLE main.sales.customers TO `data_analysts`;
# MAGIC
# MAGIC -- Grant with ALL PRIVILEGES
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA main.analytics TO `data_engineers`;
# MAGIC ```
# MAGIC
# MAGIC ### **Revoking Permissions**
# MAGIC ```sql
# MAGIC REVOKE SELECT ON TABLE main.sales.customers FROM `data_analysts`;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Advanced Access Control
# MAGIC %md
# MAGIC ## 🛡️ Advanced Access Control
# MAGIC
# MAGIC ### **Column-Level Security**
# MAGIC ```sql
# MAGIC -- Grant access to specific columns
# MAGIC GRANT SELECT (customer_id, name, email) 
# MAGIC ON TABLE main.sales.customers 
# MAGIC TO `marketing_team`;
# MAGIC ```
# MAGIC
# MAGIC ### **Row-Level Security**
# MAGIC ```sql
# MAGIC -- Create row filter function
# MAGIC CREATE FUNCTION main.sales.customer_filter(region STRING)
# MAGIC RETURN IF(current_user() = 'admin', TRUE, region = 'US');
# MAGIC
# MAGIC -- Apply row filter
# MAGIC ALTER TABLE main.sales.customers 
# MAGIC SET ROW FILTER main.sales.customer_filter ON (region);
# MAGIC ```
# MAGIC
# MAGIC ### **Dynamic Views**
# MAGIC ```sql
# MAGIC -- Create view with user-based filtering
# MAGIC CREATE VIEW main.sales.my_customers AS
# MAGIC SELECT * FROM main.sales.customers
# MAGIC WHERE sales_rep = current_user();
# MAGIC
# MAGIC -- Grant access to view
# MAGIC GRANT SELECT ON VIEW main.sales.my_customers TO `sales_team`;
# MAGIC ```
# MAGIC
# MAGIC ### **Attribute-Based Access Control (ABAC)**
# MAGIC * Tag-based governance
# MAGIC * Automatic policy application
# MAGIC * Classification-driven access
# MAGIC
# MAGIC ```sql
# MAGIC -- Tag sensitive data
# MAGIC ALTER TABLE main.sales.customers 
# MAGIC SET TAGS ('pii' = 'true', 'sensitivity' = 'high');
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Governance Features
# MAGIC %md
# MAGIC ## 📊 Governance Features
# MAGIC
# MAGIC ### **Audit Logs**
# MAGIC Track all access to data assets
# MAGIC
# MAGIC ```sql
# MAGIC -- Query audit logs
# MAGIC SELECT 
# MAGIC     event_time,
# MAGIC     user_identity,
# MAGIC     action_name,
# MAGIC     request_params
# MAGIC FROM system.access.audit
# MAGIC WHERE catalog_name = 'main'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 100;
# MAGIC ```
# MAGIC
# MAGIC ### **Data Discovery**
# MAGIC * Search across catalogs
# MAGIC * Browse schemas and tables
# MAGIC * View table metadata
# MAGIC * Understand relationships
# MAGIC
# MAGIC ### **Data Classification**
# MAGIC * **Tags** - Custom metadata
# MAGIC * **Comments** - Documentation
# MAGIC * **Properties** - Additional attributes
# MAGIC
# MAGIC ```sql
# MAGIC -- Add table comment
# MAGIC COMMENT ON TABLE main.sales.customers IS 
# MAGIC   'Customer master data with PII';
# MAGIC
# MAGIC -- Add column comment
# MAGIC ALTER TABLE main.sales.customers 
# MAGIC ALTER COLUMN email COMMENT 'Customer email (PII)';
# MAGIC ```
# MAGIC
# MAGIC ### **Data Quality**
# MAGIC * Constraints and expectations
# MAGIC * Data validation rules
# MAGIC * Quality metrics
# MAGIC
# MAGIC ```sql
# MAGIC -- Add constraint
# MAGIC ALTER TABLE main.sales.orders
# MAGIC ADD CONSTRAINT valid_amount CHECK (amount > 0);
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Best Practices
# MAGIC %md
# MAGIC ## 🎯 Best Practices
# MAGIC
# MAGIC ### **Catalog Organization**
# MAGIC ✅ **Separate by environment** - `dev`, `staging`, `prod`
# MAGIC ✅ **Organize by domain** - `sales`, `marketing`, `finance`
# MAGIC ✅ **Use clear naming** - Descriptive, consistent
# MAGIC ✅ **Document everything** - Comments on all objects
# MAGIC
# MAGIC ### **Access Control**
# MAGIC ✅ **Principle of least privilege** - Grant minimum access
# MAGIC ✅ **Use groups** - Not individual users
# MAGIC ✅ **Regular audits** - Review permissions quarterly
# MAGIC ✅ **Service principals** - For automation
# MAGIC ✅ **Row/column filters** - For sensitive data
# MAGIC
# MAGIC ### **Governance**
# MAGIC ✅ **Tag sensitive data** - PII, confidential
# MAGIC ✅ **Enable audit logs** - Monitor access
# MAGIC ✅ **Document schemas** - Add comments
# MAGIC ✅ **Set owners** - Accountable for each asset
# MAGIC ✅ **Data quality checks** - Constraints and validation
# MAGIC
# MAGIC ### **Migration**
# MAGIC ✅ **Plan namespace** - Before migration
# MAGIC ✅ **Test access** - Validate permissions
# MAGIC ✅ **Migrate incrementally** - One catalog at a time
# MAGIC ✅ **Update queries** - Use three-level names
# MAGIC ✅ **Train users** - On new structure
# MAGIC
# MAGIC ### **Common Patterns**
# MAGIC ```
# MAGIC Environment-based:
# MAGIC ├── dev (developers full access)
# MAGIC ├── staging (limited write access)
# MAGIC └── prod (read-only for most)
# MAGIC
# MAGIC Domain-based:
# MAGIC ├── sales (sales team)
# MAGIC ├── marketing (marketing team)
# MAGIC └── finance (finance team)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Unity Catalog** - Unified governance for data and AI
# MAGIC ✅ **Three-Level Namespace** - Catalog.Schema.Object
# MAGIC ✅ **Access Control** - Catalog, schema, table, column, row
# MAGIC ✅ **Audit Logs** - Complete access history
# MAGIC ✅ **Data Discovery** - Search and browse assets
# MAGIC ✅ **Governance** - Tags, comments, quality checks
# MAGIC
# MAGIC ## 🎯 Key Concepts
# MAGIC
# MAGIC ### **Namespace Hierarchy**
# MAGIC ```
# MAGIC Catalog (main, dev, prod)
# MAGIC   └── Schema (sales, marketing)
# MAGIC         └── Objects (tables, views, volumes)
# MAGIC ```
# MAGIC
# MAGIC ### **Permission Model**
# MAGIC 1. **Grant catalog access** - `USE CATALOG`
# MAGIC 2. **Grant schema access** - `USE SCHEMA`
# MAGIC 3. **Grant object access** - `SELECT`, `MODIFY`
# MAGIC 4. **Apply row/column filters** - Fine-grained control
# MAGIC
# MAGIC ### **Governance Workflow**
# MAGIC 1. **Organize** - Structure catalogs and schemas
# MAGIC 2. **Secure** - Grant appropriate permissions
# MAGIC 3. **Document** - Add comments and tags
# MAGIC 4. **Monitor** - Review audit logs
# MAGIC 5. **Maintain** - Regular access reviews
# MAGIC
# MAGIC ## 💡 Remember
# MAGIC
# MAGIC * Always use **fully qualified names** - `catalog.schema.table`
# MAGIC * Grant to **groups**, not individual users
# MAGIC * Apply **least privilege** principle
# MAGIC * **Document** all data assets
# MAGIC * Enable and review **audit logs**
# MAGIC * Use **tags** for classification
# MAGIC * Test permissions before production
# MAGIC
# MAGIC ## ➡️ Next: Module 8.2 - Data Lineage & Delta Sharing
# MAGIC
# MAGIC 🎉 **Module 8.1 Complete!**
