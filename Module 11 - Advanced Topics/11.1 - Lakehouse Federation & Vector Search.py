# Databricks notebook source
# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ✅ **Lakehouse Federation** - Query external sources without copying
# MAGIC ✅ **Foreign Catalogs** - Unified SQL interface
# MAGIC ✅ **Query Optimization** - Push down filters and aggregations
# MAGIC ✅ **Vector Search** - Semantic similarity search
# MAGIC ✅ **RAG** - Retrieval Augmented Generation
# MAGIC ✅ **Hybrid Search** - Combine vectors with filters
# MAGIC
# MAGIC ## 🎯 Key Concepts
# MAGIC
# MAGIC ### **Lakehouse Federation Workflow**
# MAGIC
# MAGIC ```sql
# MAGIC -- 1. Create connection
# MAGIC CREATE CONNECTION postgres_prod
# MAGIC TYPE postgresql
# MAGIC OPTIONS (host '...', user '...', password secret('...'));
# MAGIC
# MAGIC -- 2. Create foreign catalog
# MAGIC CREATE FOREIGN CATALOG external_db
# MAGIC USING CONNECTION postgres_prod;
# MAGIC
# MAGIC -- 3. Query external data
# MAGIC SELECT * FROM external_db.public.orders
# MAGIC WHERE date >= '2024-01-01';
# MAGIC
# MAGIC -- 4. Join with local data
# MAGIC SELECT d.name, COUNT(e.order_id)
# MAGIC FROM main.customers d
# MAGIC JOIN external_db.public.orders e ON d.id = e.customer_id
# MAGIC GROUP BY d.name;
# MAGIC ```
# MAGIC
# MAGIC ### **Vector Search Workflow**
# MAGIC
# MAGIC ```python
# MAGIC # 1. Create endpoint
# MAGIC vsc.create_endpoint(name="endpoint", endpoint_type="STANDARD")
# MAGIC
# MAGIC # 2. Create index
# MAGIC vsc.create_delta_sync_index(
# MAGIC     endpoint_name="endpoint",
# MAGIC     index_name="main.default.doc_index",
# MAGIC     source_table_name="main.default.documents",
# MAGIC     primary_key="doc_id",
# MAGIC     embedding_source_column="embedding"
# MAGIC )
# MAGIC
# MAGIC # 3. Query
# MAGIC results = index.similarity_search(
# MAGIC     query_text="machine learning",
# MAGIC     num_results=5
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### **RAG Pattern**
# MAGIC
# MAGIC ```
# MAGIC 1. Retrieve - Vector search for relevant docs
# MAGIC 2. Augment - Add docs to prompt context
# MAGIC 3. Generate - LLM creates answer
# MAGIC 4. Cite - Return sources
# MAGIC ```
# MAGIC
# MAGIC ## 🎯 Best Practices
# MAGIC
# MAGIC ### **Federation**
# MAGIC
# MAGIC ✅ Push down filters to source database
# MAGIC ✅ Select specific columns, not SELECT *
# MAGIC ✅ Use read-only credentials
# MAGIC ✅ Cache frequently accessed data
# MAGIC ✅ Use for enrichment, not large scans
# MAGIC
# MAGIC ### **Vector Search**
# MAGIC
# MAGIC ✅ Chunk documents 200-500 tokens
# MAGIC ✅ Add metadata for filtering
# MAGIC ✅ Use hybrid search (vectors + filters)
# MAGIC ✅ Monitor retrieval quality
# MAGIC ✅ Update index regularly
# MAGIC
# MAGIC ### **RAG**
# MAGIC
# MAGIC ✅ Retrieve top 3-5 most relevant docs
# MAGIC ✅ Include source citations
# MAGIC ✅ Validate answers against context
# MAGIC ✅ Monitor answer quality
# MAGIC ✅ A/B test different configurations
# MAGIC
# MAGIC ## 💡 Remember
# MAGIC
# MAGIC * **Federation** - Query without copying, push down operations
# MAGIC * **Vector Search** - Semantic similarity using embeddings
# MAGIC * **Embeddings** - Vectors representing text meaning
# MAGIC * **RAG** - Combine retrieval with generation
# MAGIC * **Hybrid** - Use filters to narrow vector search
# MAGIC * **Monitoring** - Track performance and quality
# MAGIC
# MAGIC ## 🎓 Module 11.1 Complete!
# MAGIC
# MAGIC You've learned:
# MAGIC * Querying external databases with Federation
# MAGIC * Building semantic search with Vector Search
# MAGIC * Implementing RAG for LLM applications
# MAGIC * Combining vectors and filters for hybrid search
# MAGIC
# MAGIC ## ➡️ Next: Module 11.2 - Databricks Apps & Git Integration
# MAGIC
# MAGIC 🎉 **Congratulations on completing Module 11.1!**

# COMMAND ----------

# DBTITLE 1,Hybrid Search
# MAGIC %md
# MAGIC ## 🔀 Hybrid Search
# MAGIC
# MAGIC **Hybrid Search** combines vector similarity with keyword filters for best results.
# MAGIC
# MAGIC ### **Why Hybrid Search?**
# MAGIC
# MAGIC * **Vector Search** - Semantic similarity (meaning)
# MAGIC * **Keyword Filters** - Exact matches, metadata, dates
# MAGIC * **Together** - Best of both worlds
# MAGIC
# MAGIC ### **Example: Hybrid Query**
# MAGIC
# MAGIC ```python
# MAGIC # Semantic search + metadata filters
# MAGIC results = index.similarity_search(
# MAGIC     query_text="machine learning algorithms",
# MAGIC     filters={
# MAGIC         "category": "AI",
# MAGIC         "date": {"$gte": "2024-01-01"},
# MAGIC         "language": "en"
# MAGIC     },
# MAGIC     num_results=10
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### **Common Filter Patterns**
# MAGIC
# MAGIC #### **1. Category Filtering**
# MAGIC
# MAGIC ```python
# MAGIC # Only search within specific category
# MAGIC filters={"category": "documentation"}
# MAGIC ```
# MAGIC
# MAGIC #### **2. Time-based Filtering**
# MAGIC
# MAGIC ```python
# MAGIC # Only recent documents
# MAGIC filters={"date": {"$gte": "2024-01-01"}}
# MAGIC
# MAGIC # Date range
# MAGIC filters={
# MAGIC     "date": {
# MAGIC         "$gte": "2024-01-01",
# MAGIC         "$lte": "2024-12-31"
# MAGIC     }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC #### **3. Multi-field Filters**
# MAGIC
# MAGIC ```python
# MAGIC # Complex filters
# MAGIC filters={
# MAGIC     "category": "AI",
# MAGIC     "language": "en",
# MAGIC     "confidence": {"$gte": 0.8},
# MAGIC     "status": "published"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### **Filter Operators**
# MAGIC
# MAGIC | Operator | Description | Example |
# MAGIC |----------|-------------|----------|
# MAGIC | `$eq` | Equals | `{"status": {"$eq": "active"}}` |
# MAGIC | `$ne` | Not equals | `{"status": {"$ne": "deleted"}}` |
# MAGIC | `$gt` | Greater than | `{"score": {"$gt": 0.8}}` |
# MAGIC | `$gte` | Greater or equal | `{"date": {"$gte": "2024-01-01"}}` |
# MAGIC | `$lt` | Less than | `{"age": {"$lt": 30}}` |
# MAGIC | `$lte` | Less or equal | `{"date": {"$lte": "2024-12-31"}}` |
# MAGIC | `$contains` | Array contains | `{"tags": {"$contains": "ai"}}` |
# MAGIC
# MAGIC ### **Performance Tips**
# MAGIC
# MAGIC ✅ **Use filters to reduce search space**
# MAGIC ```python
# MAGIC # Narrow search before vector comparison
# MAGIC filters={"category": "docs", "language": "en"}
# MAGIC ```
# MAGIC
# MAGIC ✅ **Index frequently filtered fields**
# MAGIC ```python
# MAGIC # When creating index
# MAGIC indexed_columns=["category", "date", "language"]
# MAGIC ```
# MAGIC
# MAGIC ✅ **Monitor query latency**
# MAGIC ```python
# MAGIC import time
# MAGIC
# MAGIC start = time.time()
# MAGIC results = index.similarity_search(...)
# MAGIC latency = time.time() - start
# MAGIC print(f"Query latency: {latency:.3f}s")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,RAG with Vector Search
# MAGIC %md
# MAGIC ## 🤖 RAG with Vector Search
# MAGIC
# MAGIC **Retrieval Augmented Generation (RAG)** combines vector search with LLMs for accurate, grounded responses.
# MAGIC
# MAGIC ### **What is RAG?**
# MAGIC
# MAGIC RAG retrieves relevant documents and uses them as context for LLM generation.
# MAGIC
# MAGIC ```
# MAGIC User Question
# MAGIC      ↓
# MAGIC Vector Search (find relevant docs)
# MAGIC      ↓
# MAGIC Retrieved Documents (top 5)
# MAGIC      ↓
# MAGIC LLM Prompt (question + documents)
# MAGIC      ↓
# MAGIC Generated Answer (grounded in docs)
# MAGIC ```
# MAGIC
# MAGIC ### **RAG Architecture**
# MAGIC
# MAGIC ```python
# MAGIC def rag_pipeline(question):
# MAGIC     # 1. Retrieve relevant documents
# MAGIC     docs = vector_search.similarity_search(
# MAGIC         query_text=question,
# MAGIC         num_results=5
# MAGIC     )
# MAGIC     
# MAGIC     # 2. Build context from documents
# MAGIC     context = "\n\n".join([
# MAGIC         f"Document {i+1}: {doc['text']}"
# MAGIC         for i, doc in enumerate(docs)
# MAGIC     ])
# MAGIC     
# MAGIC     # 3. Create prompt with context
# MAGIC     prompt = f"""Use the following context to answer the question.
# MAGIC     
# MAGIC Context:
# MAGIC {context}
# MAGIC
# MAGIC Question: {question}
# MAGIC
# MAGIC Answer based only on the context above.
# MAGIC
# MAGIC Answer:"""
# MAGIC     
# MAGIC     # 4. Call LLM
# MAGIC     response = llm.generate(prompt)
# MAGIC     
# MAGIC     # 5. Return answer with sources
# MAGIC     return {
# MAGIC         "answer": response,
# MAGIC         "sources": [doc['doc_id'] for doc in docs]
# MAGIC     }
# MAGIC ```
# MAGIC
# MAGIC ### **Benefits of RAG**
# MAGIC
# MAGIC ✅ **Accurate** - Grounded in your actual data
# MAGIC ✅ **Current** - Always uses latest information
# MAGIC ✅ **Traceable** - See source documents
# MAGIC ✅ **Cost-effective** - No model fine-tuning needed
# MAGIC ✅ **Flexible** - Easy to update knowledge base
# MAGIC ✅ **Trustworthy** - Cites sources
# MAGIC
# MAGIC ### **RAG Best Practices**
# MAGIC
# MAGIC #### **1. Chunk Documents Appropriately**
# MAGIC
# MAGIC ```python
# MAGIC # Good chunk size: 200-500 tokens
# MAGIC def chunk_document(text, chunk_size=400):
# MAGIC     chunks = []
# MAGIC     for i in range(0, len(text), chunk_size):
# MAGIC         chunks.append(text[i:i+chunk_size])
# MAGIC     return chunks
# MAGIC ```
# MAGIC
# MAGIC #### **2. Add Metadata**
# MAGIC
# MAGIC ```python
# MAGIC # Include metadata for filtering
# MAGIC document = {
# MAGIC     "text": "...",
# MAGIC     "metadata": {
# MAGIC         "source": "user_manual.pdf",
# MAGIC         "date": "2024-01-15",
# MAGIC         "category": "documentation"
# MAGIC     }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC #### **3. Use Hybrid Search**
# MAGIC
# MAGIC ```python
# MAGIC # Combine vector search with filters
# MAGIC results = index.similarity_search(
# MAGIC     query_text="machine learning",
# MAGIC     filters={"category": "AI", "date": {"$gte": "2024-01-01"}},
# MAGIC     num_results=5
# MAGIC )
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Vector Search Setup Demo
# 🔍 Demo: Vector Search Setup

from databricks.vector_search.client import VectorSearchClient

print("🔍 Databricks Vector Search Setup")
print("=" * 60)

print("\n📚 Vector Search Workflow:")
print("""\n1. Create Endpoint (compute)
   ↓
2. Create Index on Delta Table
   ↓
3. Query for Similar Vectors
   ↓
4. Get Ranked Results
""")

# ═══════════════════════════════════════════════════════════════
# 1. Create Vector Search Client
# ═══════════════════════════════════════════════════════════════
print("\n1️⃣ Create Vector Search Client:")
print("""\nfrom databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()
""")

# ═══════════════════════════════════════════════════════════════
# 2. Create Endpoint
# ═══════════════════════════════════════════════════════════════
print("\n2️⃣ Create Vector Search Endpoint:")
print("""\nvsc.create_endpoint(
    name="my_vector_endpoint",
    endpoint_type="STANDARD"  # or "STANDARD_SMALL"
)
""")
print("\n   Endpoint provides compute for serving")

# ═══════════════════════════════════════════════════════════════
# 3. Create Vector Search Index
# ═══════════════════════════════════════════════════════════════
print("\n3️⃣ Create Vector Search Index:")
print("""\n# Assume we have a Delta table with documents
# Columns: doc_id, text, embedding (array<double>)

vsc.create_delta_sync_index(
    endpoint_name="my_vector_endpoint",
    index_name="main.default.document_index",
    source_table_name="main.default.documents",
    pipeline_type="TRIGGERED",  # or "CONTINUOUS"
    primary_key="doc_id",
    embedding_source_column="embedding",
    # Optional: Use Databricks embedding model
    embedding_model_endpoint_name="databricks-bge-large-en"
)
""")

# ═══════════════════════════════════════════════════════════════
# 4. Query for Similar Documents
# ═══════════════════════════════════════════════════════════════
print("\n4️⃣ Query for Similar Documents:")
print("""\n# Get index
index = vsc.get_index(
    endpoint_name="my_vector_endpoint",
    index_name="main.default.document_index"
)

# Search by text (auto-embedded)
results = index.similarity_search(
    query_text="What is machine learning?",
    columns=["doc_id", "text", "title"],
    num_results=5
)

# Display results
for doc in results.get('result', {}).get('data_array', []):
    doc_id, text, title, score = doc
    print(f"Doc: {doc_id}")
    print(f"Title: {title}")
    print(f"Score: {score:.4f}")
    print(f"Text: {text[:100]}...")
    print()
""")

print("\n✅ Vector Search Components:")
print("   • Endpoint - Compute for vector serving")
print("   • Index - Vector storage and search engine")
print("   • Embeddings - Vector representations of text")
print("   • Similarity - Distance metric (cosine, L2)")
print("   • Delta Sync - Auto-update from Delta table")

print("\n💡 Key Benefits:")
print("   • Semantic search (meaning-based)")
print("   • Scalable (billions of vectors)")
print("   • Real-time (low latency)")
print("   • Auto-sync with Delta tables")
print("   • Built-in embedding models")

# COMMAND ----------

# DBTITLE 1,Vector Search Introduction
# MAGIC %md
# MAGIC ## 🔍 Vector Search
# MAGIC
# MAGIC **Vector Search** enables semantic similarity search using embeddings.
# MAGIC
# MAGIC ### **What is Vector Search?**
# MAGIC
# MAGIC Find similar items based on **meaning**, not just keywords
# MAGIC
# MAGIC ```
# MAGIC Traditional Keyword Search:
# MAGIC "red car" → Only matches "red" AND "car"
# MAGIC
# MAGIC Vector Search:
# MAGIC "red car" → Matches:
# MAGIC   • "crimson automobile" ✅ (similar meaning)
# MAGIC   • "scarlet vehicle" ✅ (similar meaning)
# MAGIC   • "ruby sedan" ✅ (similar meaning)
# MAGIC   • "blue truck" ❌ (different meaning)
# MAGIC ```
# MAGIC
# MAGIC ### **How It Works**
# MAGIC
# MAGIC 1. **Embeddings** - Convert text to vectors (arrays of numbers)
# MAGIC 2. **Index** - Store vectors in searchable index
# MAGIC 3. **Query** - Convert query to vector
# MAGIC 4. **Search** - Find nearest vectors by distance
# MAGIC 5. **Results** - Return most similar items
# MAGIC
# MAGIC ```
# MAGIC Text: "machine learning"
# MAGIC   ↓ Embedding Model
# MAGIC Vector: [0.23, -0.45, 0.67, ...] (1536 dimensions)
# MAGIC   ↓ Similarity Search
# MAGIC Similar: "artificial intelligence", "deep learning"
# MAGIC ```
# MAGIC
# MAGIC ### **Key Features**
# MAGIC
# MAGIC ✅ **Semantic Search** - Find by meaning, not keywords
# MAGIC ✅ **Similarity Scoring** - Ranked results by relevance
# MAGIC ✅ **Scalable** - Handle billions of vectors
# MAGIC ✅ **Real-time** - Low-latency queries (<100ms)
# MAGIC ✅ **Hybrid Search** - Combine with keyword filters
# MAGIC
# MAGIC ### **Use Cases**
# MAGIC
# MAGIC #### **1. RAG (Retrieval Augmented Generation)**
# MAGIC Find relevant documents for LLM context
# MAGIC
# MAGIC #### **2. Recommendation Systems**
# MAGIC Find similar products, content, users
# MAGIC
# MAGIC #### **3. Semantic Search**
# MAGIC Search documents by meaning
# MAGIC
# MAGIC #### **4. Anomaly Detection**
# MAGIC Find outliers in vector space
# MAGIC
# MAGIC #### **5. Duplicate Detection**
# MAGIC Find near-duplicate records
# MAGIC
# MAGIC #### **6. Question Answering**
# MAGIC Match questions to answers

# COMMAND ----------

# DBTITLE 1,Federation Best Practices
# MAGIC %md
# MAGIC ## 🎯 Federation Best Practices
# MAGIC
# MAGIC ### **Query Optimization**
# MAGIC
# MAGIC #### **✅ Push Down Filters**
# MAGIC
# MAGIC Filters are pushed to the source database for efficient processing
# MAGIC
# MAGIC ```sql
# MAGIC -- ✅ Good: Filter pushed to PostgreSQL
# MAGIC SELECT * FROM postgres_catalog.orders
# MAGIC WHERE date = '2024-01-15' 
# MAGIC   AND status = 'completed';
# MAGIC
# MAGIC -- ❌ Bad: Filter after reading all data
# MAGIC SELECT * FROM (
# MAGIC   SELECT * FROM postgres_catalog.orders
# MAGIC ) 
# MAGIC WHERE date = '2024-01-15';
# MAGIC ```
# MAGIC
# MAGIC #### **✅ Select Specific Columns**
# MAGIC
# MAGIC ```sql
# MAGIC -- ✅ Good: Only needed columns
# MAGIC SELECT id, customer_id, amount 
# MAGIC FROM postgres_catalog.orders;
# MAGIC
# MAGIC -- ❌ Bad: All columns
# MAGIC SELECT * FROM postgres_catalog.orders;
# MAGIC ```
# MAGIC
# MAGIC #### **✅ Aggregate at Source**
# MAGIC
# MAGIC ```sql
# MAGIC -- ✅ Good: Aggregation pushed down
# MAGIC SELECT 
# MAGIC     customer_id, 
# MAGIC     COUNT(*) as order_count,
# MAGIC     SUM(amount) as total
# MAGIC FROM postgres_catalog.orders
# MAGIC GROUP BY customer_id;
# MAGIC ```
# MAGIC
# MAGIC ### **Performance Tips**
# MAGIC
# MAGIC #### **Cache Frequently Accessed Data**
# MAGIC
# MAGIC ```sql
# MAGIC -- Create materialized view for frequent queries
# MAGIC CREATE MATERIALIZED VIEW cached_recent_orders AS
# MAGIC SELECT * FROM postgres_catalog.orders
# MAGIC WHERE date >= CURRENT_DATE - INTERVAL 7 DAYS;
# MAGIC
# MAGIC -- Refresh periodically
# MAGIC REFRESH MATERIALIZED VIEW cached_recent_orders;
# MAGIC ```
# MAGIC
# MAGIC #### **Use for Enrichment, Not Large Scans**
# MAGIC
# MAGIC ```python
# MAGIC # ✅ Good: Enrich large Databricks data with small external lookup
# MAGIC large_databricks_data.join(
# MAGIC     small_external_dimension,
# MAGIC     "customer_id"
# MAGIC )
# MAGIC
# MAGIC # ❌ Avoid: Large scans of external data
# MAGIC external_large_table.join(databricks_small_table, "id")
# MAGIC ```
# MAGIC
# MAGIC ### **Security Best Practices**
# MAGIC
# MAGIC ✅ **Use read-only accounts** for federation
# MAGIC ✅ **Store credentials in secrets** - Never hardcode
# MAGIC ✅ **Grant minimal permissions** - Only needed tables
# MAGIC ✅ **Monitor access** - Review query logs
# MAGIC ✅ **Use network security** - VPC peering, private links
# MAGIC ✅ **Rotate credentials** - Regular updates

# COMMAND ----------

# DBTITLE 1,Federation Setup
# MAGIC %md
# MAGIC ## 🔌 Setting Up Lakehouse Federation
# MAGIC
# MAGIC ### **Step 1: Create Connection**
# MAGIC
# MAGIC Define connection to external database
# MAGIC
# MAGIC ```sql
# MAGIC -- Create connection to PostgreSQL
# MAGIC CREATE CONNECTION postgres_prod
# MAGIC TYPE postgresql
# MAGIC OPTIONS (
# MAGIC   host 'db.example.com',
# MAGIC   port '5432',
# MAGIC   user 'readonly_user',
# MAGIC   password secret('pg_password')  -- Stored in secrets
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- Create connection to Snowflake
# MAGIC CREATE CONNECTION snowflake_prod
# MAGIC TYPE snowflake
# MAGIC OPTIONS (
# MAGIC   host 'account.snowflakecomputing.com',
# MAGIC   user 'databricks_user',
# MAGIC   password secret('snowflake_password'),
# MAGIC   sfWarehouse 'COMPUTE_WH'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### **Step 2: Create Foreign Catalog**
# MAGIC
# MAGIC Create catalog pointing to external database
# MAGIC
# MAGIC ```sql
# MAGIC -- Create foreign catalog
# MAGIC CREATE FOREIGN CATALOG postgres_catalog
# MAGIC USING CONNECTION postgres_prod
# MAGIC OPTIONS (
# MAGIC   database 'production'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### **Step 3: Query External Data**
# MAGIC
# MAGIC Query external tables using standard SQL
# MAGIC
# MAGIC ```sql
# MAGIC -- Query PostgreSQL table from Databricks
# MAGIC SELECT * 
# MAGIC FROM postgres_catalog.public.orders
# MAGIC WHERE order_date >= '2024-01-01'
# MAGIC LIMIT 100;
# MAGIC ```
# MAGIC
# MAGIC ### **Step 4: Join Local and Remote Data**
# MAGIC
# MAGIC ```sql
# MAGIC -- Join Databricks table with PostgreSQL table
# MAGIC SELECT 
# MAGIC     d.customer_name,
# MAGIC     d.email,
# MAGIC     COUNT(p.order_id) as order_count,
# MAGIC     SUM(p.amount) as total_amount
# MAGIC FROM main.customers d
# MAGIC JOIN postgres_catalog.public.orders p
# MAGIC   ON d.customer_id = p.customer_id
# MAGIC WHERE p.order_date >= '2024-01-01'
# MAGIC GROUP BY d.customer_name, d.email
# MAGIC ORDER BY total_amount DESC;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,What is Lakehouse Federation
# MAGIC %md
# MAGIC ## 🌐 What is Lakehouse Federation?
# MAGIC
# MAGIC **Lakehouse Federation** enables querying external databases directly from Databricks without copying data.
# MAGIC
# MAGIC ### **Supported Data Sources**
# MAGIC
# MAGIC * **PostgreSQL**
# MAGIC * **MySQL**
# MAGIC * **SQL Server**
# MAGIC * **Snowflake**
# MAGIC * **Amazon Redshift**
# MAGIC * **Google BigQuery**
# MAGIC * **Oracle Database**
# MAGIC
# MAGIC ### **Key Benefits**
# MAGIC
# MAGIC ✅ **No Data Copy** - Query data where it lives
# MAGIC ✅ **Real-time Access** - Always query current data
# MAGIC ✅ **Unified SQL** - Single query interface for all sources
# MAGIC ✅ **Cost Efficient** - No duplicate storage costs
# MAGIC ✅ **Governance** - Unified access control across sources
# MAGIC
# MAGIC ### **Why Use Federation?**
# MAGIC
# MAGIC * **Reduce ETL complexity** - No need to copy data
# MAGIC * **Real-time insights** - Query latest operational data
# MAGIC * **Simplified architecture** - One query interface
# MAGIC * **Lower costs** - Eliminate duplicate storage
# MAGIC * **Faster time-to-insight** - No waiting for data pipelines

# COMMAND ----------

# DBTITLE 1,Module 11.1 Introduction
# MAGIC %md
# MAGIC # 🌐 Module 11.1: Lakehouse Federation & Vector Search
# MAGIC
# MAGIC ## 📋 Learning Objectives
# MAGIC * Understand Lakehouse Federation
# MAGIC * Query external data sources
# MAGIC * Implement Vector Search
# MAGIC * Build RAG applications
