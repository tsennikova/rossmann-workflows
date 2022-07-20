# Databricks notebook source
catalog = dbutils.jobs.taskValues.get(taskKey="Setup", key="catalog")
db_name = dbutils.jobs.taskValues.get(taskKey="Setup", key="db_name")
spark.sql(f"SET c.catalog = {catalog}")
spark.sql(f"SET da.db_name = {db_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG `${c.catalog}`

# COMMAND ----------

# MAGIC %sql
# MAGIC USE `${da.db_name}`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE br_rossmann_transactions_and_states 
# MAGIC AS SELECT * EXCEPT(a.Store)
# MAGIC   FROM br_rossmann_transactions a 
# MAGIC     LEFT JOIN br_rossmann_states b ON a.Store=b.Store;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE br_rossmann_transactions_enriched
# MAGIC AS SELECT * EXCEPT(a.Store)
# MAGIC   FROM br_rossmann_transactions_and_states  a 
# MAGIC     LEFT JOIN br_rossmann_stores b ON a.Store=b.Store;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sl_rossmann_transactions
# MAGIC AS SELECT * 
# MAGIC   FROM br_rossmann_transactions_enriched
