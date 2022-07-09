# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG field_demos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE field_demos.rossmann_workflows.br_rossmann_transactions_and_states 
# MAGIC AS SELECT * EXCEPT(a.Store)
# MAGIC   FROM field_demos.rossmann_workflows.br_rossmann_transactions a 
# MAGIC     LEFT JOIN field_demos.rossmann_workflows.br_rossmann_states b ON a.Store=b.Store;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE field_demos.rossmann_workflows.br_rossmann_transactions_enriched
# MAGIC AS SELECT * EXCEPT(a.Store)
# MAGIC   FROM field_demos.rossmann_workflows.br_rossmann_transactions_and_states  a 
# MAGIC     LEFT JOIN field_demos.rossmann_workflows.br_rossmann_stores b ON a.Store=b.Store;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE field_demos.rossmann_workflows.sl_rossmann_transactions
# MAGIC AS SELECT * 
# MAGIC   FROM field_demos.rossmann_workflows.br_rossmann_transactions_enriched
