# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG field_demos

# COMMAND ----------

spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled=false") 

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM field_demos.rossmann_workflows.sl_rossmann_transactions RETAIN 0.01 HOURS
