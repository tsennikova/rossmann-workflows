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

spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled=false") 

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM sl_rossmann_transactions RETAIN 0.01 HOURS
