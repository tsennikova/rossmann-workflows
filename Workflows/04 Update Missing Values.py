# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG field_demos

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE field_demos.rossmann_workflows.sl_rossmann_transactions SET Promo2SinceWeek = 0 WHERE Promo2SinceWeek is null

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE field_demos.rossmann_workflows.sl_rossmann_transactions SET CompetitionDistance = 1000 WHERE CompetitionDistance is null
