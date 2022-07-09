# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG field_demos

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM field_demos.rossmann_workflows.sl_rossmann_transactions WHERE CompetitionOpenSinceMonth is null or CompetitionOpenSinceMonth is null
