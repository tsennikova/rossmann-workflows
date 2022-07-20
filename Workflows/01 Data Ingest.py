# Databricks notebook source
catalog = dbutils.jobs.taskValues.get(taskKey="Setup", key="catalog")
db_name = dbutils.jobs.taskValues.get(taskKey="Setup", key="db_name")
spark.sql(f"SET c.catalog = {catalog}")
spark.sql(f"SET da.db_name = {db_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG `${c.catalog}`

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS `${da.db_name}`;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE `${da.db_name}`;

# COMMAND ----------

# Load the data from its source.
bronzeDF = spark.read \
                .format("parquet")\
                .schema("Store long, DayOfWeek long, Date timestamp, Sales long, Customers long, Open long, Promo long, StateHoliday string, SchoolHoliday long") \
                .load("/mnt/tania/rossmann_demo/parquet/") 

write_format = "delta"
table_name = "br_rossmann_transactions"

# Write the data to its target.
bronzeDF.write \
        .format(write_format) \
        .saveAsTable(table_name)

# COMMAND ----------

statesDF = spark.read.csv('/mnt/tania/rossmann_demo/store_states_ll.csv', header = True, schema="Store long, State string, Latitude float, Longitude float")

table_name = "br_rossmann_states"

# Write the data to its target.
statesDF.write \
  .format(write_format) \
  .saveAsTable(table_name)

# COMMAND ----------

storeDF = spark.read.csv('/mnt/tania/rossmann_demo/store.csv', 
                          header = True, 
                          schema="Store long, StoreType string, Assortment string, CompetitionDistance long, CompetitionOpenSinceMonth long, CompetitionOpenSinceYear long, Promo2 long, Promo2SinceWeek long, Promo2SinceYear long, PromoInterval string")

table_name = "br_rossmann_stores"

# Write the data to its target.
storeDF.write \
  .format(write_format) \
  .saveAsTable(table_name)
