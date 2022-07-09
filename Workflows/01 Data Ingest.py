# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG field_demos

# COMMAND ----------

# Load the data from its source.
bronzeDF = spark.readStream \
                .format("cloudFiles") \
                .option("cloudFiles.format", "parquet") \
                .option("cloudFiles.maxFilesPerTrigger", 1) \
                .schema("Store long, DayOfWeek long, Date timestamp, Sales long, Customers long, Open long, Promo long, StateHoliday string, SchoolHoliday long") \
                .load("/mnt/tania/rossmann_demo/parquet/") 

# Write the data to its target.
bronzeDF.writeStream \
        .trigger(once=True) \
        .option("mergeSchema", "true")\
        .format("delta") \
        .table("field_demos.rossmann_workflows.br_rossmann_transactions")

# COMMAND ----------

statesDF = spark.read.csv('/mnt/tania/rossmann_demo/store_states_ll.csv', header = True, schema="Store long, State string, Latitude float, Longitude float")

write_format = "delta"
table_name = "field_demos.rossmann_workflows.br_rossmann_states"

# Write the data to its target.
statesDF.write \
  .format(write_format) \
  .saveAsTable(table_name)

# COMMAND ----------

storeDF = spark.read.csv('/mnt/tania/rossmann_demo/store.csv', 
                          header = True, 
                          schema="Store long, StoreType string, Assortment string, CompetitionDistance long, CompetitionOpenSinceMonth long, CompetitionOpenSinceYear long, Promo2 long, Promo2SinceWeek long, Promo2SinceYear long, PromoInterval string")

table_name = "field_demos.rossmann_workflows.br_rossmann_stores"

# Write the data to its target.
storeDF.write \
  .format(write_format) \
  .saveAsTable(table_name)
