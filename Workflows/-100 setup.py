# Databricks notebook source
# MAGIC %md
# MAGIC This file shoud be run from 'Delta + ML Rossmann' notebook. It mounts the dbfs on the container and performs a table cleanup for multiple runs.

# COMMAND ----------

catalog = "field_demos"acatalog = "field_demos"
spark.sql(f"SET c.catalog = {catalog}")

# COMMAND ----------

DA = UCHelper(catalog=catalog)
DA.cleanup()            # Remove the existing database and files
DA.init(create_db=True) # True is the default

DA.conclude_setup()

# COMMAND ----------

import logging
import re
import warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

aws_bucket_name = "oetrta/tania/rossmann_ml_dbsql_demo"
file_type = "csv"
mount_name = "/tania/rossmann_demo"
try:
  dbutils.fs.mount("s3a://%s" % aws_bucket_name, "/mnt/%s" % mount_name)
except:
  dbutils.fs.unmount("/mnt/%s" % mount_name)
  dbutils.fs.mount("s3a://%s" % aws_bucket_name, "/mnt/%s" % mount_name)
#display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

#Allow schema inference for auto loader

import re
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

checkpoint_path = f"/Users/{current_user}/rossmann_lineage_checkpoint"

#dbutils.fs.rm(mount_name+"/retail/_checkpoint", True)
spark.conf.set("spark.databricks.cloudFiles.schemaInference.enabled", "true")
spark.conf.set("spark.sql.streaming.checkpointLocation", checkpoint_path)
