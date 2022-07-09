# Databricks notebook source
# MAGIC %md
# MAGIC This file shoud be run from 'Delta + ML Rossmann' notebook. It mounts the dbfs on the container and performs a table cleanup for multiple runs.

# COMMAND ----------

from delta.tables import *
import pandas as pd
import logging
logging.getLogger('py4j').setLevel(logging.ERROR)
from pyspark.sql.functions import to_date, col
import tempfile
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType, input_file_name
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
dbutils.fs.rm(mount_name+"/retail/_checkpoint", True)
spark.conf.set("spark.databricks.cloudFiles.schemaInference.enabled", "true")
spark.conf.set("spark.sql.streaming.checkpointLocation","dbfs:/<checkpoint-path>")

# COMMAND ----------

spark.sql("""drop table if exists field_demos.rossmann_lineage.br_rossmann_states""")
spark.sql("""drop table if exists field_demos.rossmann_lineage.br_rossmann_transactions_and_states""")
spark.sql("""drop table if exists field_demos.rossmann_lineage.gl_rossmann_transactions""")

spark.sql("""drop table if exists field_demos.rossmann_lineage.br_rossmann_stores""")
spark.sql("""drop table if exists field_demos.rossmann_lineage.gl_rossmann_transactions""")
spark.sql("""drop table if exists field_demos.rossmann_lineage.br_rossmann_transactions_enriched""")
spark.sql("""drop table if exists field_demos.rossmann_lineage.rossmann_ml_scoring""")

spark.sql("""drop table if exists field_demos.rossmann_lineage.sl_rossmann_transactions""")

# COMMAND ----------


