# Databricks notebook source
# MAGIC %md
# MAGIC #Rossmann Stores Sales Prediction
# MAGIC <p>In this example, we demonstrate how to forecast sales using store, promotion, and competitor data. </p>
# MAGIC <img src = "https://github.com/tsennikova/databricks-demo/blob/main/Rossmann%20Sales.png?raw=true" width="1000">
# MAGIC 
# MAGIC 
# MAGIC **Usecase**:
# MAGIC 
# MAGIC Rossmann operates over 3,000 drug stores in 7 European countries. Currently, Rossmann store managers are tasked with predicting their daily sales. Store sales are influenced by many factors, including promotions, competition, school and state holidays, seasonality, and locality. With thousands of individual managers predicting sales based on their unique circumstances, the accuracy of results can be quite varied.
# MAGIC 
# MAGIC **Demo Objective**:
# MAGIC 
# MAGIC * Show change data capture from streaming source
# MAGIC * Show unified batch and streaming
# MAGIC * Show data management with Delta (deletes, updates, schema evolution, time trevel)
# MAGIC * Show cluster management capability
# MAGIC * Show how to track ML model parameters, metrics, tags and artifacts
# MAGIC * Show Databricks platform seamless collaboration capability
# MAGIC * Demonstrate the simplicity of managing multiple model runs
# MAGIC * Show model deployment and serving via Rest API and batch
# MAGIC * Show model versioning and state transition from Staging to Production
# MAGIC * Show building dashboards in SQL Analytics
# MAGIC 
# MAGIC **Content:**
# MAGIC 
# MAGIC The data comtain historical sales for 1,115 Rossmann stores.
# MAGIC 
# MAGIC 
# MAGIC **Files**:
# MAGIC 
# MAGIC * transactions.parquet - historical data including Sales
# MAGIC * store.csv - supplemental information about the stores
# MAGIC * store_states.csv - mapping of German States
# MAGIC 
# MAGIC We will use Gradient Boosted Tree Regression to predict Sales
# MAGIC 
# MAGIC Once the model is trained, we'll use MFLow to track its performance and save it in the registry to deploy it in production
# MAGIC 
# MAGIC Data Source Acknowledgement: This Data Source Provided By Kaggle
# MAGIC 
# MAGIC *https://www.kaggle.com/c/rossmann-store-sales/data*

# COMMAND ----------

# DBTITLE 1,Cross Notebook Reference
# MAGIC %run ./setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Ingest

# COMMAND ----------

display(dbutils.fs.ls("/mnt/%s" % "/tania/rossmann_demo"))

# COMMAND ----------

display(spark.read.format("parquet").schema("Store long, DayOfWeek long, Date timestamp, Sales long, Customers long, Open long, Promo long, StateHoliday string, SchoolHoliday long").load("dbfs:/mnt/tania/rossmann_demo/parquet/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG field_demos

# COMMAND ----------

#%sql
#CREATE SCHEMA field_demos.rossmann_lineage

# COMMAND ----------

# DBTITLE 1,Open Stream for Transactions Table
# Load the data from its source.
bronzeDF = spark.readStream \
                .format("cloudFiles") \
                .option("cloudFiles.format", "parquet") \
                .option("cloudFiles.maxFilesPerTrigger", 1) \
                .schema("Store long, DayOfWeek long, Date timestamp, Sales long, Customers long, Open long, Promo long, StateHoliday string, SchoolHoliday long") \
                .load("/mnt/tania/rossmann_demo/parquet/") 

# Write the data to its target.
bronzeDF.writeStream \
        .trigger(processingTime='1 seconds') \
        .option("mergeSchema", "true")\
        .format("delta") \
        .table("field_demos.rossmann_lineage.br_rossmann_transactions")

# COMMAND ----------

# DBTITLE 1,Read States Mapping Table as Batch
statesDF = spark.read.csv('/mnt/tania/rossmann_demo/store_states_ll.csv', header = True, schema="Store long, State string, Latitude float, Longitude float")
display(statesDF)

# COMMAND ----------

write_format = "delta"
table_name = "field_demos.rossmann_lineage.br_rossmann_states"

# Write the data to its target.
statesDF.write \
  .format(write_format) \
  .saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Merge Batch and Stream
# MAGIC %sql
# MAGIC CREATE TABLE field_demos.rossmann_lineage.br_rossmann_transactions_and_states 
# MAGIC AS SELECT * EXCEPT(a.Store)
# MAGIC   FROM field_demos.rossmann_lineage.br_rossmann_transactions a 
# MAGIC     LEFT JOIN field_demos.rossmann_lineage.br_rossmann_states b ON a.Store=b.Store;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM field_demos.rossmann_lineage.br_rossmann_transactions_and_states 

# COMMAND ----------

# DBTITLE 1,Read Store Table as Batch
storeDF = spark.read.csv('/mnt/tania/rossmann_demo/store.csv', 
                          header = True, 
                          schema="Store long, StoreType string, Assortment string, CompetitionDistance long, CompetitionOpenSinceMonth long, CompetitionOpenSinceYear long, Promo2 long, Promo2SinceWeek long, Promo2SinceYear long, PromoInterval string")

# COMMAND ----------

display(storeDF)

# COMMAND ----------

table_name = "field_demos.rossmann_lineage.br_rossmann_stores"

# Write the data to its target.
storeDF.write \
  .format(write_format) \
  .saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Enrich Transaction Data with Information about Stores
# MAGIC %sql
# MAGIC CREATE TABLE field_demos.rossmann_lineage.br_rossmann_transactions_enriched
# MAGIC AS SELECT * EXCEPT(a.Store)
# MAGIC   FROM field_demos.rossmann_lineage.br_rossmann_transactions_and_states  a 
# MAGIC     LEFT JOIN field_demos.rossmann_lineage.br_rossmann_stores b ON a.Store=b.Store;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM field_demos.rossmann_lineage.br_rossmann_transactions_enriched

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM field_demos.rossmann_lineage.br_rossmann_transactions_enriched where

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Cleaning

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE field_demos.rossmann_lineage.sl_rossmann_transactions
# MAGIC AS SELECT * 
# MAGIC   FROM field_demos.rossmann_lineage.br_rossmann_transactions_enriched

# COMMAND ----------

# DBTITLE 1,Data Analysis with Koalas
import pyspark.pandas as pd
silver_df = pd.read_table("field_demos.rossmann_lineage.sl_rossmann_transactions")

# COMMAND ----------

# DBTITLE 1,Check for the NULL values
silver_df.isnull().sum()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Delta DML Support
# MAGIC Delta Lake supports standard DML including UPDATE, DELETE and MERGE INTO providing developers more controls to manage their big datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete and Update Support
# MAGIC Let's `DELETE` `NULL` values from `CompetitionOpenSinceMonth`

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM field_demos.rossmann_lineage.sl_rossmann_transactions WHERE CompetitionOpenSinceMonth is null or CompetitionOpenSinceMonth is null

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Support
# MAGIC Let's `UPDATE` `NULL` values in `Promo2SinceWeek`

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE field_demos.rossmann_lineage.sl_rossmann_transactions SET Promo2SinceWeek = 0 WHERE Promo2SinceWeek is null

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE field_demos.rossmann_lineage.sl_rossmann_transactions ADD COLUMNS (SalesPerCustomer double);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM field_demos.rossmann_lineage.sl_rossmann_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE field_demos.rossmann_lineage.sl_rossmann_transactions SET SalesPerCustomer = Sales / Customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM field_demos.rossmann_lineage.sl_rossmann_transactions

# COMMAND ----------

# MAGIC %md ## Let's Travel back in Time!
# MAGIC Databricks Delta’s time travel capabilities simplify building data pipelines for the following use cases. 
# MAGIC 
# MAGIC * Audit Data Changes
# MAGIC * Reproduce experiments & reports
# MAGIC * Rollbacks
# MAGIC 
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
# MAGIC 
# MAGIC You can query by:
# MAGIC 1. Using a timestamp
# MAGIC 1. Using a version number
# MAGIC 
# MAGIC using Python, Scala, and/or Scala syntax; for these examples we will use the SQL syntax.  
# MAGIC 
# MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

# COMMAND ----------

# MAGIC %md ### Review Delta Lake Table History
# MAGIC All the transactions for this table are stored within this table including the initial set of insertions, update, delete, merge, and inserts with schema modification

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY field_demos.rossmann_lineage.sl_rossmann_transactions

# COMMAND ----------

# MAGIC %md ### Time Travel via Version Number
# MAGIC Below are SQL syntax examples of Delta Time Travel by using a Version Number

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(case when CompetitionOpenSinceMonth is null then 1 else 0 end) as CompetitionOpenSinceMonth_NULL from field_demos.rossmann_lineage.sl_rossmann_transactions VERSION AS OF 0

# COMMAND ----------

# MAGIC %md ### Vacuum
# MAGIC By default, Delta Lake retains table history for 30 days and makes it available for “time travel” and rollbacks.
# MAGIC If you determine that GDPR or CCPA compliance requires that these stale records be made unavailable for querying before the default retention period is up, you can use the VACUUM function to remove files that are no longer referenced by a Delta table and are older than a specified retention threshold. Once you have removed table history using the VACUUM command, all users lose the ability to view that history and roll back.

# COMMAND ----------

# MAGIC %md
# MAGIC To remove artifacts younger than 7 days, use the RETAIN num HOURS option:

# COMMAND ----------

spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled=false") 

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM field_demos.rossmann_lineage.sl_rossmann_transactions RETAIN 0.01 HOURS

# COMMAND ----------

# MAGIC %md ##Machine Learning with Databricks

# COMMAND ----------

import pandas as pd
import xgboost as xgb
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from pyspark.ml import Pipeline
import mlflow.xgboost
from pyspark.ml.feature import StringIndexer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# COMMAND ----------

# MAGIC %md ###Data Preparation

# COMMAND ----------

mlDF = spark.table("field_demos.rossmann_lineage.sl_rossmann_transactions") 

# COMMAND ----------

train_x, test_x, train_y, test_y = train_test_split(mlDF.select("Store", "DayOfWeek",  "Customers", "Open", "Promo",
                                                                "SchoolHoliday", "Latitude", "Longitude", 
                                                                "CompetitionDistance", "CompetitionOpenSinceMonth",
                                                                "CompetitionOpenSinceYear", "Promo2", "Promo2SinceWeek",
                                                                "Promo2SinceYear").toPandas(),
                                                    mlDF.select("Sales").toPandas().values.ravel(), 
                                                    random_state=42)

train = xgb.DMatrix(data=train_x, label=train_y)
test = xgb.DMatrix(data=test_x, label=test_y)

# COMMAND ----------

def train_model(params):
  with mlflow.start_run(nested=True) as run:
   
    booster = xgb.train(params=params, dtrain=train, num_boost_round=1000,\
                        evals=[(test, "test")], early_stopping_rounds=10, verbose_eval=100)   
    predictions_test = booster.predict(test)
    
    mae = mean_absolute_error(test_y, predictions_test)
    mse = mean_squared_error(test_y, predictions_test)
    r2 = r2_score (test_y, predictions_test)
    
    mlflow.log_metric('mae', mae)
    mlflow.log_metric('mse', mse)
    mlflow.log_metric('r2', r2)
    
    model_uri = f"runs:/{run.info.run_uuid}/model" # model identifier representing this run!
    return { "model_uri":model_uri, 'loss': -1*r2, 'booster': booster.attributes()}

# COMMAND ----------

mlflow.xgboost.autolog()
result = train_model(
  {
    'learning_rate': 0.4,
     'max_depth': 65,
     'min_child_weight': 0.6,
     'reg_alpha': 0.15,
     'reg_lambda': 0.2,
     'objective': 'reg:squarederror'
  }
)
r2 = -1*result['loss']
r2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run predictions on python [Single node]

# COMMAND ----------

def predict_python(model_uri, data):  
  model = mlflow.pyfunc.load_model(model_uri)
  predictions = model.predict(data)
  df = pd.DataFrame(predictions.astype(int),columns=["prediction"])
  return df
preds_df = predict_python(result["model_uri"],test_x)
preds_df.head(5)

# COMMAND ----------

# Combining dataframes to compare
frames = [test_x.reset_index(drop=True), preds_df.reset_index(drop=True), pd.Series(test_y).rename("Real_Sales")]
goldDF = pd.concat(frames, axis=1)
display(goldDF.loc[goldDF['Store'] < 50])

# COMMAND ----------

spark.createDataFrame(goldDF[["Store", "prediction", "Real_Sales"]]).write.format("delta").mode("overwrite").saveAsTable("field_demos.rossmann_lineage.rossmann_ml_scoring")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE field_demos.rossmann_lineage.gl_rossmann_transactions 
# MAGIC AS SELECT * EXCEPT(a.Store)
# MAGIC   FROM field_demos.rossmann_lineage.sl_rossmann_transactions a 
# MAGIC     LEFT JOIN field_demos.rossmann_lineage.rossmann_ml_scoring b ON a.Store=b.Store;

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Our data is ready! Let's create a dashboard to monitor the sales
# MAGIC 
# MAGIC https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/f9236ce5-9481-4fa1-a184-8d6c951b755f?o=1444828305810485

# COMMAND ----------

# MAGIC %md
# MAGIC <img src = "https://github.com/tsennikova/databricks-demo/blob/main/Rossmann%20Sales.png?raw=true" width="1000">

# COMMAND ----------


