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

def predict_python(model_uri, data):  
  model = mlflow.pyfunc.load_model(model_uri)
  predictions = model.predict(data)
  df = pd.DataFrame(predictions.astype(int),columns=["prediction"])
  return df
preds_df = predict_python("runs:/b08ceac1600646718f61f44dfcad60b5/model",test_x)
preds_df.head(5)

# COMMAND ----------

# Combining dataframes to compare
frames = [test_x.reset_index(drop=True), preds_df.reset_index(drop=True), pd.Series(test_y).rename("Real_Sales")]
goldDF = pd.concat(frames, axis=1)
spark.createDataFrame(goldDF[["Store", "prediction", "Real_Sales"]]).write.format("delta").mode("overwrite").saveAsTable("rossmann_ml_scoring")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gl_rossmann_transactions 
# MAGIC AS SELECT * EXCEPT(a.Store)
# MAGIC   FROM sl_rossmann_transactions a 
# MAGIC     LEFT JOIN rossmann_ml_scoring b ON a.Store=b.Store;
