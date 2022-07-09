# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG field_demos

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
spark.createDataFrame(goldDF[["Store", "prediction", "Real_Sales"]]).write.format("delta").mode("overwrite").saveAsTable("field_demos.rossmann_workflows.rossmann_ml_scoring")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE field_demos.rossmann_workflows.gl_rossmann_transactions 
# MAGIC AS SELECT * EXCEPT(a.Store)
# MAGIC   FROM field_demos.rossmann_workflows.sl_rossmann_transactions a 
# MAGIC     LEFT JOIN field_demos.rossmann_workflows.rossmann_ml_scoring b ON a.Store=b.Store;
