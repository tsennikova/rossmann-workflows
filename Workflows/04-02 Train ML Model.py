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

import pandas as pd
import xgboost as xgb
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from pyspark.ml import Pipeline
import mlflow.xgboost
from pyspark.ml.feature import StringIndexer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# COMMAND ----------

mlDF = spark.table("sl_rossmann_transactions")

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

    model_uri = f"runs:/{run.info.run_uuid}/model" # model identifier representing this run!
  return { "model_uri":model_uri, 'loss': -1*r2, 'booster': booster.attributes()}

# COMMAND ----------

mlflow.set_experiment("/Users/tatiana.sennikova@databricks.com/rossmann_workflows")
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

def predict_python(model_uri, data):  
  model = mlflow.pyfunc.load_model(model_uri)
  predictions = model.predict(data)
  df = pd.DataFrame(predictions.astype(int),columns=["prediction"])
  return df
preds_df = predict_python(result["model_uri"],test_x)
# Combining dataframes to compare
frames = [test_x.reset_index(drop=True), preds_df.reset_index(drop=True), pd.Series(test_y).rename("Real_Sales")]
goldDF = pd.concat(frames, axis=1)

# COMMAND ----------

spark.createDataFrame(goldDF[["Store", "prediction", "Real_Sales"]]).write.format("delta").mode("overwrite").saveAsTable("rossmann_ml_scoring")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS gl_rossmann_transactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gl_rossmann_transactions 
# MAGIC AS SELECT * EXCEPT(a.Store)
# MAGIC   FROM sl_rossmann_transactions a 
# MAGIC     LEFT JOIN rossmann_ml_scoring b ON a.Store=b.Store;
