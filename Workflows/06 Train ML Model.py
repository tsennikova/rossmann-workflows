# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG field_demos

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

mlDF = spark.table("field_demos.rossmann_workflows.sl_rossmann_transactions"

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
