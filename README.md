# Rossmann Stores Sales Prediction with Workflows and UC Lineage

<p>In this example, we demonstrate how to forecast sales using store transactions, marketing and competitor's data. </p>
<img src = "https://github.com/tsennikova/databricks-demo/blob/main/Workflows_Lineage.png?raw=true" width="1000">


**Usecase**:

Rossmann operates over 3,000 drug stores in 7 European countries. Currently, Rossmann store managers are tasked with predicting their daily sales. Store sales are influenced by many factors, including promotions, competition, school and state holidays, seasonality, and locality. With thousands of individual managers predicting sales based on their unique circumstances, the accuracy of results can be quite varied.

**Demo Objective**:

* Show unified batch and streaming
* Show data management with Delta (deletes, updates, schema evolution, time trevel)
* Show lineage
* Show Exploratory Data Analysis with pyspark.pandas
* Show how to track ML model parameters, metrics, tags and artifacts
* Show Databricks platform seamless collaboration capability
* Demonstrate the simplicity of managing multiple model runs
* Show building dashboards in SQL Analytics
* Show orchestration with Workflows

**Content:**

The data comtain historical sales for 1,115 Rossmann stores.


**Files**:

* transactions.parquet - historical data including Sales
* store.csv - supplemental information about the stores
* store_states.csv - mapping of German States


Data Source Acknowledgement: This Data Source Provided By Kaggle

*https://www.kaggle.com/c/rossmann-store-sales/data*

<h3>How to Run</h3>

**Cluster Configuration:**
* Configure a cluster with 11+ ML Runtime 
* Make sure that `oetrta-IAM-access` role assumend by the cluster
* Set up the Spark config with: `spark.databricks.dataLineage.enabled true`

**During Demo:**
* Run the notebook: `Delta + ML Rossmann` to show the overall flow and architecture
* For lineage switch to SQL, go to Data, under _field_demos_ catalog you will find database _rossmann_workflows_{your_username}_databricks_com_
The lineage graph should look like that:
<img src = "https://github.com/tsennikova/databricks-demo/blob/main/Lineage.png?raw=true" width="900">
* The dashboard is built on top of `field_demos.rossmann_lineage` schema. Make sure that it stil exists whenever you run it
* Please clean up your tables after demo by running the last cell of the notebook `DA.cleanup()`

**To Demonstrate Workflows:**
* Use files from Workflows folder. Number in the name stands for the order of the task to be configured.
Computation graph after the calculation should look the following way:
<img src = "https://github.com/tsennikova/databricks-demo/blob/main/Workflows.png?raw=true" width="900">
