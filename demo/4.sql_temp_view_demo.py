# Databricks notebook source
# MAGIC %md
# MAGIC ### Accessing Dataframes using SQL 
# MAGIC ### Objectives:
# MAGIC #### Create Temporary views on dataframe
# MAGIC #### Access the view from both SQL and Python cells

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC Local Temporary View

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results
# MAGIC WHERE race_year = 2019

# COMMAND ----------

race_year=2019
race_results_2019_df=spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Global Temporary View

# COMMAND ----------

# MAGIC %md
# MAGIC Global Temporary view unlike local Temp view have access across all notebooks in the same cluster

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES FROM global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.gv_race_results

# COMMAND ----------

