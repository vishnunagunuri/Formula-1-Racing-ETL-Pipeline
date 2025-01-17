# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1. Read the constructors.json file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema="constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructor_df=spark.read.schema(constructors_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructer_dropped_df=constructor_df.drop('url')

# COMMAND ----------

constructor_timestamp_df=add_ingestion_date(constructer_dropped_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df=constructor_timestamp_df.withColumnRenamed('constructorId','constructor_id').withColumnRenamed('constructorRef','constructor_ref').withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit("success")