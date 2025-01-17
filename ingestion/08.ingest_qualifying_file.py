# Databricks notebook source
# MAGIC %md
# MAGIC #### step 1.Read the folder that has multiple multiline json files using the spark dataframe reader api

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

from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, StructField

# COMMAND ----------

qualifying_schema= StructType(fields=[StructField("qualifyId", IntegerType(), False),StructField("raceId",IntegerType(), True),StructField("driverId", IntegerType(), True),StructField("constructorId",IntegerType(),True),StructField("number", IntegerType(), True),StructField("position", IntegerType(), True),StructField("q1",StringType(), True),StructField("q2", StringType(), True),StructField("q3", StringType(), True)])

# COMMAND ----------

qualifying_df=spark.read.schema(qualifying_schema).option("multiline",True).json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 . Rename Columns and add new columns

# COMMAND ----------

qualifying_timestamp_df=add_ingestion_date(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df=qualifying_timestamp_df.withColumnRenamed("qualifyId","qualify_id").withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("constructorId","constructor_id").withColumn("data source",lit(v_data_source))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3.write to output to processed container in the parquet format

# COMMAND ----------

overwrite_partition(final_df,"f1_processed","qualifying","race_id")

# COMMAND ----------

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,COUNT(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_processed.qualifying