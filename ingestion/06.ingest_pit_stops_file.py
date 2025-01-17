# Databricks notebook source
# MAGIC %md
# MAGIC #### step 1.Read the json file using the spark dataframe reader api

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

pit_stops_schema= StructType(fields=[StructField("raceId", IntegerType(), False),StructField("driverId", IntegerType(), True),StructField("stop", StringType(), True),StructField("lap", IntegerType(), True),StructField("time",StringType(), True),StructField("duration", StringType(), True),StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

pit_stops_df=spark.read.schema(pit_stops_schema).option("multiline",True).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 . Rename Columns and add new columns

# COMMAND ----------

pit_stops_timestamp_df=add_ingestion_date(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df=pit_stops_timestamp_df.withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumn("data_source",lit(v_data_source)).withColumn("file_date",(lit(v_file_date)))

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3.write to output to processed container in the parquet format

# COMMAND ----------

overwrite_partition(final_df,'f1_processed','pit_stops','race_id')

# COMMAND ----------

#final_df.write.mode("append").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_processed.pit_stops;

# COMMAND ----------

