# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1. Read the results.json file using the dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DataType

# COMMAND ----------

results_schema=StructType(fields=[StructField("resultId",IntegerType(),False),StructField("raceId",IntegerType(),True),StructField("driverId",IntegerType(),True),StructField("constructorId",IntegerType(),True),StructField("number",IntegerType(),True),StructField("grid",IntegerType(),True),StructField("position",IntegerType(),True),StructField("positionText",StringType(),True),StructField("positionOrder",IntegerType(),True),StructField("points",FloatType(),True),StructField("laps",IntegerType(),True),StructField("time",StringType(),True),StructField("milliseconds",IntegerType(),True),StructField("fastestLap",IntegerType(),True),StructField("rank",IntegerType(),True),StructField("fastestLapTime",StringType(),True),StructField("fastestLapSpeed",FloatType(),True),StructField("statusId",StringType(),True)])

# COMMAND ----------

results_df=spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Rename columns and add new columns

# COMMAND ----------

results_timestamp_df=add_ingestion_date(results_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_with_columns_df=results_timestamp_df.withColumnRenamed("resultId","result_id").withColumnRenamed("resultDate","result_date").withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("constructorId","constructor_id").withColumnRenamed("positionText","position_text").withColumnRenamed("positionOrder","position_order").withColumnRenamed("fastestLap","fastest_lap").withColumnRenamed("fastestLapTime","fastest_lap_time").withColumnRenamed("fastestLapSpeed","fastest_lap_speed").withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3. Drop unnecessary columns

# COMMAND ----------

results_final_df=results_with_columns_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 . Write output to the processed container in parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC Incremental Load:Method 1:

# COMMAND ----------

#for race_id_list in results_final_df.select("race_id").distinct().collect():
#    if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#        spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id={race_id_list.race_id})")

# COMMAND ----------

#results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC Incremental Load:Method 2:
# MAGIC

# COMMAND ----------

overwrite_partition(results_final_df,'f1_processed','results','race_id')

# COMMAND ----------

#spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

#results_final_df=results_final_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text", "position_order", "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "data_source", "file_date", "ingestion_date", "race_id")

# COMMAND ----------

#if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#    results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
#else:
#    results_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

display(spark.read.parquet("abfss://processed@dlfor1.dfs.core.windows.net/results"))

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_processed.results

# COMMAND ----------

