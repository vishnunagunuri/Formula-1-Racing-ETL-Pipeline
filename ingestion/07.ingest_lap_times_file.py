# Databricks notebook source
# MAGIC %md
# MAGIC #### step 1.Read the csv file using the spark dataframe reader api

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

lap_times_schema= StructType(fields=[StructField("raceId", IntegerType(), False),StructField("driverId", IntegerType(), True),StructField("lap", IntegerType(), True),StructField("position", IntegerType(), True),StructField("time",StringType(), True),StructField("duration", StringType(), True),StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

lap_times_df=spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 . Rename Columns and add new columns

# COMMAND ----------

lap_times_timestamp_df=add_ingestion_date(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df=lap_times_timestamp_df.withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3.write to output to processed container in the parquet format

# COMMAND ----------

overwrite_partition(final_df,"f1_processed","lap_times","race_id")

# COMMAND ----------

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

display(spark.read.parquet("abfss://processed@dlfor1.dfs.core.windows.net/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,COUNT(1)
# MAGIC FROM f1_processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_processed.lap_times