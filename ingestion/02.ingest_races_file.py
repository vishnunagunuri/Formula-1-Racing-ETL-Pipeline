# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1.Read the CSV file using the spark dataframe reader

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

from pyspark.sql.types import StructField, StructType,IntegerType, StringType,DoubleType,DataType,TimestampType


# COMMAND ----------

from pyspark.sql.types import DateType
races_schema=StructType(fields=[StructField('raceId',IntegerType(),False),StructField('year',IntegerType(),True),StructField('round',IntegerType(),True),StructField('circuitId',IntegerType(),True),StructField('name',StringType(),True),StructField('date',DateType(),True),StructField('time',StringType(),True),StructField('url',StringType(),True)])

# COMMAND ----------

races_df=spark.read.option("header",True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2:Add Ingestion_date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,col,lit,concat

# COMMAND ----------

races_timestamp_df=add_ingestion_date(races_df)

# COMMAND ----------

races_renamed_df=races_timestamp_df.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss")).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3. Select the required columnsa and rename if required

# COMMAND ----------

races_select_df=races_renamed_df.select(col("raceId").alias("race_id"),col("year").alias("race_year"),col("round"),col("circuitId").alias("circuit_id"),col("name"),col("ingestion_date"),col("race_timestamp"),col("data_source"),col("file_date"))

# COMMAND ----------

display(races_select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4.Write the output in the parquet format to the processed folder

# COMMAND ----------

races_select_df.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

display(races_select_df)

# COMMAND ----------

dbutils.notebook.exit("success")