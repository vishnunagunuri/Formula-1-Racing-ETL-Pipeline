# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1. Read the drivers.json file using the dataframe reader api

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("v_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("v_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType , DateType

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),True),StructField("surname",StringType(),True)])

# COMMAND ----------

drivers_schema=StructType(fields=[StructField("driverId",IntegerType(),True),
                                  StructField("driverRef",StringType(),True),StructField("number",IntegerType(),True),
                                  StructField("code",StringType(),True),
                                  StructField("name",name_schema,True),
                                  StructField("dob",DateType(),True),
                                  StructField("nationality",StringType(),True),
                                  StructField("url",StringType(),True)])

# COMMAND ----------

drivers_df=spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Rename columns and add new columns 

# COMMAND ----------

from pyspark.sql.functions import concat, lit, col

# COMMAND ----------

drivers_timestamp_df=add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_with_columns_df = drivers_timestamp_df.withColumnRenamed("driverId", "driver_id").withColumnRenamed("driverRef", "driver_ref").withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname"))).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3. Drop the unwanted columns

# COMMAND ----------

drivers_final_df=drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4. write to output to the parquet file 

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

dbutils.notebook.exit("success")