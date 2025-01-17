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

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType,IntegerType, StringType,DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField('circuitId',IntegerType(),False),StructField('CircuitRef',StringType(),True),StructField('name',StringType(),True),StructField('location',StringType(),True),StructField('country',StringType(),True),StructField('lat',DoubleType(),True),StructField('lng',DoubleType(),True),StructField('alt',IntegerType(),True),StructField('url',StringType(),True)])

# COMMAND ----------

circuits_df=spark.read.option("header",True)\
    .schema(circuits_schema).\
        csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select Only Required Columns

# COMMAND ----------

circuits_select_df=circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

display(circuits_select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Instead we can use the below method, which will let us perform other operations on column names.Like we can change the column name.

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_select_df=circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename Columns As Required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df=circuits_select_df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").withColumnRenamed("lat","latitude").withColumnRenamed("lng","longitude").withColumnRenamed("alt","altitude").withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Ingestion date to the dataframe

# COMMAND ----------

circuits_final_df=add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("success")