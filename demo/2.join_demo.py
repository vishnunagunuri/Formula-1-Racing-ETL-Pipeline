# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits").filter("circuit_id<70").withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races").filter("race_year=2019").withColumnRenamed("name","race_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"inner").select(circuits_df.circuit_name ,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

# Left Outer Join
race_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"left").select(circuits_df.circuit_name ,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

# Right Outer Join
race_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"right").select(circuits_df.circuit_name ,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

# Full Outer Join
race_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"full").select(circuits_df.circuit_name ,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Semi Joins: Same as inner join but selects columns only from left table

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"semi")

# COMMAND ----------

# MAGIC %md
# MAGIC Anti Joins: Opposite of semi join.everything in the left df which is not found in right df

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"anti")

# COMMAND ----------

# MAGIC %md
# MAGIC Cross Joins: every record in the left joins with the record in the right and gives a product.

# COMMAND ----------

race_circuits__cross_df=circuits_df.crossJoin(races_df)

# COMMAND ----------

display(race_circuits__cross_df)

# COMMAND ----------

