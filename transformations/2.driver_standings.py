# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum,when,col,count
driver_standings_df=race_results.groupBy("race_year","driver_name","driver_nationality","team").agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df.filter("race_year=2019"))

# COMMAND ----------

from pyspark.sql.functions import desc,rank
from pyspark.sql.window import Window

driver_rank_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))

# COMMAND ----------

final_df=driver_standings_df.withColumn("rank",rank().over(driver_rank_spec))  

# COMMAND ----------

display(final_df.filter("race_year=2019").orderBy("rank"))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------
