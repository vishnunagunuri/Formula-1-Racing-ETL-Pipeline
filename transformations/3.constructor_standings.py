# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results)

# COMMAND ----------

from pyspark.sql.functions import sum,count,col,when
constructor_standings_df=race_results.groupBy("race_year","team").agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,col,desc

constructor_rank_spec= Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df=constructor_standings_df.withColumn("rank",rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year==2020"))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.construstor_standings")

# COMMAND ----------

