-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT races.race_year,
        constructors.name as team_name,  
        drivers.name as driver_name,
        results.position, 
        results.points,
        11-results.position as calculated_points
  FROM f1_processed.results 
  JOIN drivers on (results.driver_id = drivers.driver_id)
  JOIN constructors on (results.constructor_id = constructors.constructor_id)
  JOIN races on (results.race_id = races.race_id)
  WHERE results.position<=10

-- COMMAND ----------

DESC EXTENDED f1_presentation.calculated_race_results

-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_results

-- COMMAND ----------

