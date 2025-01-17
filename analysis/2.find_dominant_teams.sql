-- Databricks notebook source
SELECT team_name,
COUNT(1) as total_races,
SUM(calculated_points) as total_points,
AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) > 100
ORDER BY avg_points DESC

-- COMMAND ----------

