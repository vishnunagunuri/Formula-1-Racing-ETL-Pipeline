-- Databricks notebook source
-- MAGIC %python
-- MAGIC html="""<h1 style="color:black;text-align:center;font-family:Courier New">Report on F1 Dominant Drivers</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers AS
SELECT driver_name,COUNT(1) as total_races,
SUM(calculated_points) as total_points,
AVG(calculated_points) as avg_points,
RANK() OVER (ORDER BY AVG(calculated_points) DESC) as driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) > 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year,driver_name,COUNT(1) as total_races,
SUM(calculated_points) as total_points,
AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_Rank<=10 )
GROUP BY race_year,driver_name
ORDER BY race_year,avg_points DESC

-- COMMAND ----------

