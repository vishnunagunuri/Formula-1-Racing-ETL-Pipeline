-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

USE f1_processed

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * 
FROM drivers

-- COMMAND ----------

DESC drivers;

-- COMMAND ----------

SELECT * FROM drivers
WHERE nationality = 'British'

-- COMMAND ----------

SELECT *,CONCAT(driver_ref,'-',code) as new_driver_ref
FROM drivers

-- COMMAND ----------

SELECT *,SPLIT(name," ")[0] AS first_name,SPLIt(name,' ')[1] AS surname
FROM drivers

-- COMMAND ----------

SELECT *,current_timestamp() as time
FROM drivers

-- COMMAND ----------

SELECT count(*) FROM drivers WHERE nationality = 'British'

-- COMMAND ----------

SELECT nationality,count(*) FROM drivers GROUP BY nationality

-- COMMAND ----------

