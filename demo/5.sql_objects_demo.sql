-- Databricks notebook source
-- MAGIC %md
-- MAGIC  **Lesson Objectives**
-- MAGIC -  1.Spark SQL Documentation
-- MAGIC -  2.Create Database Demo
-- MAGIC -  3.Data-catalog tab in the UI
-- MAGIC -  4.SHOW command
-- MAGIC -  5.DESCRIBE command
-- MAGIC - 6.Find the Current Database
-- MAGIC

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE demo

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - CREATE managed table using Python
-- MAGIC - CREATE manages table using SQL
-- MAGIC - Effect of Dropping a managed table
-- MAGIC - Describe Table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT * FROM race_results_python
WHERE race_year = 2019

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS race_results_sql
AS 
SELECT * FROM race_results_python
  WHERE race_year = 2019;


-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESC EXTENDED race_results_sql

-- COMMAND ----------

DROP TABLE race_results_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Learning Objectives:**
-- MAGIC - Create External Tables using Python
-- MAGIC - Create External Tables using SQL
-- MAGIC - Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
USING PARQUET
LOCATION "abfss://presentation@dlfor1.dfs.core.windows.net/race_results_ext_sql"
AS 
SELECT * FROM race_results_ext_py
WHERE race_year =2019

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Views on Tables**
-- MAGIC - Create Temp View
-- MAGIC - Create Global Temp View
-- MAGIC - Create Permanent view

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year =2020

-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year =2020

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year =2020

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

