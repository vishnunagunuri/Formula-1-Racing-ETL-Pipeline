-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://processed@dlfor1.dfs.core.windows.net/"

-- COMMAND ----------

DESC DATABASE EXTENDED f1_processed

-- COMMAND ----------

USE f1_processed

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DROP TABLE circuits

-- COMMAND ----------

