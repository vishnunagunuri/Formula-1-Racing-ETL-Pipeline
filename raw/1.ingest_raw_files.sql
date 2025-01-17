-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Circuits File**

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "abfss://raw@dlfor1.dfs.core.windows.net/circuits.csv",header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Races Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date STRING,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "abfss://raw@dlfor1.dfs.core.windows.net/races.csv",header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Creating Constructors table**
-- MAGIC - Single Line JSON
-- MAGIC - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "abfss://raw@dlfor1.dfs.core.windows.net/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Drivers Table**
-- MAGIC - Single Line JSON
-- MAGIC - Complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
 driverId INT,
 driverRef STRING,
 number INT,
 code STRING,
 name STRUCT<forename: STRING,SURNAME: STRING>,
 dob DATE,
 nationality STRING,
 url STRING
)
USING json
OPTIONS (path "abfss://raw@dlfor1.dfs.core.windows.net/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **CREATE Results Table**
-- MAGIC - Single Line JSON
-- MAGIC - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed STRING,
  statusId STRING
)
USING json
OPTIONS (path "abfss://raw@dlfor1.dfs.core.windows.net/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Pit Stops File**
-- MAGIC - Multiline JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
USING json
OPTIONS (path "abfss://raw@dlfor1.dfs.core.windows.net/pit_stops.json",multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Tables for List of Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Lap Times Table**
-- MAGIC - CSV FIle
-- MAGIC - Multiple Files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driver_id INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING csv
OPTIONS (path "abfss://raw@dlfor1.dfs.core.windows.net/lap_times")

-- COMMAND ----------

SELECT COUNT(1) FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Qualifying Table
-- MAGIC - JSON File
-- MAGIC - Multiline JSON
-- MAGIC - Multiple Files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyingId INT,
  raceId INT
)
USING json
OPTIONS (path "abfss://raw@dlfor1.dfs.core.windows.net/qualifying",multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying;

-- COMMAND ----------

