-- Databricks notebook source
CREATE DATABASE if not exists f1_raw;

-- COMMAND ----------

use f1_raw;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/mnt/formula1dl/" )
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/mnt/azstrorage36/raw")

-- COMMAND ----------

show databases;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE CIRCUITS TABLE

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw_circuits;
CREATE TABLE IF NOT EXISTS f1_raw_circuits(
  circuitID INT,
  circuitRef STRING,
  name String,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
using CSV
OPTIONS(path "/mnt/azstrorage36/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw_circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE RACES TABLE

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw_races;

CREATE TABLE IF NOT EXISTS f1_raw_races(

  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS(path "/mnt/azstrorage36/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw_races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE TABLE FOR JSON FILES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Create cosntructors table
-- MAGIC   Single line JSON file
-- MAGIC   Simple structure
-- MAGIC   

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw_constructors;

CREATE TABLE IF NOT EXISTS f1_raw_constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING JSON
options(path "/mnt/azstrorage36/raw/constructors.json", header true)

-- COMMAND ----------

select * from f1_raw_constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE DRIVERS TABLE
-- MAGIC   Single line JSON
-- MAGIC   Complex Structure
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw_drivers;

CREATE TABLE IF NOT EXISTS f1_raw_drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename:STRING,surname:STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)

USING JSON
OPTIONS(path "/mnt/azstrorage36/raw/drivers.json" ,header true)

-- COMMAND ----------

select * from f1_raw_drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE RESULTS TABLE
-- MAGIC   Single line JSON
-- MAGIC   simple structure
-- MAGIC   

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw_results;

CREATE TABLE IF NOT EXISTS f1_raw_results(
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
  millionseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING
)
USING JSON
OPTIONS(path "/mnt/azstrorage36/raw/results.json")

-- COMMAND ----------

select * from f1_raw_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create PITSTOPS TABLE
-- MAGIC   Multiline JSON
-- MAGIC   simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw_pit_stops;

CREATE TABLE IF NOT EXISTS f1_raw_pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING
)
USING JSON
options(path "/mnt/azstrorage36/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from f1_raw_pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE TABLE FOR LIST OF FILES
-- MAGIC   CSV file
-- MAGIC   Multiple Files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;

CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
options(path "/mnt/azstrorage36/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE QUALIFYING TABLE
-- MAGIC   Multiline JSON
-- MAGIC   Multiple FILES

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
  qualifyId INT,
  raceId INT
)
USING JSON
OPTIONS(path "/mnt/azstrorage36/raw/qualifying", multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying;

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying;