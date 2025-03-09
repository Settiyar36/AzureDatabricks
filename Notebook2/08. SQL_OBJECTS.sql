-- Databricks notebook source
CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESC DATABASE demo;

-- COMMAND ----------

DESC DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

use demo;


-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %run "/Workspace/formula1dl-Project/Includes/Configurations"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC results_df=spark.read.parquet(f"{presentation_folder_path}/final")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

show databases;

-- COMMAND ----------


use demo;

-- COMMAND ----------

show tables;

-- COMMAND ----------

desc database extended demo;

-- COMMAND ----------

select * from demo.race_results_python where race_year=2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC THIS IS managed table we are creating

-- COMMAND ----------

CREATE TABLE demo.race_results_sql

as

select * from demo.race_results_python where race_year=2020

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### from below command, we can get the type and location of the table. 
-- MAGIC as it is managed table

-- COMMAND ----------

desc extended demo.race_results_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC IF this table is dropped, it will drop the metastore as well

-- COMMAND ----------

drop table demo.race_results_sql;

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #CREATING EXTERNAL TABLE
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC results_df.write.format("parquet").option("path", f"{presentation_folder_path}/final_parquet_externalTable").saveAsTable("demo.results_df_externalTable_python")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Here, the type id EXternal, and location is mount location

-- COMMAND ----------

desc extended demo.results_df_externalTable_python;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC here, we can check the file is mounted here 

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC display(dbutils.fs.ls(f"{presentation_folder_path}"))

-- COMMAND ----------



-- COMMAND ----------

CREATE TABLE demo.results_df_externalTable_sql
(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)

USING PARQUET
LOCATION "/mnt/formula1dl/presentation/demo.results_df_externalTable_python"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  i have accidentaly created with wrong folder name and i want to change it, so i am dropping it and creating again

-- COMMAND ----------

drop table demo.results_df_externalTable_sql

-- COMMAND ----------


CREATE TABLE demo.results_df_externalTable_sql
(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap STRING,
  race_time STRING,
  points FLOAT,
  created_date TIMESTAMP
)

USING PARQUET
LOCATION "/mnt/formula1dl/presentation/demo.results_df_externalTable_sql"

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

insert into demo.results_df_externalTable_sql
select * from demo.results_df_externalTable_python 
where race_year=2020;

-- COMMAND ----------

select * from demo.results_df_externalTable_sql;

-- COMMAND ----------

select count(1) from demo.results_df_externalTable_sql;

-- COMMAND ----------

show tables in demo;