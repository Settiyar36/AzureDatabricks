-- Databricks notebook source
-- MAGIC %md
-- MAGIC 01. Create temp view
-- MAGIC 02. Create global view
-- MAGIC 03. Create permanent view
-- MAGIC

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

-- MAGIC %md
-- MAGIC ##as it is already exists , i am creating in new variable
-- MAGIC

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC
-- MAGIC results_df.write.format("parquet").saveAsTable("demo.race_results_python1")

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC results_df=spark.read.parquet(f"{presentation_folder_path}/final")

-- COMMAND ----------

select * from demo.race_results_python1 where race_year=2020

-- COMMAND ----------

create TEMP VIEW v_temp_race_results
as 
SELECT * FROM demo.race_results_python1
where race_year=2018;

-- COMMAND ----------

select * from v_temp_race_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now we are creating a global temp view 

-- COMMAND ----------

create or REPLACE GLOBAL TEMP VIEW gv_race_results
as 
SELECT * FROM demo.race_results_python1
where race_year=2012;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # AS , we cannot access global view directly, we have to use database name followed by view name 

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

select * from global_temp.gv_race_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## here we are creating permanent view, it is by default created in default folder.

-- COMMAND ----------

create or REPLACE  VIEW pv_race_results
as 
SELECT * FROM demo.race_results_python1
where race_year=2010;

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from pv_race_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###SO, it is advisable to create by using database name

-- COMMAND ----------

create or REPLACE  VIEW demo.pv_race_results
as 
SELECT * FROM demo.race_results_python1
where race_year=2010;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### now check for location of this permanent view 
-- MAGIC

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC as i havent droppped , it is in defaulttoo
-- MAGIC

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from pv_race_results;