-- Databricks notebook source
use f1_processed;

-- COMMAND ----------

drop table if exists f1_presentation.calculate_race_results;
CREATE TABLE f1_presentation.calculate_race_results
USING PARQUET
AS
select races.race_year,
constructors.name as team_name,
drivers.name as driver_name,
results.position,
results.points,
11 - results.position AS calculated_points
from results
JOIN drivers on (results.driver_id=drivers.driver_id)
JOIN constructors on (results.constructor_id=constructors.constructor_id)
JOIN races on (results.race_id=races.race_id)
where results.position<=10;

-- COMMAND ----------

select * from f1_presentation.calculate_race_results;