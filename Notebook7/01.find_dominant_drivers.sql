-- Databricks notebook source
select * from 
f1_presentation.calculate_race_results;

-- COMMAND ----------

SELECT driver_name,
count(1) as total_races,
SUM(calculated_points) as total_points,
avg(calculated_points) as avergae_points
from f1_presentation.calculate_race_results
GROUP by driver_name
having count(1)>=50
ORDER BY total_points DESC;

-- COMMAND ----------

SELECT driver_name,
count(1) as total_races,
SUM(calculated_points) as total_points,
avg(calculated_points) as average_points
from f1_presentation.calculate_race_results
GROUP by driver_name
having count(1)>=50
ORDER BY average_points DESC;

-- COMMAND ----------

