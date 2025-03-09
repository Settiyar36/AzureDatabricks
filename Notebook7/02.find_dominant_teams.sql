-- Databricks notebook source
select * from 
f1_presentation.calculate_race_results;

-- COMMAND ----------

SELECT team_name,
count(1) as total_races,
SUM(calculated_points) as total_points,
avg(calculated_points) as average_points
from f1_presentation.calculate_race_results
GROUP by team_name
having count(1)>=100
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT team_name,
count(1) as total_races,
SUM(calculated_points) as total_points,
avg(calculated_points) as average_points
from f1_presentation.calculate_race_results
Where race_year between 2011 and 2020
GROUP by team_name
having count(1)>=100
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT team_name,
count(1) as total_races,
SUM(calculated_points) as total_points,
avg(calculated_points) as average_points
from f1_presentation.calculate_race_results
Where race_year between 2001 and 2011
GROUP by team_name
having count(1)>=100
ORDER BY average_points DESC;