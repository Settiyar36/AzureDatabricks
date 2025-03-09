-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC html="""<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers</h1>"""
-- MAGIC
-- MAGIC displayHTML(html)

-- COMMAND ----------


CREATE OR REPLACE TEMP view d_dominant_drivers as 
SELECT driver_name,
count(1) as total_races,
SUM(calculated_points) as total_points,
avg(calculated_points) as average_points,
RANK() OVER(ORDER BY avg(calculated_points) DESC ) driver_rank
from f1_presentation.calculate_race_results
GROUP by driver_name
having count(1)>=50
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT race_year, driver_name,
    count(1) as total_races,
    SUM(calculated_points) as total_points,
    avg(calculated_points) as average_points
FROM f1_presentation.calculate_race_results
WHERE driver_name IN (
    SELECT driver_name 
    FROM d_dominant_drivers 
    WHERE driver_rank <= 10
)
GROUP BY race_year, driver_name
ORDER BY race_year, average_points DESC;

-- COMMAND ----------

SELECT race_year, driver_name,
    count(1) as total_races,
    SUM(calculated_points) as total_points,
    avg(calculated_points) as average_points
FROM f1_presentation.calculate_race_results
WHERE driver_name IN (
    SELECT driver_name 
    FROM d_dominant_drivers 
    WHERE driver_rank <= 10
)
GROUP BY race_year, driver_name
ORDER BY race_year, average_points DESC;

-- COMMAND ----------

SELECT race_year, driver_name,
    count(1) as total_races,
    SUM(calculated_points) as total_points,
    avg(calculated_points) as average_points
FROM f1_presentation.calculate_race_results
WHERE driver_name IN (
    SELECT driver_name 
    FROM d_dominant_drivers 
    WHERE driver_rank <= 10
)
GROUP BY race_year, driver_name
ORDER BY race_year, average_points DESC;