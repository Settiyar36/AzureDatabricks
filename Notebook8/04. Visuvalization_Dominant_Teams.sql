-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC html="""<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Teams</h1>"""
-- MAGIC
-- MAGIC displayHTML(html)

-- COMMAND ----------


CREATE OR REPLACE TEMP view d_dominant_teams as 
SELECT team_name,
count(1) as total_races,
SUM(calculated_points) as total_points,
avg(calculated_points) as average_points,
RANK() OVER(ORDER BY avg(calculated_points) DESC ) team
from f1_presentation.calculate_race_results
GROUP by team_name
having count(1)>=100
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT race_year, team_name,
    count(1) as total_races,
    SUM(calculated_points) as total_points,
    avg(calculated_points) as average_points
FROM f1_presentation.calculate_race_results
WHERE team_name IN (
    SELECT team_name 
    FROM d_dominant_teams 
    WHERE team <= 5
)
GROUP BY race_year, team_name
ORDER BY race_year, average_points DESC;

-- COMMAND ----------

SELECT race_year, team_name,
    count(1) as total_races,
    SUM(calculated_points) as total_points,
    avg(calculated_points) as average_points
FROM f1_presentation.calculate_race_results
WHERE team_name IN (
    SELECT team_name 
    FROM d_dominant_teams 
    WHERE team <= 5
)
GROUP BY race_year, team_name
ORDER BY race_year, average_points DESC;

-- COMMAND ----------

SELECT race_year, team_name,
    count(1) as total_races,
    SUM(calculated_points) as total_points,
    avg(calculated_points) as average_points
FROM f1_presentation.calculate_race_results
WHERE team_name IN (
    SELECT team_name 
    FROM d_dominant_teams 
    WHERE team <= 5
)
GROUP BY race_year, team_name
ORDER BY race_year, average_points DESC;