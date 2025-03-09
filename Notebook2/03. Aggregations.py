# Databricks notebook source
# MAGIC %md
# MAGIC Aggregations DEMO
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/formula1dl-Project/Includes/Configurations"

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

presentation_folder_path

# COMMAND ----------

processed_folder_path

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl/processed")

# COMMAND ----------

 dbutils.fs.ls("/mnt/formula1dl/presentation")
    

# COMMAND ----------

display(dbutils.fs.ls(f"{presentation_folder_path}"))

# COMMAND ----------

results_df=spark.read.parquet(f"{presentation_folder_path}/final")

# COMMAND ----------

display(results_df)

# COMMAND ----------

demo_df=results_df.filter(results_df.race_year=="2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct,avg,max,min,sum

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("team")).show()

# COMMAND ----------

demo_df.select(sum("points").alias("total_points")).show()

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_df.filter(results_df.driver_name=="Lewis Hamilton")\
    .select(sum("points").alias("total_points"))

# COMMAND ----------

from pyspark.sql.functions import lit

total_points = demo_df.select(sum("points")).collect()[0][0]  # Compute total
demo_df = demo_df.withColumn("total_points", lit(total_points))  # Add as new column
display(demo_df)


# COMMAND ----------

demo_df = demo_df.drop("total_points")
display(demo_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## USING GROUP BY TO GROUP DATA
# MAGIC

# COMMAND ----------

demo_df.groupBy("driver_name")\
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("total_races"))\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## PRACTICING

# COMMAND ----------

#Retrieve all races that happened in the year 2023.

practice_df=results_df.filter(results_df.race_year=="2019")
display(practice_df)

# COMMAND ----------

#Get all races where the circuit location is "Monza".
filterred_df=practice_df.filter(practice_df.circuit_location=="Monza")
display(filterred_df)


# COMMAND ----------

#Find all drivers who have scored more than 20 points in a race.
from pyspark.sql.functions import col

filterred_df1=practice_df.filter(col("points")>20)\
    .select(
        col("driver_name"),
        col("points"),)
    

display(filterred_df1)

# COMMAND ----------

#List all races where the grid position was 1 and the driver did not win (race_time is not the fastest).
filterred_df2=filterred_df.filter((col("grid")==1) & (col("race_time")=="/N")) \
    .select(
        col("race_name")
    )
display(filterred_df2)


# COMMAND ----------

#Fetch all records where the driver nationality is "British" and team is "Mercedes".


filterred_df3=filterred_df.filter( (col("driver_nationality")=="British")\
& (col("team")=="Mercedes")) \

display(filterred_df3)



# COMMAND ----------

# Find the total points scored by all drivers across all races.

filterred_df4 = filterred_df.groupBy(
    col("driver_name"),
    col("race_name")
).agg(
    sum("points").alias("total_points")
)

display(filterred_df4)

# COMMAND ----------

#Calculate the average points scored per race.
f1=filterred_df.groupBy(
col("race_name")).agg(
    avg("points").alias("average_points"))

display(f1)


# COMMAND ----------

#Find the minimum and maximum points scored in any race.

f2=filterred_df.groupBy(
col("race_name")).agg(
min("points"), max("points"))

display(f2)


# COMMAND ----------

#Count the total number of races in the dataset.
f2=filterred_df.select(count("race_name").alias("total_races")).show()


# COMMAND ----------


# Get the fastest lap time recorded across all races.
f3 = filterred_df.groupBy(col("race_name"))\
.agg(max("fastest_lap").alias("max_fastest_lap"))

display(f3)

# COMMAND ----------

#Find the total points per driver.

f4=filterred_df.groupBy(
col("driver_name"))\
.agg(sum("points").alias("total_points")).show()

# COMMAND ----------

#Calculate the average points per team.

f4=filterred_df.groupBy(
col("team"))\
.agg(avg("points").alias("average_points")).show()


# COMMAND ----------

