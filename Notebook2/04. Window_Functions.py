# Databricks notebook source
# MAGIC %run "/Workspace/formula1dl-Project/Includes/Configurations"

# COMMAND ----------

results_df=spark.read.parquet(f"{presentation_folder_path}/final")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

demo_df=results_df.filter(col("race_year").isin("2019", "2020"))

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count,countDistinct

# COMMAND ----------

demo_grouped_df=demo_df.groupBy(
    col("race_year"),
    col("driver_name"))\
    .agg(sum("points").alias("total_points"), countDistinct("race_name"))

display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import  rank

ranked_df=Window.partitionBy("race_year").orderBy(col("total_points").desc())
                                                           
demo_grouped_df.withColumn("rank", rank().over(ranked_df)).show(100)