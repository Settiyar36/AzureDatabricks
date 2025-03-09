# Databricks notebook source
# MAGIC %md
# MAGIC ### 01. Create temporary views on dataframes
# MAGIC ### 02. Acess the views from SQL cell.
# MAGIC ### 03. Access the views from Python cell
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/formula1dl-Project/Includes/Configurations"

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl/presentation")

# COMMAND ----------

results_df=spark.read.parquet(f"{presentation_folder_path}/final")

# COMMAND ----------

results_df.createOrReplaceTempView("view_results_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM view_results_df

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT * FROM view_results_df WHERE driver_number>40

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT circuit_location, count(points) , max(fastest_lap) FROM view_results_df GROUP BY circuit_location having count(points)>200

# COMMAND ----------

spark.sql("SELECT * FROM view_results_df  WHERE race_name='Italian Grand Prix' LIMIT 10" ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC NOTE: This is a temporary view, 
# MAGIC so if cluster detached, view will be dropped.
# MAGIC
# MAGIC ### it cannot be accessed through another notebook too.....