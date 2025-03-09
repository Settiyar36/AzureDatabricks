# Databricks notebook source
# MAGIC %md
# MAGIC ### 01. Create a global temp view on dataframe
# MAGIC ### 02. Access the view using sql 
# MAGIC ### 03. Access the view using Python
# MAGIC ### 04. Access the view from another notebook
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Any notebook from a praticular cluster , where temp view is created is accessbile. 
# MAGIC
# MAGIC ### but cant be accessed using another cluster 
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/formula1dl-Project/Includes/Configurations"

# COMMAND ----------

results_df=spark.read.parquet(f"{presentation_folder_path}/final")

# COMMAND ----------

results_df.createGlobalTempView("global_view_results_df")

# COMMAND ----------

# MAGIC %md
# MAGIC Global temp view is not accessed directly it has to accessed using databasename.global_temp_view_name

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_view_results_df

# COMMAND ----------

# MAGIC %md
# MAGIC it just shows default tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Show tables

# COMMAND ----------

# MAGIC %md
# MAGIC to see global tables using this below command

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * from global_temp.global_view_results_df

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT count(driver_name) FROM global_temp.global_view_results_df 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(driver_name) from global_temp.global_view_results_df where circuit_location="São Paulo"

# COMMAND ----------

spark.sql("select * from global_temp.global_view_results_df limit 15").show()