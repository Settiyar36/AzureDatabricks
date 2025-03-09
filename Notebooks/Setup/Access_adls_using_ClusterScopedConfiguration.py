# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC - we have set spark config in cluster advanced options
# MAGIC
# MAGIC - we set access key there 
# MAGIC
# MAGIC - confirma and restarted cluster
# MAGIC
# MAGIC - so that we are accesing data directly now without access keys providing seperately in every notebook 

# COMMAND ----------

display(dbutils.fs.ls("wasbs://demo@azstrorage36.blob.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("wasbs://demo@azstrorage36.blob.core.windows.net/circuits.csv"))
