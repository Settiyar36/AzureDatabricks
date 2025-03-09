# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Accessing Data via access keys 
# MAGIC ## Experimenting with accessing data from data lake gen 2 via access keys
# MAGIC

# COMMAND ----------

spark.conf.set("fs.azure.account.key.azstrorage36.blob.core.windows.net", "W71g6/2rEoQkM/LsyTSzFpM20Iu0EPC/ZJ9viSywK+4OVFfI9oLnSjBT83B+ASt4bsGMg==")

# COMMAND ----------

display(dbutils.fs.ls("wasbs://demo@azstrorage36.blob.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("wasbs://demo@azstrorage36.blob.core.windows.net/circuits.csv"))
