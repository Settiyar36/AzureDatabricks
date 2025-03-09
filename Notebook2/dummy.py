# Databricks notebook source
spark.conf.set("fs.azure.account.key.blob.core.windows.net", "W71g6/2rEoQkM/LsyTSzFpM20Iu0EPC/086VHFwO5nS7ZJ9viSywK+4OVFfI9oLOGFTSnSjBT83B+ASt4bsGMg==")

# COMMAND ----------

display(dbutils.fs.ls('wasbs://demo@azstrorage36.blob.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('wasbs://demo@azstrorage36.blob.core.windows.net/circuits.csv'))

# COMMAND ----------

spark.conf.set("fs.azure.azstrorage36.sas.blob.core.windows.net","sp=r&st=2025-02-11T04:28:25Z&se=2025-02-11T12:28:25Z&spr=https&sv=2022-11-02&sr=c&sig=YIW6xswq1yxKnlf%2FYvWaar5zC518cQp9h6YsXsI7Y3c%3D")

# COMMAND ----------

dbutils.fs.ls('wasbs://demo@azstrorage36.blob.core.windows.net')

# COMMAND ----------

