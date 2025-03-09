# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC ## Accessing data from azure blob storage using SAS TOKENS 
# MAGIC

# COMMAND ----------

# Replace with your actual values
storage_account_name = "azstrorage36"
container_name = "demo"
sas_token = "sp=r&st=2025-02-07T07:20:59Z&se=2025-02-07T15:20:59Z&spr=https&sv=2022-11-02&sr=c&sig=k7j40C3ntxqL%2FL3StrwG8wTG0DtU%3D"  # Without '?' at the beginning

# Setting Spark configuration for SAS authentication

spark.conf.set(f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net", sas_token)

#reading from azure blob
df = spark.read.text(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/circuits.csv")

display(df)