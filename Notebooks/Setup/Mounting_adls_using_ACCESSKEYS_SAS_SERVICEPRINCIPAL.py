# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Using service principal 
# MAGIC ## Experimenting with mounting data from azure blob / datalake to databricks 
# MAGIC
# MAGIC

# COMMAND ----------

client_id="b32873af-7bae-4fff-a5b3-2975ba3b8" 
tenant_id="45605182-4754-47de-aef9-e52b63c6"
client_secret="tHA8Q~jkbDzmJSTS7SMMRZ~8TNg~WcsazO" 

# COMMAND ----------

# USE THIS WHEN U CREATED SECRET SCOPE (PREMIUM TIER)

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')

# COMMAND ----------

 configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

# TO MOUNT

dbutils.fs.mount(
  source = "abfss://demo@azstrorage36.dfs.core.windows.net/",
  mount_point = "/mnt/azstrorage36/demo",
  extra_configs = configs)

# COMMAND ----------

#TO DISPLAY FILES PRESENT IN MOUNTED STORAGE ACCOUNT

display(dbutils.fs.ls("/mnt/azstrorage36/demo"))


# COMMAND ----------

# TO DISPLAY DATA 

display(spark.read.csv("/mnt/azstrorage36/demo/circuits.csv"))

# COMMAND ----------

# TO DISPLAY MOUNTS PRESESNT IN THAT STORAGE ACCOUNT

display(dbutils.fs.mounts())

# COMMAND ----------

# TO unmount the azure blob from databricks 

dbutils.fs.unmount('/mnt/azstrorage36/demo')


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC This method is not secure, but this way we can mount azure blob to our databricks
# MAGIC

# COMMAND ----------

accesskeys = "W71g6/2rEoQkM/LsyTSzFpM20Iu0EPC/086VHFwO5nS7ZJ9viSywK+4OVFfI9oLOGFTSnSjBT83B+ASt4bsGMg=="

# Set up Spark config with the storage account access key
spark.conf.set(
  "spark.hadoop.fs.azure.account.key.<azstrorage36>.blob.core.windows.net",
  accesskeys
)

# Mount the Azure Blob Storage container
dbutils.fs.mount(
  source = "wasbs://demo@azstrorage36.blob.core.windows.net/",
  mount_point = "/mnt/azstrorage36/demo2",
  extra_configs = {
    f"fs.azure.account.key.azstrorage36.blob.core.windows.net": accesskeys
  }
)

# Verify mount
display(dbutils.fs.ls("/mnt/azstrorage36/demo2"))

# COMMAND ----------

# TO DISPLAY DATA 

display(spark.read.csv("/mnt/azstrorage36/demo2/circuits.csv"))

# COMMAND ----------

# TO unmount the azure blob from databricks 

dbutils.fs.unmount('/mnt/azstrorage36/demo2')
