# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC ## Accesing azure blob data using 
# MAGIC ## service principal
# MAGIC

# COMMAND ----------

client_id="b32873af-7bae-4fff-a5b3-2105ba3b8" 
tenant_id="45605182-4754-47de-aef9-e1452b63c6"
client_secret="tHA8Q~jkbDzmJSTS7SvyMMRZ~8TNg~WcsazO" 

# COMMAND ----------

# Cell 2, also named "Cell 3"
# Replace with actual values
storage_account_name = "azstrorage36"
container_name = "demo"

# Set Spark Configuration for Service Principal Authentication
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Example to read a file from Blob Storage
df = spark.read.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/circuits.csv")

display(df)

# COMMAND ----------


# Replace with actual values
storage_account_name = "azstrorage36"
container_name = "demo"

# Set Spark Configuration for Service Principal Authentication
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Example to read a file from Blob Storage
df = spark.read.text(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/circuits.csv")

display(df)