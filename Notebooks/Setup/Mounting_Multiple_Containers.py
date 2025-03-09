# Databricks notebook source
# if you have secret scope use this :

    # USE THIS WHEN U CREATED SECRET SCOPE (PREMIUM TIER)
def mount_adls(storage_account_name, container_name):
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-appsecret')

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # GET SECRETS 
    client_id="b32873af-7bae-4fff-a5b3-210e975ba3b8" 
    tenant_id="45605182-4754-47de-aef9-e13b452b63c6"
    client_secret="tHA8Q~jkbDzmJSTS7Sv3NSsyMMRZ~8TNg~WcsazO" 

    # SPARK CONFIG 
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }

    #unmounting if already mount exists :
    if any(mount.mountPoint==f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
      dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}") 

    # MOUNTING 
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs
    )

    # TO DISPLAY mounts PRESENT IN MOUNTED STORAGE ACCOUNT
    display(dbutils.fs.mounts())

# COMMAND ----------

#call the function and mount raw container 
mount_adls('azstrorage36', 'raw')



# COMMAND ----------

# call function and mount processed container 

mount_adls('azstrorage36', 'processed')

# COMMAND ----------

# call function and mount presentation container

mount_adls('azstrorage36', 'presentation')

# COMMAND ----------

mount_adls('azstrorage36', 'deltalake-demo')