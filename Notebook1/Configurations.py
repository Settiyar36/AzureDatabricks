# Databricks notebook source

# we are specifying the container/ mount paths in every notebook, as hardcoding values is not good practice, we are using this to specify them as variables and use them when required. 

# COMMAND ----------

raw_folder_path="/mnt/azstrorage36/raw"
processed_folder_path="/mnt/formula1dl/processed"
presentation_folder_path="/mnt/formula1dl/presentation"

# COMMAND ----------

# if you have not mounted to databricks use abfss / wasbs according to the storage account type/ 

# COMMAND ----------

        # if you are using azure data lake gen 2 

#   raw_folder="abfss://raw@azstrorgae36.dfs.core.windows.net/"
#   processed_folder="abfss://processed@azstrorgae36.dfs.core.windows.net/"
#   presentation_folder="abfss://presentation@azstrorgae36.dfs.core.windows.net/"

        # if you are using azure blob storage 

#   raw_folder="wasbs://raw@azstrorgae36.blob.core.windows.net/"
#   processed_folder="wasbs://processed@azstrorgae36.blob.core.windows.net/"
#   presentation_folder="wasbs://presentation@azstrorgae36.blob.core.windows.net/"

