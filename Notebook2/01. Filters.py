# Databricks notebook source


# COMMAND ----------

# MAGIC %run "/Workspace/formula1dl-Project/Includes/Configurations"

# COMMAND ----------

processed_folder_path

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl/processed"))

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_filtered_df = races_df.filter(
    (races_df.race_year == 2021) & (races_df.round > 5)
)
display(races_filtered_df)

# COMMAND ----------

races_filtered_df = races_df.filter(
    (races_df["race_year"] == 2019) & (races_df["round"] > 5)
)
display(races_filtered_df)