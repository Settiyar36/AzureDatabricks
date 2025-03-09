# Databricks notebook source
# MAGIC %md
# MAGIC Local temp view cant be accessed using this notebook, as its scope is until that particular notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM view_results_df