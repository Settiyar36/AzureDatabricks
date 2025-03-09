# Databricks notebook source
    # as we are using ingesiton_date as a column in every notebook, it is useful to create a function and jsut call it when required.


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df=input_df.withColumn("ingestion_date", current_timestamp())
    return output_df