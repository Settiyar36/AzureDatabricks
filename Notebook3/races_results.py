# Databricks notebook source
# MAGIC %md
# MAGIC ## READ ALL DATA REQUIRED
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/formula1dl-Project/Includes/Configurations"

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl/processed")

# COMMAND ----------

drivers_df=spark.read.parquet(f"{processed_folder_path}/drivers")\
    .withColumnRenamed("name","driver_name")\
    .withColumnRenamed("nationality","driver_nationality")\
    .withColumnRenamed("number","driver_number")

# COMMAND ----------

construcors_df=spark.read.parquet(f"{processed_folder_path}/constructors")\
    .withColumnRenamed("name","team")

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races")\
    .withColumnRenamed("number","driver_number")\
    .withColumnRenamed("race_timestamp","race_date")\
    .withColumnRenamed("name","race_name")

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits")\
    .withColumnRenamed("location","circuit_location")

# COMMAND ----------

results_df=spark.read.parquet(f"{processed_folder_path}/results")\
    .withColumnRenamed("time","race_time")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_circuit_df=races_df.join(
    circuits_df,
    races_df.circuit_id==circuits_df.
    circuits_id,
    "inner"
    ).select(
        col("race_year"),
        col("race_id"), 
        col("race_name"), 
        col("race_date"),
        col("circuit_location")
        )

# COMMAND ----------

race_results_df=results_df.join(race_circuit_df, results_df.race_id==race_circuit_df.race_id)\
    .join(drivers_df, results_df.driver_id==drivers_df.driver_id)\
    .join(construcors_df, results_df.constructor_id==construcors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points").withColumn("created_date", current_timestamp())\
.withColumn("created_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

from pyspark.sql.functions import col

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'"))

# COMMAND ----------

from pyspark.sql.functions import col

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'")
                .orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").option("path","/mnt/formula1dl/presentation").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_presentation.race_results;