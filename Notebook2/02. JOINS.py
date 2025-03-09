# Databricks notebook source
# MAGIC %run "/Workspace/formula1dl-Project/Includes/Configurations"

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits")\
.withColumnRenamed("circuits_id","circuit_id")\
.withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races")\
    .withColumnRenamed("name","race_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

circuits_df.printSchema()
races_df.printSchema()


# COMMAND ----------

circuits_df.select("circuit_id").distinct().show()


# COMMAND ----------

races_df.select("circuit_id").distinct().show()


# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

common_rows = circuits_df.alias("c")\
    .join(races_df.alias("r"), col("c.circuit_id") == col("r.circuit_id"), "inner")\
    .select("c.*")  # Selecting all columns from circuits_df

common_rows.show()


# COMMAND ----------

races_circuits_df = circuits_df.alias("c")\
    .join(races_df.alias("r"), col("c.circuit_id") == col("r.circuit_id"), "inner")\
    .select(
        col("c.circuit_name"),
        col("c.location"),
        col("c.country"),
        col("r.race_year"),
        col("r.round"),
        col("r.race_name")
    )

display(races_circuits_df)


# COMMAND ----------

# MAGIC %md
# MAGIC in the above cell i am getting all rows because of duplicate circuit_id values 

# COMMAND ----------

matching_rows = circuits_df.join(races_df, "circuit_id", "inner").count()
print(f"Matching rows in both DataFrames: {matching_rows}")

    #check count of matching rows, so u can be clear how many rows ahs to return for the inner join 

# COMMAND ----------

non_matching_circuits = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left_anti")
display(non_matching_circuits)


#check unmatched rows count for better clarity

# COMMAND ----------

  # drop duplicates if present in both the tables before join 

#circuits_df = circuits_df.dropDuplicates(["circuit_id"])
#races_df = races_df.dropDuplicates(["circuit_id"])


# COMMAND ----------

# MAGIC %md
# MAGIC ##INNER JOIN

# COMMAND ----------

        # now you can perform join

races_circuits_df = circuits_df.alias("c")\
    .join(races_df.alias("r"), col("c.circuit_id") == col("r.circuit_id"), "inner")\
    .select("c.circuit_name", "c.location", "c.country", "r.race_year", "r.round", "r.race_name")

display(races_circuits_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###RIGHT OUUTER JOIN
# MAGIC

# COMMAND ----------

        # now you can perform join

races_circuits_df = circuits_df.alias("c")\
    .join(races_df.alias("r"), col("c.circuit_id") == col("r.circuit_id"), "right")\
    .select("c.circuit_name", "c.location", "c.country", "r.race_year", "r.round", "r.race_name")

display(races_circuits_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## LEFT OUTER JOIN
# MAGIC

# COMMAND ----------

        # now you can perform join

races_circuits_df = circuits_df.alias("c")\
    .join(races_df.alias("r"), col("c.circuit_id") == col("r.circuit_id"), "left")\
    .select("c.circuit_name", "c.location", "c.country", "r.race_year", "r.round", "r.race_name")

display(races_circuits_df)


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## full join

# COMMAND ----------

        # now you can perform join

races_circuits_df = circuits_df.alias("c")\
    .join(races_df.alias("r"), col("c.circuit_id") == col("r.circuit_id"), "full")\
    .select("c.circuit_name", "c.location", "c.country", "r.race_year", "r.round", "r.race_name")

display(races_circuits_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## SEMI JOIN
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # SEMI JOIN only returns rows from the left table, even though you select right table columns explicitly, it throws error

# COMMAND ----------

        # now you can perform join

races_circuits_df = circuits_df.alias("c")\
    .join(races_df.alias("r"), col("c.circuit_id") == col("r.circuit_id"))
display(races_circuits_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## ANTI JOIN

# COMMAND ----------

# MAGIC %md
# MAGIC ## anti join gives only unmatached rows from the right table
# MAGIC

# COMMAND ----------

        # now you can perform join

races_circuits_df = circuits_df.alias("c")\
    .join(races_df.alias("r"), col("c.circuit_id") == col("r.circuit_id"), "anti")

display(races_circuits_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##CROSS JOIN

# COMMAND ----------

# MAGIC %md
# MAGIC ## CROSS JOIN gives all the rows 

# COMMAND ----------

        # now you can perform join

races_circuits_df = circuits_df.alias("c")\
    .join(races_df.alias("r"), col("c.circuit_id") == col("r.circuit_id"), "cross")

display(races_circuits_df)
