# Databricks notebook source
# MAGIC %run "../pyspark_udemy_course _includes/configuration"

# COMMAND ----------

races_df= spark.read.parquet(f'{processed_folder_path}/races').filter('race_year= 2019').withColumnRenamed('name','race_name')

# COMMAND ----------

circuits_df=spark.read.parquet(f'{processed_folder_path}/circuits').withColumnRenamed('name','circuit_name')

# COMMAND ----------

circuits_df.display()

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df, circuits_df.circuit_id==races_df.circuit_id).select(circuits_df.name, circuits_df.location, circuits_df.country, races_df.name, races_df.round)

# COMMAND ----------

race_circuits_df.display()

# COMMAND ----------


