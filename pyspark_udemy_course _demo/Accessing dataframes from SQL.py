# Databricks notebook source
# MAGIC %md
# MAGIC #### Accessing dataframes from SQL
# MAGIC 1. create temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python

# COMMAND ----------

# MAGIC %run "../pyspark_udemy_course _includes/configuration"

# COMMAND ----------

race_results=spark.read.parquet(f'{presenting_folder_path}/race_results')

# COMMAND ----------

race_results.createTempView('v_race_results')

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT * from v_race_results
# MAGIC where race_year =2020

# COMMAND ----------


