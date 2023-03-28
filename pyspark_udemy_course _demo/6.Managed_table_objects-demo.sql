-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo;
Use demo;

-- COMMAND ----------

-- MAGIC %run "../pyspark_udemy_course _includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC race_results_df= spark.read.parquet(f'{presenting_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC race_results_df.write.mode('overwrite').format('parquet').saveAsTable('demo.race_results_table')

-- COMMAND ----------

SELECT * FROM demo.race_results_table

-- COMMAND ----------

DESC EXTENDED demo.race_results_table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS race_results_sql AS
SELECT
  *
FROM
demo.race_results_table
where race_year =2020;

-- COMMAND ----------

SELECT * FROM race_results_sql

-- COMMAND ----------

-- Create external tables
