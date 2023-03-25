-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

desc database demo; 

-- COMMAND ----------

-- MAGIC %run "../pyspark_udemy_course _includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC race_results_df= spark.read.parquet(f'{presenting_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC race_results_df.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC race_results_df.write.format('parquet').saveAsTable('demo.race_results_table')

-- COMMAND ----------

SELECT * FROM demo.race_results_table

-- COMMAND ----------

USE demo;

-- COMMAND ----------

show tables; 

-- COMMAND ----------

DESC EXTENDED race_results_table 

-- COMMAND ----------

CREATE TABLE races_2021
AS
SELECT
  *
from
  race_results_table
where
  race_year = 2020

-- COMMAND ----------

ALTER TABLE races_2020 SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5');

-- COMMAND ----------

CREATE TABLE races_2020
AS
SELECT
  *
from
  race_results_table
where
  race_year = 2020

-- COMMAND ----------

CREATE TABLE if not exists races_2020

-- COMMAND ----------

ALTER TABLE races_2020 SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5');

-- COMMAND ----------

CREATE TABLE races_2021
AS
SELECT
  *
from
  race_results_table
where
  race_year = 2020

-- COMMAND ----------


