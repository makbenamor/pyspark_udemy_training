-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://pyspark@sakimo2024.dfs.core.windows.net/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------


