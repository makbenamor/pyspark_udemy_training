# Databricks notebook source
dbutils.notebook.run('1. ingests Circuits',0,{'p_data_source':'Ergast API'})

# COMMAND ----------

dbutils.notebook.run('2. ingest Races',0)

# COMMAND ----------

dbutils.notebook.run('3.ingest_constructors.JSON file',0)

# COMMAND ----------

dbutils.notebook.run('4. Ingest drivers.Json file',0)

# COMMAND ----------

dbutils.notebook.run('5.Ingest result file',0)

# COMMAND ----------

dbutils.notebook.run('6.ingest pitstops',0)

# COMMAND ----------

dbutils.notebook.run('7. laptimes',0)

# COMMAND ----------

dbutils.notebook.run('8.ingest_qualifying_files',0)

# COMMAND ----------


