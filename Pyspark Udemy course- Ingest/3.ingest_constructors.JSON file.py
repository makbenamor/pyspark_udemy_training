# Databricks notebook source
schema_input= "constructorId INT, constructorRef STRING , name STRING, nationality STRING, url STRING"

# COMMAND ----------

df_constructors= spark.read.json('abfss://pyspark@sakimo2023.dfs.core.windows.net/raw/constructors.json', schema=schema_input)

# COMMAND ----------

display(df_constructors)


# COMMAND ----------

df_constructors=df_constructors.drop('url')

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_constructors=df_constructors.withColumnRenamed('constructorId','constructor_id') \
                               .withColumnRenamed('constructorRef', 'constructor_ref') \
                               .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

df_constructors.write.mode('overwrite').parquet("abfss://pyspark@sakimo2023.dfs.core.windows.net/processed/constructors")

# COMMAND ----------

display(df_constructors)

# COMMAND ----------


