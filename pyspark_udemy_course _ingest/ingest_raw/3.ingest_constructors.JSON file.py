# Databricks notebook source
# MAGIC %run "../pyspark_udemy_course _includes/common_functions"

# COMMAND ----------

# MAGIC %run "../pyspark_udemy_course _includes/configuration"

# COMMAND ----------

schema_input= "constructorId INT, constructorRef STRING , name STRING, nationality STRING, url STRING"

# COMMAND ----------

df_constructors= spark.read.json(f'{raw_folder_path}/constructors.json', schema=schema_input)

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

df_constructors.write.mode('overwrite').parquet(f'{processed_folder_path}/constructors')

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

dbutils.notebook.exit("3.ingest_constructors.JSON file - Success")
