# Databricks notebook source
from pyspark.sql.types import * 
from pyspark.sql.functions import *

# COMMAND ----------

schema_input = StructType(
    fields=[
        StructField("qualifyId", IntegerType(), False),
        StructField("driverId", IntegerType(), False),
        StructField("raceId", IntegerType(), False),
        StructField("constructorId", IntegerType(), False),
        StructField("number", IntegerType(), False),
        StructField("position", IntegerType(), False),
        StructField("q1", StringType(), True),
        StructField("q2", StringType(), True),
        StructField("q3", StringType(), True)
    ]
)

# COMMAND ----------

qualifying_df= spark.read \
 .schema(schema_input) \
.option("multiLine", True) \
.json('abfss://pyspark@sakimo2023.dfs.core.windows.net/raw/qualifying')

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://pyspark@sakimo2023.dfs.core.windows.net/processed/qualifying")

# COMMAND ----------

display(final_df)

# COMMAND ----------


