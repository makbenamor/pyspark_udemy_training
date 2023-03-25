# Databricks notebook source
from pyspark.sql.types import * 
from pyspark.sql.functions import *

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv("abfss://pyspark@sakimo2023.dfs.core.windows.net/raw/lap_times")

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://pyspark@sakimo2023.dfs.core.windows.net/processed/lap_times")

# COMMAND ----------

display(final_df)

# COMMAND ----------

dbutils.notebook.exit("7. laptimes - Success")
