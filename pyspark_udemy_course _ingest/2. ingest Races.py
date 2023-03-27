# Databricks notebook source
# MAGIC %run "../pyspark_udemy_course _includes/configuration"

# COMMAND ----------

# MAGIC %run "../pyspark_udemy_course _includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import *

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

races_df= spark.read \
 .option("header", True) \
 .schema(races_schema) \
 .csv(f'{raw_folder_path}/races.csv')
             

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(
    col("raceId").alias("race_id"),
    col("year").alias("race_year"),
    col("round"),
    col("circuitId").alias("circuit_id"),
    col("name"),
    col("ingestion_date"),
    col("race_timestamp"),
)

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet(f'{processed_folder_path}/races')

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

dbutils.notebook.exit("2. ingest Races - Success")
