# Databricks notebook source
path= "abfss://pyspark@sakimo2023.dfs.core.windows.net/raw/races.csv"

# COMMAND ----------

df_races.printSchema()

# COMMAND ----------

input_shema = circuits_schema = StructType(
    fields=[
        StructField("race_id", IntegerType(), False),
        StructField("race_year", IntegerType(), False),
        StructField("round", IntegerType(), False),
        StructField("circuit_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("date", DateType(), False),
        StructField("time", StringType(), False),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

df_races= spark.read \
 .option("header", True) \
 .schema(input_shema) \
 .csv("abfss://pyspark@sakimo2023.dfs.core.windows.net/raw/races.csv")
             

# COMMAND ----------

display(df_races)

# COMMAND ----------

df_races.printSchema()

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

df_races.display()

# COMMAND ----------

df_races = df_races.withColumn("ingest_date", current_timestamp()).withColumn(
    "race_timestamp",
    to_timestamp(concat(col("date"), lit(" "), col('time')), "yyyy-MM-dd HH:mm:ss"),
)

# COMMAND ----------

df_races = df_races.select(
    col("race_id").alias("race_id"),
    col("race_year").alias("race_year"),
    col("round"),
    col("circuit_id").alias("circuit_id"),
    col("name"),
    col('ingest_date').alias("ingestion_date"),
    col("race_timestamp"),
)

# COMMAND ----------

df_races.display()

# COMMAND ----------

df_races.write.mode('overwrite').partitionBy('race_year').parquet('abfss://pyspark@sakimo2023.dfs.core.windows.net/processed/races')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls abfss://pyspark@sakimo2023.dfs.core.windows.net/processed/races

# COMMAND ----------


