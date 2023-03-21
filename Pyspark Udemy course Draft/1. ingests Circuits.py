# Databricks notebook source
path="abfss://pyspark@sakimo2023.dfs.core.windows.net/processed/circuits"

# COMMAND ----------

df_circuits = spark.read \
    .option('header', True) \
    .option('inferSchema', True) \
    .csv("abfss://pyspark@sakimo2023.dfs.core.windows.net/raw/circuits.csv")


# COMMAND ----------

display(df_circuits.printSchema())

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

circuits_schema= StructType(fields=
                            [StructField("circuitId",IntegerType(),False),
                             StructField("circuitRef",StringType(),True), 
                             StructField("name",StringType(),True),
                             StructField("location",StringType(),True),
                             StructField("country",StringType(),True),
                             StructField("lat",DoubleType(),True),
                             StructField("lng",DoubleType(),True),
                             StructField("alt",IntegerType(),True),
                             StructField("url",StringType(),True)
                            ])

# COMMAND ----------

circuits_schema

# COMMAND ----------

df_circuits = spark.read \
    .option('header', True) \
    .schema(circuits_schema) \
    .csv("abfss://pyspark@sakimo2023.dfs.core.windows.net/raw/circuits.csv")

# COMMAND ----------

from pyspark.sql.functions import *
circuits_selected_df = df_circuits.select(
    col("circuitId").alias("circuit_id"), 
    col("circuitRef").alias("circuit_ref"),
    col("name"),
    col("location"),
    col("country"),
    col("lat"),
    col("lng"),
    col("alt")
)

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

df_circuits= circuits_selected_df.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude")

# COMMAND ----------

df_circuits.show()

# COMMAND ----------

df_circuits= df_circuits.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(df_circuits)

# COMMAND ----------

df_circuits.write.mode('overwrite').parquet("abfss://pyspark@sakimo2023.dfs.core.windows.net/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls abfss://pyspark@sakimo2023.dfs.core.windows.net/processed/circuits

# COMMAND ----------

df= spark.read.parquet(path)
display(df)

# COMMAND ----------

df_circuits=df_circuits.drop('url')

# COMMAND ----------

display(df_circuits)

# COMMAND ----------


