# Databricks notebook source
# MAGIC %run "../pyspark_udemy_course _includes/configuration"

# COMMAND ----------

# MAGIC %run "../pyspark_udemy_course _includes/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
widget_value=dbutils.widgets.get('p_data_source')
widget_value

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import *

# COMMAND ----------

df_circuits = spark.read \
    .option('header', True) \
    .option('inferSchema', True) \
    .csv(f"{raw_folder_path}/circuits.csv")


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
    .csv(f'{raw_folder_path}/circuits.csv')

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
.withColumnRenamed("alt","altitude") \
.withColumn('source',lit(widget_value))

# COMMAND ----------

df_circuits.show()

# COMMAND ----------

df_circuits= add_ingestion_date(df_circuits)

# COMMAND ----------

display(df_circuits)

# COMMAND ----------

df_circuits=df_circuits.drop('url')

# COMMAND ----------

display(df_circuits)

# COMMAND ----------

df_circuits.write.mode('overwrite').parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text('p_data_source','')

# COMMAND ----------

dbutils.notebook.exit("1. ingests Circuits - Success")
