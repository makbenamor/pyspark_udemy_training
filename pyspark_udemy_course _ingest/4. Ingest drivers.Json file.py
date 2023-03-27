# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Read files from a Nested Json file
# MAGIC 
# MAGIC 1. Create the schema

# COMMAND ----------

# MAGIC %run "../pyspark_udemy_course _includes/common_functions"

# COMMAND ----------

# MAGIC %run "../pyspark_udemy_course _includes/configuration"

# COMMAND ----------

from pyspark.sql.types import * 

# COMMAND ----------

name_schema= StructType(fields= 
                        [
                            StructField("forename", StringType(),True),
                            StructField("surname", StringType(), True)                            
                        ])

# COMMAND ----------

input_schema= StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

# Read the Json file to the DF
drivers_df = spark.read \
.schema(input_schema) \
.json(f'{raw_folder_path}/drivers.json')

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f'{processed_folder_path}/Drivers')

# COMMAND ----------

dbutils.notebook.exit("4. Ingest drivers.Json file - Success")
