# Databricks notebook source
# MAGIC %run "../pyspark_udemy_course _includes/configuration"

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import *

# COMMAND ----------

df_races = (
    spark.read.csv(f"{raw_folder_path}/races.csv", header=True, inferSchema=True)
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("circuitId", "circuit_id")
    .withColumnRenamed("year", 'race_year')
    .withColumnRenamed('date','race_date')
    .withColumnRenamed('name','race_name')
)

# COMMAND ----------

df_races.display()

# COMMAND ----------

df_circuits = (
    spark.read.csv(f"{raw_folder_path}/circuits.csv", header=True, inferSchema=True)
    .withColumnRenamed("circuitRef", "circuit_ref")
    .withColumnRenamed("circuitId", "circuit_id")
    .withColumnRenamed('location','circuit_location')
)

# COMMAND ----------

df_circuits.display()

# COMMAND ----------

schema1 = StructType(
    fields=[
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True),
    ]
)

schema2 = StructType(
    fields=[
        StructField("driverId", IntegerType(), False),
        StructField("driverRef", StringType(), False),
        StructField("code", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("name", schema1),
        StructField("nationality", StringType(), True),
        StructField("number", IntegerType(), True),
        StructField("url", StringType(), True),
    ]
)
df_drivers = spark.read.schema(schema2).json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

df_drivers = (
    df_drivers.withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("driverRef", "driver_ref")
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))
)

# COMMAND ----------

df_drivers = (
    df_drivers.withColumnRenamed("name", "driver_name")
    .withColumnRenamed("number", "driver_number")
    .withColumnRenamed("nationality", "driver_nationality")
)

# COMMAND ----------

df_drivers.display()

# COMMAND ----------

df_constructors = (
    spark.read.json(f"{raw_folder_path}/constructors.json")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("constructorRef", "constructor_ref")
    .withColumnRenamed("name", "team")
)

# COMMAND ----------

df_constructors.display()

# COMMAND ----------

df_results = (
    spark.read.option("mutiline", True)
    .json(f"{raw_folder_path}/results.json")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("resultId","result_id")
    .withColumnRenamed("statusId","status_id")
    .withColumnRenamed("time","race_time")
    .withColumnRenamed("fastestlap","fastest_lap")
)

# COMMAND ----------

# df_races_circuit = df_races + df_circuits
df_races_circuit= df_circuits.join(df_races,df_races.circuit_id==df_circuits.circuit_id,'inner').select(df_races.race_id, df_races.race_year, df_races.race_name, df_races.race_date, df_circuits.circuit_location)

# COMMAND ----------

# Join all the results to all other dataframes

race_results_df = df_results.join(df_races_circuit , df_results.race_id == df_races_circuit.race_id) \
                            .join(df_constructors, df_results.constructor_id==df_constructors.constructor_id) \
                            .join(df_drivers, df_drivers.driver_id == df_results.driver_id)


# COMMAND ----------

race_results_df.display()

# COMMAND ----------

df_final = race_results_df.select('race_year','race_name', 'race_date', 'circuit_location','driver_name','driver_number', 'driver_nationality', 'team' ,'grid' , col('fastest_lap'), col('race_time'), 'points', 'position' ).withColumn('created_date', current_timestamp())

# COMMAND ----------

df_final.display()

# COMMAND ----------

show= df_final.filter("race_year==2020 and race_name== 'Abu Dhabi Grand Prix'").orderBy(df_final.points.desc())

# COMMAND ----------

df_final.write.mode('overwrite').parquet(f'{presenting_folder_path}/race_results')

# COMMAND ----------

# MAGIC %md 
# MAGIC ####aggregations

# COMMAND ----------

#find out the nationality of drivers with most points in 2018, 2019 and 2020

agg1= df_final.groupBy('driver_nationality','race_year').agg(sum('points').alias('country_sum_points')).orderBy(desc_nulls_last('race_year'),desc_nulls_last('country_sum_points'))

# COMMAND ----------

agg1.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Window Function

# COMMAND ----------

demo_grouped = df_final.groupBy("race_year", "driver_name").agg(
    sum("points").alias("total_points"),
    countDistinct("race_name").alias("number_of_races")
).orderBy(desc_nulls_last('race_year'))

# COMMAND ----------

df_final.display()

# COMMAND ----------

df_outputdf_output=demo_grouped.filter('race_year in (2020, 2019)').orderBy('race_year',desc_nulls_last('total_points'))

# COMMAND ----------

df_output.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import * 

# COMMAND ----------

driver_rank= Window.partitionBy('race_year').orderBy(desc_nulls_last('total_points'))
df_output.withColumn('rank',rank().over(driver_rank)).show(100)

# COMMAND ----------


