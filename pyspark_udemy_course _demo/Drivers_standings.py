# Databricks notebook source
# MAGIC %run "../pyspark_udemy_course _includes/configuration"

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import *

# COMMAND ----------

df_standings= spark.read.parquet(f'{presenting_folder_path}/race_results')

# COMMAND ----------

df_standings.show()

# COMMAND ----------

driver_standings_df = (
    df_standings.groupBy("race_year", "driver_name", "driver_nationality", "team")
    .agg(
        sum("points").alias("total_points"),
        count(when(col("position") == 1,True)).alias("number_of_wins"),
    )
    .orderBy(desc_nulls_last("race_year"), desc_nulls_last('total_points'),desc_nulls_last("number_of_wins"))
)

# COMMAND ----------

display(driver_standings_df)

# COMMAND ----------

from pyspark.sql.window import * 

# COMMAND ----------

window_spec=Window.partitionBy('race_year').orderBy(desc('total_points'),desc('number_of_wins'))
final_standings=driver_standings_df.withColumn('rank',rank().over(window_spec))

# COMMAND ----------

display(final_standings.orderBy(desc('race_year')))

# COMMAND ----------


