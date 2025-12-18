# Databricks notebook source
from pyspark.sql import functions

# COMMAND ----------

# DBTITLE 1,Read silver layer table
df_gold = spark.read.table("workspace.default.yellow_tripdata_silver")
df_gold.count()

# COMMAND ----------

# Total trips per day
count_of_trips_per_day = df_gold.withColumn("trip_date", df_gold["tpep_pickup_datetime"].cast('date')).groupBy("trip_date").count()
count_of_trips_per_day.display()

# COMMAND ----------

# Total revenue per day
total_revenue_per_day = df_gold.withColumn("trip_date", df_gold["tpep_pickup_datetime"].cast("date")).groupBy("trip_date").agg(functions.sum("fare_amount").alias("total_revenue"))
total_revenue_per_day.display()

# COMMAND ----------

# Average trip distance per day
average_trip_distance_per_day = df_gold.withColumn("trip_date", df_gold["tpep_pickup_datetime"].cast("date")).groupBy("trip_date").agg(functions.avg("trip_distance").alias("avg_trip_distance"))
average_trip_distance_per_day.display()

# COMMAND ----------

# Average fare per passenger count
df_gold.groupBy("passenger_count").agg(functions.avg("fare_amount")).display()

# COMMAND ----------

df_gold = (
    df_gold
    .withColumn("trip_date", df_gold["tpep_pickup_datetime"].cast("date"))
    .groupBy("trip_date")
    .agg(
        functions.count("*").alias("total_trips"),
        functions.sum("fare_amount").alias("total_revenue"),
        functions.avg("fare_amount").alias("avg_fare"),
        functions.avg("trip_distance").alias("avg_trip_distance"),
        )
)
df_gold.display()
df_gold.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.yellow_tripdata_gold")

# COMMAND ----------

