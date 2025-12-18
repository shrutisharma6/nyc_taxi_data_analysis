# Databricks notebook source
# DBTITLE 1,Read data from bronze layer
df_silver = spark.read.table("workspace.default.yellow_tripdata_bronze")

# COMMAND ----------

# DBTITLE 1,Inspect schema
df_silver.printSchema()
df_silver.count()

# COMMAND ----------

# DBTITLE 1,Remove duplicates
df_silver = df_silver.dropDuplicates()
df_silver.count()

# COMMAND ----------

# DBTITLE 1,Filter invalid trips
# Remove trips with null pickup/dropoff times, zero/negative fare, or distance.
df_silver = df_silver.filter(
    (df_silver["tpep_pickup_datetime"].isNotNull()) &
    (df_silver["tpep_dropoff_datetime"].isNotNull()) &
    (df_silver["fare_amount"] > 0) &
    (df_silver["trip_distance"] > 0) 

)
# df_silver.display()
df_silver.count()

# COMMAND ----------

# Add trip_duration in minutes.
df_silver = df_silver.withColumn("trip_duration", (df_silver["tpep_dropoff_datetime"].cast('long')-df_silver["tpep_pickup_datetime"].cast('long'))/60)
df_silver.display()


# COMMAND ----------

# Check distinct payment types
df_silver.select("payment_type").distinct().show()

# COMMAND ----------

# Count trips per passenger count
df_silver.groupBy("passenger_count").count().show()

# COMMAND ----------

# DBTITLE 1,Write into the table
df_silver.write.format('delta').mode('overwrite').saveAsTable('workspace.default.yellow_tripdata_silver')