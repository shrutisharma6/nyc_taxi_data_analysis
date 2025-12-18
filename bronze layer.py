# Databricks notebook source
# DBTITLE 1,Read the table data
df_bronze = spark.read.table("workspace.default.yellow_tripdata")

# COMMAND ----------

# DBTITLE 1,Display top 5 rows
df_bronze.display(5)

# COMMAND ----------

# DBTITLE 1,Inspect the schema
df_bronze.printSchema()

# COMMAND ----------

# DBTITLE 1,Save as bronze table
df_bronze.write.format('delta').mode('overwrite').saveAsTable('workspace.default.yellow_tripdata_bronze')

# COMMAND ----------

# DBTITLE 1,Count total rows
df_bronze.count()

# COMMAND ----------

# Count trips have missing pickup or dropoff times
df_bronze.filter(df_bronze["tpep_pickup_datetime"].isNull()| df_bronze["tpep_dropoff_datetime"].isNull()).count()

# COMMAND ----------

# Trips with zero or negative fare or distance
df_bronze.filter((df_bronze["fare_amount"] < 0) | (df_bronze["trip_distance"] < 0)).display()

# COMMAND ----------

