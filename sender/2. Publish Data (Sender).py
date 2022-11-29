# Databricks notebook source
# MAGIC %md
# MAGIC Example data was manually uploaded to DBFS:/FileStore/tables/flight-data/ and is available in the csv format.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import expr

# COMMAND ----------

# Declare the data location and schema

file = 'all'
path = f"dbfs:/FileStore/flight-data/{file}/"

schema = StructType([
  StructField("origin", StringType(), True),
  StructField("amount", LongType(), True)
])

# COMMAND ----------

# Read the data, 1 file at a time
data = spark\
    .readStream\
    .option("header", "true")\
    .option("maxFilesPerTrigger", 1)\
    .schema(schema)\
    .csv(path)

# COMMAND ----------

# Process and write the data to the table that was shared
query = data\
    .writeStream\
    .outputMode("append")\
    .trigger(processingTime="30 seconds")\
    .option("checkpointLocation", f"/tmp/delta/flight_data/{file}/_checkpoints/")\
    .toTable("main.default.example_table")

query.awaitTermination()
