# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **This notebook reads data changes from a delta table shared with them using delta open share and it updates a local delta table only with the new changes.**
# MAGIC 
# MAGIC To Note: This notebook will have to be ran every time the updated data is needed as the deltaSharing format does not support streamed reading for continuous changes reading

# COMMAND ----------

# Install the python delta sharing connector. This connector allows the loading of data using the alternative ways. It also allows for listing all available schemas and tables when using the SharingClient. 
# %pip install delta-sharing
# import delta_sharing

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import rank, col, when

# COMMAND ----------

# Declare the location of the profile file that you downloaded from the activation link that was provided by the share granter

file = "settings.share"
profile_file = "/dbfs/FileStore/keys/" + file

# COMMAND ----------

# List all the available tables

# client = delta_sharing.SharingClient(profile_file)
# print("Tables:")
# for share in client.list_shares():
#     for schema in client.list_schemas(share):
#         for table in client.list_tables(schema):
#             print(f"{share}.{schema}.{table}")

# COMMAND ----------

# Declare the shared table path: <profile_file_location>#<share_name>.<schema_name>.<table_name>

table_url = "dbfs:/FileStore/keys/" + file + "#example_share.default.example_table"

# COMMAND ----------

# Display the first 5 changes as an example of how the CDF table looks

spark.read\
    .format("deltaSharing")\
    .option("readChangeFeed", "true")\
    .option("startingVersion", 0)\
    .load(table_url)\
    .show(5, True)

# COMMAND ----------

# Read the changes that have been made
# Exclude changes older than the latest changes that were read
# Exclude update_preimage changes as they just show the old values, before an update happened
# Include only the latest change for a row

windowSpec = Window.partitionBy("origin").orderBy("_commit_version")
latest_version = spark.sql("""
    SELECT coalesce(max(latest_version) ,0)
    FROM main.default.recipient_example
    LIMIT 1
""").collect()[0][0]

changes = spark.read\
    .format("deltaSharing")\
    .option("readChangeFeed", "true")\
    .option("startingVersion", latest_version)\
    .load(table_url)\
    .where("_change_type <> 'update_preimage'")\
    .withColumn("_change_type", when(col("_change_type") == "update_postimage", "update").otherwise(col("_change_type")))\
    .withColumn("rank", rank().over(windowSpec))\
    .where("rank = 1")

# Create a temporary view of the changes so they can be processed
changes.createOrReplaceTempView("recipient_example_changes")

# Display the changes
display(changes)

# COMMAND ----------

# Alternative way to read the change data feed using the delta-sharing connector
# data = delta_sharing.load_table_changes_as_spark(table_url)

# Alternative way to read the change data feed using the delta-sharing connector to Pandas
# data = delta_sharing.load_table_changes_as_pandas(table_url)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE CHANGES INTO THE ALREADY AVAILABLE DATA
# MAGIC -- MAKE SURE THAT ONLY NEW CHANGES ARE APPLIED
# MAGIC 
# MAGIC MERGE INTO main.default.recipient_example as TARGET
# MAGIC USING recipient_example_changes as SOURCE
# MAGIC ON TARGET.origin = SOURCE.origin
# MAGIC WHEN MATCHED AND SOURCE._change_type='update' AND TARGET.latest_version < SOURCE._commit_version THEN UPDATE SET TARGET.amount = SOURCE.amount, TARGET.latest_version = SOURCE._commit_version
# MAGIC WHEN MATCHED AND SOURCE._change_type='delete' AND TARGET.latest_version < SOURCE._commit_version THEN DELETE
# MAGIC WHEN NOT MATCHED AND SOURCE._change_type <> 'delete' THEN INSERT (TARGET.origin, TARGET.amount, TARGET.latest_version) VALUES (SOURCE.origin, SOURCE.amount, SOURCE._commit_version)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DISPLAY THE AVAILABLE DATA WITH THE CHANGES APPLIED
# MAGIC 
# MAGIC SELECT * FROM main.default.recipient_example
