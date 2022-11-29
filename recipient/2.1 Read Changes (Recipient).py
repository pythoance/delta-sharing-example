# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **This notebook reads data changes from a delta table shared with them using delta open share and it updates a local delta table only with the new changes.**
# MAGIC 
# MAGIC To Note: This notebook will have to be ran every time the updated data is needed as the deltaSharing format does not support streamed reading for continuous changes reading

# COMMAND ----------

# Install the python delta sharing connector. This connector allows the loading of data using the alternative ways. It also allows for listing all available schemas and tables when using the SharingClient. 
%pip install delta-sharing

# COMMAND ----------

import delta_sharing
from pyspark.sql import Window
from pyspark.sql.functions import rank, col, when

# COMMAND ----------

# Declare the location of the profile file that you downloaded from the activation link that was provided by the share granter

file = "config.share"
profile_file = "/dbfs/FileStore/keys/" + file

# COMMAND ----------

# List all the available tables

client = delta_sharing.SharingClient(profile_file)
print("Tables:")
for share in client.list_shares():
    for schema in client.list_schemas(share):
        for table in client.list_tables(schema):
            print(f"  - {share.name}.{schema.name}.{table.name}")

# COMMAND ----------

# Declare the shared table path: <profile_file_location>#<share_name>.<schema_name>.<table_name>

table_url = "dbfs:/FileStore/keys/" + file + "#example_share.default.example_table"

# COMMAND ----------

# Display the first 5 changes as an example of how the CDF table looks

delta_sharing.load_table_changes_as_spark(url=table_url, starting_version= 0).show(5)

# COMMAND ----------

# Read the changes that have been made. From the read changes, some are excluded:
#   - versions older than the latest recorded version in our local table. the newest recorded one will be reprocessed for continuity
#   - update_preimage changes because they just show the previous value in case of a row update
#   - old versions of a row because only the latest version matters

windowSpec = Window.partitionBy("origin").orderBy("_commit_version")
latest_version = spark.sql("""
    SELECT coalesce(max(latest_version) ,0)
    FROM main.default.recipient_example
    LIMIT 1
""").collect()[0][0]

changes = delta_sharing.load_table_changes_as_spark(url=table_url, starting_version=latest_version)\
    .where("_change_type <> 'update_preimage'")\
    .withColumn("_change_type", when(col("_change_type") == "update_postimage", "update").otherwise(col("_change_type")))\
    .withColumn("rank", rank().over(windowSpec))\
    .where("rank = 1")

# Create a temporary view of the changes so they can be processed
changes.createOrReplaceTempView("recipient_example_changes")

# Display the changes
display(changes)

# To read the change data feed as pandas DataFrames use:
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
