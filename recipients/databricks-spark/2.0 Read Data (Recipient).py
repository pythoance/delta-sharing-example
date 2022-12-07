# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **This notebook reads data from a delta table shared using delta open share and it overwrites a local delta table with the new data.**
# MAGIC 
# MAGIC To Note: This notebook needs to be ran every time the updated data is needed as the deltaSharing format does not support continuous data reading.

# COMMAND ----------

# Install the python delta sharing connector. This connector allows the loading of data as spark DataFrames or pandas DataFrames. It also allows for listing of all available schemas and tables.
%pip install delta-sharing

# COMMAND ----------

import delta_sharing

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

# Read the data with all the changes applied

data = delta_sharing.load_as_spark(table_url)

# To read the data as pandas DataFrames use:
# data = delta_sharing.load_as_pandas(table_url)

# COMMAND ----------

# Overwrite the old data with the new data

data.write.mode("overwrite").saveAsTable("main.default.second_recipient_example")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DISPLAY THE NEW AVAILABLE DATA
# MAGIC 
# MAGIC SELECT * FROM main.default.second_recipient_example
