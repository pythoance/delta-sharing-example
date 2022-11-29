# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **This notebook reads data from a delta table shared with them using delta open share and it overwrites a local delta table with the new data.**
# MAGIC 
# MAGIC To Note: This notebook will have to be ran every time the updated data is needed as the deltaSharing format does not support streamed reading for continuous data reading

# COMMAND ----------

# Install the python delta sharing connector. This connector allows the loading of data using the alternative ways. It also allows for listing all available schemas and tables when using the SharingClient. 
# %pip install delta-sharing
# import delta_sharing

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

# Read the data with all the changes applied

data = spark.read\
    .format("deltaSharing")\
    .load(table_url)

# COMMAND ----------

# Alternative way to read the data using the delta-sharing connector
# data = delta_sharing.load_as_spark(table_url)

# Alternative way to read the data using the delta-sharing connector to Pandas
# data = delta_sharing.load_as_pandas(table_url)

# COMMAND ----------

# Overwrite the old data with the new data

data.write.mode("overwrite").saveAsTable("main.default.second_recipient_example")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DISPLAY THE NEW AVAILABLE DATA
# MAGIC 
# MAGIC SELECT * FROM main.default.second_recipient_example
