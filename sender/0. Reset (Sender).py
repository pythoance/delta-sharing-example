# Databricks notebook source
# MAGIC %sql
# MAGIC -- REMOVE THE SHARED TABLES FROM THE SHARE SO THEY CAN BE DELETED
# MAGIC 
# MAGIC ALTER SHARE example_share REMOVE TABLE main.default.example_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DELETE THE SHARED TABLES
# MAGIC 
# MAGIC DROP TABLE IF EXISTS main.default.example_table;

# COMMAND ----------

# DELETE THE CHECKPOINTS AND ALL EXTRA SAVED DATA

dbutils.fs.rm("dbfs:/tmp/delta/flight_data/", True)

# COMMAND ----------

# MAGIC %run "./1. Setup (Sender)"
