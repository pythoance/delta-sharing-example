# Databricks notebook source
# MAGIC %sql
# MAGIC -- DELETE ALL TABLES AND VIEWS THAT WERE USED DURING THE EXAMPLE
# MAGIC 
# MAGIC DROP TABLE IF EXISTS main.default.recipient_example;
# MAGIC DROP TABLE IF EXISTS main.default.second_recipient_example;
# MAGIC DROP VIEW IF EXISTS main.default.recipient_example_changes;

# COMMAND ----------

# MAGIC %run "1. Setup (Recipient)"
