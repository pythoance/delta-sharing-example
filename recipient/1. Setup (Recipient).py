# Databricks notebook source
# MAGIC %sql
# MAGIC -- CREATE THE TABLES THAT ARE GOING TO BE USED DURING THIS EXAMPLE
# MAGIC CREATE TABLE IF NOT EXISTS main.default.recipient_example (origin STRING, amount BIGINT, latest_version BIGINT);
# MAGIC CREATE TABLE IF NOT EXISTS main.default.second_recipient_example (origin STRING, amount BIGINT);
