# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS fmcg;
# MAGIC USE CATALOG fmcg;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA fmcg.gold;
# MAGIC CREATE SCHEMA fmcg.silver;
# MAGIC CREATE SCHEMA fmcg.bronze;