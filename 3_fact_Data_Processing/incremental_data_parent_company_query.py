# Databricks notebook source
# MAGIC %sql
# MAGIC COPY INTO fmcg.gold.fact_orders
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     CAST(date AS DATE) AS date,
# MAGIC     product_code,
# MAGIC     customer_code,
# MAGIC     CAST(sold_quantity AS BIGINT) AS sold_quantity
# MAGIC   FROM '/Volumes/fmcg/gold/parent_incremental_update/fact_orders.csv'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true');