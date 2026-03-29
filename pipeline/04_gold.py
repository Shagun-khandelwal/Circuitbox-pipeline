# Databricks notebook source
# MAGIC %md
# MAGIC ### ===========================================================
# MAGIC ### CIRCUITBOX — GOLD LAYER
# MAGIC - Aggregated KPI tables + customer_order_summary
# MAGIC - (Materialized View equivalent — recomputed on every run)
# MAGIC ### ===========================================================

# COMMAND ----------

# MAGIC %run ./00_utils

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = PipelineLogger("GOLD")

# COMMAND ----------

# MAGIC %md
# MAGIC fetch all silver tables

# COMMAND ----------

orders = spark.table("circuitbox.silver.orders")
customers = spark.table("circuitbox.silver.dim_customers")
addresses = (spark.table("circuitbox.silver.dim_addresses").filter(F.col("is_current") == True)
.select("customer_id","city","state","full_address"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Revenue by day

# COMMAND ----------

revenue_by_day = (
    orders
    .groupBy("order_date","order_month","order_year")
    .agg(
        F.round(F.sum("item_total"),2).alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.avg("item_total"),2).alias("avg_order_value"),
        F.sum("item_quantity").alias("total_units_sold")
    )
    .orderBy("order_date")
)

write_delta(
    revenue_by_day,
    "circuitbox.gold.revenue_by_day",
    mode="overwrite",
    logger=logger)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Performance

# COMMAND ----------

product_perf = (
    orders
    .groupBy("item_id","item_name","item_category")
    .agg(
        F.sum("item_quantity").alias("Total_units_sold"),
        F.round(F.sum("item_total"),2).alias("Total_revenue"),
        F.countDistinct("order_id").alias("times_ordered"),
        F.round(F.avg("item_price"),2).alias("avg_price")
    )
    .orderBy(F.desc("Total_revenue"))
)

write_delta(
    product_perf,
    "circuitbox.gold.product_performance",
    mode="overwrite",
    logger = logger
)