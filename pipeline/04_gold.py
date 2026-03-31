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

# COMMAND ------

# %md
# ### Order Status funnel

# COMMAND ------

window_all = Window.rowsBetween(
    Window.unboundedPreceding, Window.unboundedFollowing
)

status_funnel = (
    orders
    .groupBy("order_status")
    .agg(
        F.countDistinct("order_id").alias("order_count")
    )
    .withColumn("total",F.sum("order_count").over(window_all))
    .withColumn("pct_of_total",F.round(F.col("order_count")/F.col("total")*100,1))
    .drop("total")
)

write_delta(status_funnel,
            "circuitbox.gold.order_status_funnel",mode ="overwrite",logger = logger)

# COMMAND -------

# %md
# ### Payment mix

# COMMAND -------

payment_mix = (
    orders
    .groupBy("payment_method")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.round(F.sum("item_total"),2).alias("total_revenue")
    )
)

write_delta(payment_mix,
            "circuitbox.gold.payment_mix",
            mode="overwrite",
            logger=logger)

# COMMAND ---------------

# %md
# ### Materialized view Equivalent
# - customer_order_summary - pre_joined, recomputed every run
# - Power BI reads directly - no joins needed

# COMMAND ----------------

w = Window.rowsBetween(
    Window.unboundedPreceding, Window.unboundedFollowing
)

customer_summary = (
    orders
    .groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.sum("item_total"),2).alias("lifetime_value"),
        F.round(F.avg("item_total"),2).alias("avg_order_value"),
        F.max("order_date").alias("last_order_date"),
        F.min("order_date").alias("first_order_date"),
        F.datediff(F.current_date(),F.col("last_order_date")).alias("days_since_last_order")
    )
    .withColumn("customer_segment",
                F.when(F.col("lifetime_value")>=1000,"VIP")
                .when(F.col("lifetime_value") >= 500, "Regular")
                .otherwise("New")
                )
    .join(customers.select("customer_id","customer_name","age","age_group","email_domain","contact_completeness"),"customer_id","left")
    .join(addresses,"customer_id","left")
)
write_delta(customer_summary,
            "circuitbox.gold.customer_order_summary",
            mode="overwrite",
            logger=logger)


logger.summary()