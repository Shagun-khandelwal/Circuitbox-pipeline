# Databricks notebook source
# MAGIC %md
# MAGIC ### CircuitBox - SCD Layer
# MAGIC - SCD Type 1 -> dim_customers (overwrite, no history)
# MAGIC - SCD Type 2 -> dim_addresses (full history, date ranges)
# MAGIC - Uses apply_scd1() and apply_scd2() from 00_utils

# COMMAND ----------

# MAGIC %run ./00_utils

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

logger = PipelineLogger("SCD")

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD Type 1 : dim_customers

# COMMAND ----------

df_customers = spark.table("circuitbox.silver.customers")

apply_scd1(
    spark = spark,
    source_df = df_customers,
    target_table= "circuitbox.silver.dim_customers",
    key_cols= ["customer_id"],
    logger = logger
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD Type 2: dim_addresses

# COMMAND ----------

df_addresses = spark.table("circuitbox.silver.addresses")

apply_scd2(
    spark = spark,
    source_df= df_addresses,
    target_table="circuitbox.silver.dim_addresses",
    key_cols = ["customer_id"],
    # Only trigger a new history row if these columns change
    track_cols= ["address_line_1","city","state","postcode"],
    logger = logger
)

# COMMAND ----------

logger.summary()