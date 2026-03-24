# Databricks notebook source
# MAGIC %sql
# MAGIC create volume if not exists circuitbox.raw_landing.checkpoints;

# COMMAND ----------

Base_path = "/Volumes/circuitbox/raw_landing/circuitbox_files"
Checkpoint_base = "/Volumes/circuitbox/raw_landing/checkpoints"

#orders_json
orders_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format","json")
    .option("cloudFiles.inferColumnTypes","true")
    .option("cloudFiles.schemaLocation",f"{Checkpoint_base}/orders_schema")
    .load(f"{Base_path}/orders/")
)
(
    orders_raw.writeStream
    .format("delta")
    .option("checkpointLocation",f"{Checkpoint_base}/orders")
    .option("mergeSchema","true")
    .outputMode("append")
    .trigger(availableNow=True)
    .table("circuitbox.bronze.orders_raw")
)

# COMMAND ----------

# customers_json

customers_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format","json")
    .option("cloudFiles.inferColumnTypes","true")
    .option("cloudFiles.schemaLocation",f"{Checkpoint_base}/customers_schema")
    .load(f"{Base_path}/customers/")
)
(
    customers_raw.writeStream
    .format("delta")
    .option("checkpointLocation",f"{Checkpoint_base}/customers")
    .option("mergeSchema","true")
    .outputMode("append")
    .trigger(availableNow=True)
    .table("circuitbox.bronze.customers_raw")
)
# addresses_csv

addresses_raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("header","true")
    .option("inferSchema","true")
    .option("cloudFiles.schemaLocation",f"{Checkpoint_base}/addresses_schema")
    .load(f"{Base_path}/addresses/")
)
(
    addresses_raw.writeStream
    .format("delta")
    .option("checkpointLocation",f"{Checkpoint_base}/addresses")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("circuitbox.bronze.addresses_raw")
)