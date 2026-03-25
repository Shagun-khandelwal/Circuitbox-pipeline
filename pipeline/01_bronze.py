# Databricks notebook source
# MAGIC %run ./00_utils

# COMMAND ----------

from pyspark.sql import functions as F

logger = PipelineLogger("Bronze")

Base = "/Volumes/CircuitBox/raw_landing/circuitbox_files"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders

# COMMAND ----------

# DBTITLE 1,Cell 4
try:
    df_orders = (
        spark.read
        .format("json")
        .option("inferSchema","true")
        .option("multiLine","false")
        .load(f"{Base}/orders/")
        .withColumn("_ingested_at",F.current_timestamp())
        .withColumn("_source_file",F.col("_metadata.file_name"))
    )
    write_delta(df_orders,
                "CircuitBox.bronze.orders",
                mode="overwrite",
                logger=logger)
except Exception as e:
    logger.log("bronze_orders","FAIL",0,str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

try:
    df_customers = (
        spark.read
        .format("json")
        .option("inferSchema","true")
        .load(f"{Base}/customers/")
        .withColumn("_ingested_at",F.current_timestamp())
        .withColumn("_source_file",F.col("_metadata.file_name"))
    )
    write_delta(df_customers,
                "CircuitBox.bronze.customers",
                mode="overwrite",
                logger=logger)
except Exception as e:
    logger.log("bronze_customers","FAIL",0,str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC Addresses

# COMMAND ----------

try:
    df_addresses = (
        spark.read.format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .load(f"{Base}/addresses/")
        .withColumn("_ingested_at",F.current_timestamp())
        .withColumn("_source_file",F.col("_metadata.file_name"))        
    )
    write_delta(df_addresses,
                "CircuitBox.bronze.addresses",
                mode="overwrite",
                logger=logger)
except Exception as e:
    logger.log("bronze_products","FAIL",0,str(e))
    raise

logger.summary()