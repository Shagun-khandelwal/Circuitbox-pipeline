# Databricks notebook source
# MAGIC %md
# MAGIC ### Create catalog circuitbox

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists circuitbox
# MAGIC comment 'CircuitBox Catalog which is a top level container for all schemas';

# COMMAND ----------

# MAGIC %md
# MAGIC ### create schemas raw_landing,bronze,silver,gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists circuitbox.raw_landing;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists circuitbox.bronze;
# MAGIC create schema if not exists circuitbox.silver;
# MAGIC create schema if not exists circuitbox.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ### create volume -> circuitbox -> raw_landing -> Circuitbox_files
# MAGIC Also add data files in this volume 

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume if not exists circuitbox.raw_landing.circuitbox_files;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test volume circuitbox_files data files

# COMMAND ----------

dbutils.fs.ls('/Volumes/circuitbox/raw_landing/circuitbox_files/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test reading the files

# COMMAND ----------

# Test orders Json 
orders_test = spark.read.json("/Volumes/circuitbox/raw_landing/circuitbox_files/orders")
orders_test.printSchema()
orders_test.show(3,truncate=False)

# COMMAND ----------

# Test customers json
customers_test = spark.read.json("/Volumes/circuitbox/raw_landing/circuitbox_files/customers")
customers_test.printSchema()
customers_test.show(3,truncate=False)


# COMMAND ----------

# Test addresses CSV
addresses_test = spark.read \
    .option("header","True") \
    .option("inferSchema","True") \
    .csv("/Volumes/circuitbox/raw_landing/circuitbox_files/addresses")
addresses_test.printSchema()
addresses_test.show(3,truncate=False)