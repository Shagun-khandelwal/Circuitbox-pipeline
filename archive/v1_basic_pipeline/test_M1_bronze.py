# Databricks notebook source
# Test 1 : Tables exists
tables = spark.sql("Show tables in circuitbox.bronze").collect()
table_names = [t.tableName for t in tables]
print(table_names)

# COMMAND ----------

# Test 2: Row counts match source files
orders_count = spark.table("circuitbox.bronze.orders_raw").count()
customers_count =spark.table("circuitbox.bronze.customers_raw").count()
addresses_count = spark.table("circuitbox.bronze.addresses_raw").count()

print(f"Orders -> {orders_count} rows (expected: 12)")
print(f"Customers -> {customers_count} rows (expected: 10)")
print(f"Addresses -> {addresses_count} rows (expected: 10)")

# auto-pass/fail
assert orders_count == 12, f" Orders count mismatch! Got {orders_count}"
assert customers_count == 10, f"Customers count mismatch! Got {customers_count}"
assert addresses_count == 10, f"Addresses count mismatch! Got {addresses_count}"


# COMMAND ----------

# Test 3: Spot check actual data
print("=== Orders Sample ===")
spark.table("circuitbox.bronze.orders_raw") \
    .select("order_id","customer_id","order_status","payment_method","order_timestamp") \
    .show(5,truncate=False)

print("=== Customers Sample ===")
spark.table("circuitbox.bronze.customers_raw") \
    .select("customer_id","customer_name","email","telephone") \
    .show(5,truncate=False)

print("=== Addresses Sample ===")
spark.table("circuitbox.bronze.addresses_raw") \
    .select("customer_id","city","state","postcode") \
    .show(5,truncate=False)
