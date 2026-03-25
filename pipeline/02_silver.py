# Databricks notebook source
# MAGIC %md
# MAGIC ### Circuitbox - Silver Layer
# MAGIC - Clean + Validate using shared DQ Engine from 00_utils
# MAGIC - Orders -> mixed rules
# MAGIC - Customers -> SQL style rules ( readable conditions as strings)
# MAGIC - Addresses -> Python-style (complex multi-column checks)

# COMMAND ----------

# MAGIC %run ./00_utils

# COMMAND ----------

from pyspark.sql import functions as F

logger = PipelineLogger("Silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders Silver

# COMMAND ----------

df_orders = spark.table("circuitbox.bronze.orders")

df_orders_exploaded = (
    df_orders
    .withColumn("item", F.explode("items"))
    .withColumn("item_id",F.col("item.item_id"))
    .withColumn("item_name",F.col("item.name"))
    .withColumn("item_category",F.col("item.category"))
    .withColumn("item_price",F.col("item.price").cast("double"))
    .withColumn("item_quantity",F.col("item.quantity").cast("int"))
    .withColumn(("item_total"),F.col("item_price")*F.col("item_quantity"))
    .withColumn("order_timestamp",F.to_timestamp("order_timestamp","yyyy-MM-dd HH:mm:ss"))
    .withColumn("order_date",F.to_date("order_timestamp"))
    .withColumn("order_hour",F.hour("order_timestamp"))
    .withColumn("order_month",F.month("order_timestamp"))
    .withColumn("order_year",F.year("order_timestamp"))
    .withColumn("_processed_at",F.current_timestamp())
    .drop("items","item")
)

order_rules = [
    # fail -> stop the whole pipeline if PK is missing
    {
        "name":"order_id_not_null",
        "condition": F.col("order_id").isNotNull(),
        "action": "fail"
    },
    {
        "name": "customer_id_not_null",
        "condition": F.col("customer_id").isNotNull(),
        "action": "fail"            
    },

    # drop -> remove rows with invalid status
    {
        "name":"valid_order_status",
        "condition": F.col("order_status").isin(
            ["Pending","Shipped","Completed","Cancelled"]
        ),
        "action": "drop"
    },

    # warn -> keep row, flag in DQ report
    {
        "name": "known_payment_method",
        "condition": F.col("payment_method") != "Unknown",
        "action": "warn"
    },
    {
        "name": "positive_item_price",
        "condition": F.col("item_price") > 0,
        "action": "warn"
    }
]

df_orders_clean = run_quality_checks(
    df_orders_exploaded,order_rules,"silver_orders",logger
)

write_delta(df_orders_clean,
            "circuitbox.silver.orders",
            mode="overwrite",
            logger=logger)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers Silver
# MAGIC SQL style rules

# COMMAND ----------

df_customers = spark.table("Circuitbox.bronze.customers")

df_customers_clean = (
    df_customers
    .withColumn("date_of_birth",F.to_date("date_of_birth","yyyy-MM-dd"))
    .withColumn("created_date",F.to_date("created_date","yyyy-MM-dd"))
    .withColumn("age",F.floor(
        F.datediff(F.current_date(),F.col("date_of_birth"))/365.25
    ))
    .withColumn("age_group",
        F.when(F.col("age")<20, "Under 20")
        .when(F.col("age") < 25,"20-24")
        .when(F.col("age") < 30,"25-29")
        .otherwise("30+"))
    .withColumn("email_domain",F.when(F.col("email").isNotNull(),F.split(F.col("email"),"@")[1]))
    .withColumn("contact_completeness",(
        F.col("email").isNotNull().cast("int") + 
        F.col("telephone").isNotNull().cast("int") +
        F.col("customer_name").isNotNull().cast("int")
    ))
    .withColumn("_processed_at",F.current_timestamp())
)

customer_rules = [
    {
        "name":"customer_id_not_null",
        "condition":F.expr("customer_id is not null"),
        "action":"fail"
    },
    {
        "name":"vaild_date_of_birth",
        "condition":F.expr("date_of_birth < current_date()"),
        "action":"warn"
    },
    {
        "name":"name_not_empty",
        "condition":F.expr(
            "customer_name is not null and length(trim(customer_name)) > 0"
            ),
        "action":"warn"
    },
    {
        "name":"valid_email_format",
        "condition":F.expr("email is not null or email like '%@%.%'"),
        "action":"warn"
    },
    {
        "name":"valid_phone_format",
        "condition":F.expr("telephone is not null or telephone like '+%'"),
        "action":"warn"
    }
]

df_customers_dq = run_quality_checks(
    df_customers_clean, customer_rules, "silver_customers",logger
)

write_delta(df_customers_dq,
            "circuitbox.silver.customers",
            mode="overwrite",
            logger=logger
)
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Addresses silver
# MAGIC Python style rules

# COMMAND ----------

df_addresses = spark.table("circuitbox.bronze.addresses")

df_addresses_clean = (
    df_addresses
    .withColumn("postcode",
                F.col("postcode").cast("string"))
    .withColumn("created_date",
                F.to_date("created_date","yyyy-MM-dd"))
    .withColumn("state",F.upper(F.trim(F.col("state"))))
    .withColumn("city",F.initcap(F.trim(F.col("city"))))
    .withColumn("full_address",
            F.concat_ws(", ",
            F.col("address_line_1"),
            F.col("city"),
            F.col("state"),
            F.col("postcode")))
    .withColumn("_processed_at",F.current_timestamp())    
)

addresses_rules = [
    {
        "name":"addresses_has_customer_id",
        "condition":F.col("customer_id").isNotNull(),
        "action":"fail"
    },
    {
        "name":"valid_postcode",
        "condition": (
            F.col("postcode").cast("int").isNotNull() & (F.length(F.col("postcode"))>=4) &
            (F.length(F.col("postcode"))<=6)
        ),
        "action":"warn"
    },
    {
        "name":"valid_state_format",
        "condition":(F.col("state").isNotNull() & F.col("state").rlike("^[A-Z]+$")),
        "action":"warn"
    },
    {
        "name":"valid_city_name",
        "condition":(F.col("city").isNotNull() & (~F.col("city").rlike("^[0-9]+$"))),
        "action":"warn"
    },
    {
        "name":"address_line_not_empty",
        "condition":(
            F.col("address_line_1").isNotNull() &
            (F.length(F.trim(F.col("address_line_1")))>0)
        ),
        "action":"warn"
    }
]

df_addresses_dq = run_quality_checks(df_addresses_clean, addresses_rules, "silver_addresses",logger)

write_delta(df_addresses_dq,
            "circuitbox.silver.addresses",
            mode="overwrite",
            logger=logger
)

# COMMAND ----------

logger.summary()