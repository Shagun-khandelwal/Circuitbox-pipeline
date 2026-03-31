# Databricks notebook source
# MAGIC %md
# MAGIC ### CircuitBox - shared Utilities
# MAGIC Note: Import this in every notebook: %run ./00_utils

# COMMAND ----------

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

# COMMAND ----------

# Logger -> mimics DLT pipeline run logs

class PipelineLogger:
    def __init__(self,layer: str):
        self.layer = layer
        self.start = datetime.now()
        self.results = []
        print(f"\n{'='*55}")
        print(f" Circuitbox Pipeline - {layer.upper()}")
        print(f" Started: {self.start.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*55}")
    
    def log(self,table:str, status: str, rows: int =0,msg :str =""):
        icon = "✅" if status == "PASS" else "❌" if status == "FAIL" else "⚠️"
        elapsed = (datetime.now() - self.start).seconds
        line = f"  {icon} [{elapsed:>3}s] {table:<35} {msg}"
        print(line)
        self.results.append({
            "layer": self.layer, "table": table,
            "status": status, "rows": rows,
            "message": msg,
            "run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    
    def summary(self):
        passed = sum(1 for r in self.results if r["status"] == "PASS")
        failed = sum(1 for r in self.results if r["status"] == "FAIL")
        warned = sum(1 for r in self.results if r["status"] == "WARN")
        print(f"\n{'='*55}")
        print(f"  SUMMARY  ✅ {passed} passed  ❌ {failed} failed  ⚠️  {warned} warned")
        print(f"{'='*55}\n")
        if failed > 0:
            raise Exception(f"Pipeline failed — {failed} table(s) had errors. Check logs above.")

# COMMAND ----------

# delta writer -- standardised write with logging

def write_delta(df: DataFrame, table:str, mode:str="append",logger:PipelineLogger=None,merge_keys:list=None):
    count = df.count()
    try:
        if mode in ("append","overwrite"): # append | merge | overwrite
            (
                df.write
                .format("delta")
                .mode(mode)
                .option("mergeSchema","true")
                .saveAsTable(table)
            )
        elif mode == "merge" and merge_keys:
            if spark.catalog.tableExists(table):
                dt = DeltaTable.forName(spark, table)
                condition = " And ".join([f"target.{k} = source.{k}" for k in merge_keys])
                (dt.alias("target")
                    .merge(df.alias("source"),condition)
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute())
        else:
            (df.write.format("delta")
             .mode("overwrite")
             .option("mergeSchema","true")
             .saveAsTable(table))
        if logger:
            logger.log(table.split(".")[-1],"PASS", count,f"Written {count} rows -> {table} (mode={mode})")
    except Exception as e:
        if logger:
            logger.log(table.split(".")[-1],"FAIL",0,str(e))
        raise
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Engine
# MAGIC Mimics @dlt.expect, @dlt.expect_or_drop, @dlt.expect_or_fail
# MAGIC

# COMMAND ----------

class DQResult:
    def __init__(self,df,rule_name,passed,failed,action):
        self.df = df
        self.rule = rule_name
        self.passed = passed
        self.failed = failed
        self.action = action # warn/ drop / fail
    
def run_quality_checks(
    df: DataFrame,
    rules: list,
    table_name: str,
    logger: PipelineLogger
) -> DataFrame:
        
    clean_df = df
    dq_rows = []

    for rule in rules:
        name = rule["name"]
        condition = rule["condition"]
        action = rule["action"] # warn | drop | fail

        passing = df.filter(condition)
        failing = df.filter(~condition)

        pass_count = passing.count()
        fail_count = failing.count()

        dq_rows.append({
            "run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "table_name":table_name,
            "rule_name":name,
            "action":action,
            "pass_count": pass_count,
            "fail_count": fail_count,
            "pass_rate": round(pass_count / (pass_count + fail_count) * 100,1) if (pass_count + fail_count) > 0 else 100.0
        })    

        if fail_count == 0:
            logger.log(table_name,"PASS",pass_count,f"DQ[{action.upper()}]  {name}: all {pass_count} rows passed")
        elif action == "warn":
            logger.log(table_name,"WARN",fail_count,f"DQ[WARN] {name}: {fail_count} rows failed (kept)")
        elif action == "drop": 
            clean_df = clean_df.filter(condition)
            logger.log(table_name,"WARN",fail_count,f"DQ [DROP] {name}: {fail_count} bad rows dropped")
        elif action == "fail": 
            logger.log(table_name,"FAIL",fail_count,f"DQ [FAIL] {name}: {fail_count} rows failed -- stopping pipeline")
            # Save DQ report before raising
            _save_dq_report(spark,dq_rows)
            raise Exception(
                f"DQ FAIL on table '{table_name}', rule '{name}':"
                f"{fail_count} rows violated: {name}"
                )
    # Save DQ report to delta table ( your quality dashboard source)
    _save_dq_report(spark,dq_rows)
    return clean_df

def _save_dq_report(spark,dq_rows:list):
    """Saves DQ results to circuitbox.gold.dq_report - used by Power BI."""
    
    if not dq_rows:
        return
    dq_df = spark.createDataFrame(dq_rows)
    (
        dq_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema","true")
        .saveAsTable("circuitbox.gold.dq_report")
    )

# COMMAND ----------

# %md
# ### SCD Type 1 Overwrite latest value (no history)
# Mimics: dlt.apply_changes(...,stored_as_scd_type=1)

# COMMAND

def apply_scd1(
    spark,
    source_df: DataFrame,
    target_table: str, # e.g. "circuitbox.silver.dim_customers"
    key_cols: list,
    logger: PipelineLogger = None
):
    """ 
    Upsert source into target on key_cols.
    Matching row -> update all columns (overwrite)
    New row  -> insert
    """

    count = source_df.count()

    if not spark.catalog.tableExists(target_table):
        # First run - just create it
        (
        source_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema","true")
        .saveAsTable(target_table)
        )
        if logger:
            logger.log("dim_customers","PASS",count,
            f"SCD1 initial load: {count} rows -> {target_table}")
        return
  
    dt = DeltaTable.forName(spark,target_table)

    merge_cond = " AND ".join(
        [f"target.{k} = source.{k}" for k in key_cols]
    )

    (dt.alias("target")
    .merge(source_df.alias("source"),merge_cond)
    .whenMatchedUpdateAll()  # type 1: overwrite all columns
    .whenNotMatchedInsertAll() # new customer : insert
    .execute()
    )
  
    if logger:
        logger.log("dim_customers","PASS",count,
        f"SCD1 upsert: {count} source rows merged into {target_table}"
        )

# COMMAND

# %md
# ### SCD Type 2 - keep full history with date ranges
# Mimics: dlt.apply_changes(...,stored_as_scd_type=2)

# COMMAND

def apply_scd2(
    spark,
    source_df: DataFrame,
    target_table: str, # e.g. "circuitbox.silver.dim_addresses"
    key_cols: list, # e.g. ["customer_id"]
    track_cols: list, #columns to detect changes on
    logger: PipelineLogger=None
):
    """
    For each key:
        - If key is new   -> insert as current row
        - If tracked cols changed -> expire old row , insert new current row
        - If nothing changed -> do nothing
    Adds columns: effective_start, effective_end (NULL = current), is_current
    """
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Add SCD2 metadata columns to incoming data
    source_df = (
        source_df
        .withColumn("effective_start",F.lit(today))
        .withColumn("effective_end",F.lit(None).cast("string"))
        .withColumn("is_current", F.lit(True))
    )

    if not spark.catalog.tableExists(target_table):
        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema","true")
            .saveAsTable(target_table)
        )
        count = source_df.count()
        if logger:
            logger.log("dim_addresses","PASS",count,
                       f"SCD2 initial load: {count} rows -> {target_table}")
        return
    
    dt = DeltaTable.forName(spark,target_table)
    target_df = dt.toDF()
    current_df = target_df.filter(F.col("is_current") == True)

    # Build join condition on key columns
    key_join_cond = [source_df[k] == current_df[k] for k in key_cols]

    # Step 1: Find changed keys
    # A key is "changed" when it exists in a target and at least one tracked column has a different value

    change_filter = None

    for col in track_cols:
        cond = source_df[col] != current_df[col]
        change_filter = cond if change_filter is None else (change_filter | cond)
    
    changed_keys_df = (
        source_df
        .join(current_df,key_join_cond,"inner")
        .filter(change_filter)
        .select([source_df[k] for k in key_cols])
        .distinct()
    )

    # step 2: Find brand new keys
    # keys in source that don't exist at all in target
    new_keys_df = (
        source_df
        .join(current_df,key_join_cond,
              "left_anti")
        .select([source_df[k] for k in key_cols])
        .distinct()
    )

    # Step 3: Combine changed + new keys

    keys_to_insert = changed_keys_df.union(new_keys_df)

    # Step 4: Expire current rows for changed keys
    # Build merge condition: target key = changed key and is_current
    expire_merge_cond = " AND ".join(
        [f"target.{k} = updates.{k}" for k in key_cols]
    )

    (
        dt.alias("target")
        .merge(
            changed_keys_df.alias("updates"),f"{expire_merge_cond} AND target.is_current = true"
        )
        .whenMatchedUpdate(set={
            "effective_end": F.lit(today),
            "is_current":F.lit(False)
        })
        .execute()
    )

    # step5: Insert new rows (changed + new keys)
    rows_to_insert = (
        source_df
        .join(keys_to_insert,key_cols,"inner")
    )

    insert_count = rows_to_insert.count()

    if insert_count > 0:
        (
            rows_to_insert.write
            .format("delta")
            .mode("append")
            .option("mergeSchema","true")
            .saveAsTable(target_table)
        )
    
    if logger:
        changed_count = changed_keys_df.count()
        new_count = new_keys_df.count()
        logger.log(
            target_table.split(".")[-1],
            "PASS",
            insert_count,
            f"SCD2: {new_count} new keys inserted, " f"{changed_count} changed keys expired + reinserted"
        )                

