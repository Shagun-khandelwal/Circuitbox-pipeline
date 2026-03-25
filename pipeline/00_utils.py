# Databricks notebook source
# MAGIC %md
# MAGIC ### CircuitBox - shared Utilities
# MAGIC Note: Import this in every notebook: %run ./00_utils

# COMMAND ----------

from pyspark.sql import functions as f, DataFrame
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

