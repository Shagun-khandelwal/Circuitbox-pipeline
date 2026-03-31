# Databricks notebook source
# MAGIC %md
# MAGIC ### Circuitbox - master workflow orchestrator
# MAGIC - Runs all layers in sequence: Bronze -> Silver -> SCD -> Gold
# MAGIC - Usage: Run this single notebook to refresh everything
# MAGIC - Schedule via: Databricks Jobs ( Workflows tab)

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

def run_layer(path:str,layer:str):
    """ Run a notebook and report result."""
    print(f"\n{'-'*50}")
    print(f" Starting {layer}...")
    print(f"{'-'*50}")
    start = datetime.now()
    try:
        dbutils.notebook.run(path,timeout_seconds=900)
        elapsed = (datetime.now() - start).seconds
        print(f"{layer} completed in {elapsed}s")
        return True
    except Exception as e:
        elapsed = (datetime.now()-start).seconds
        print(f"{layer} Failed after {elapsed}s")
        print(f" Error: {str(e)}")
        return False
    

# COMMAND ----------

print(f"\n{'='*50}")
print(f"Circuitbox pipeline run")
print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*50}")

# COMMAND ----------

layers = [
    ("/Workspace/Users/kshagun02@gmail.com/CircuitBox/pipeline/00_utils","Utilities for all layers"),
    ("/Workspace/Users/kshagun02@gmail.com/CircuitBox/pipeline/01_bronze","Bronze - Raw Ingest"),
    ("/Workspace/Users/kshagun02@gmail.com/CircuitBox/pipeline/02_silver","Silver - Clean + Validate"),
    ("/Workspace/Users/kshagun02@gmail.com/CircuitBox/pipeline/03_scd","SCD - Type1/Type2 Dims"),
    ("/Workspace/Users/kshagun02@gmail.com/CircuitBox/pipeline/04_gold","Gold - KPIs + Summary")
]

# COMMAND ----------

results = {}
for path, name in layers:
    ok = run_layer(path,name)
    results[name]= ok
    if not ok:
        print(f"\n Pipeline stopped at {name}.")
        print(f"Fix the error and re-run from this layer.")
        break


# COMMAND ----------



# COMMAND ----------

# Final summary
print(f"\n{'='*50}")
print(f"Pipeline Summary")
print(f"{'='*50}")
for name,ok in results.items():
    icon = "✅" if ok else "❌"
    print(f" {icon}  {name}")

all_passed = all(results.values())
print(f"\n {'✅ All layers complete' if all_passed else '❌ Pipeline Incomplete'}")
print(f" Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*50}\n")

if not all_passed:
    raise Exception("Pipeline did not complete successfully. Check logs above.")