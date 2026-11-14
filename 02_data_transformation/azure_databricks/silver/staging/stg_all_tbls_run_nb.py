# Databricks notebook source

catalog       = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()
#only_tables   = [t.strip() for t in dbutils.widgets.get("only_tables").split(",") if t.strip()]


# COMMAND ----------


import importlib
import stg_framework as stg
importlib.reload(stg)

# COMMAND ----------

import os

stg.prepare_session(spark)

silver_dir = os.path.dirname(os.path.dirname(stg.__file__))  
config_path = os.path.join(silver_dir, "config", "staging.json")
config = stg.load_config(config_path)

tables = stg.list_tables(config)
print(f"{len(tables)} tables to run")

# COMMAND ----------

results, failures = [], []
for t in tables:
    try:
        r = stg.run_staging_table(spark, config, t, catalog, bronze_schema, silver_schema)
        results.append(r)
        print(f"OK    {t}: {r['rows']} rows")
    except Exception as e:
        failures.append((t, str(e)[:500]))
        print(f"FAIL  {t}: {str(e)[:200]}")

# COMMAND ----------


import json
if results:
    summary_rows = [
        (r["table"], int(r["rows"]),
         json.dumps({k: v for k, v in r.items() if k.startswith("invalid_")}))
        for r in results
    ]
    display(spark.createDataFrame(summary_rows, "table string, rows long, invalid_npi_ndc_counts string"))

# COMMAND ----------

if failures:
    raise RuntimeError(f"{len(failures)} table(s) failed: " + "; ".join(f"{t} -> {e}" for t, e in failures))
