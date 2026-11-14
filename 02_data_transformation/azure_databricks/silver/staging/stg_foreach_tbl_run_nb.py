# Databricks notebook source
table_name    = dbutils.widgets.get("table_name").strip()
catalog       = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()

# COMMAND ----------

import json

import importlib
import stg_framework as stg          
importlib.reload(stg)                # pick up edits without restarting the session

# COMMAND ----------

import os

config_path = os.path.abspath(os.path.join(os.getcwd(), "../../config/stg_config.json"))
config = stg.load_config(config_path)

# COMMAND ----------

table_cfg = config[table_name]
bronze_df = spark.read.table(f"{catalog}.{bronze_schema}.{table_cfg['source_table']}")

# COMMAND ----------

#preview_df = stg.build_staging_df(bronze_df, table_cfg, config.get("_settings", {}))
#display(preview_df.limit(20))

# COMMAND ----------

result = stg.run_staging_table(spark, config, table_name, catalog, bronze_schema, silver_schema)
print(json.dumps(result, indent=2, default=str))

# COMMAND ----------

dbutils.notebook.exit(json.dumps(result, default=str))