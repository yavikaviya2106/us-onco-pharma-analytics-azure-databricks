# Importing necessary libraries
import json

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, StringType, DecimalType, TimestampNTZType, TimestampType,
)

# Variables defintions
FLAG_TRUE_VALUES = ["Y", "YES", "TRUE", "T", "1"]
FLAG_FALSE_VALUES = ["N", "NO", "FALSE", "F", "0"]


def clean_string(c, dtype=None):
    return F.nullif(F.trim(c.cast("string")), F.lit(""))


def rule_trim(c, dtype, spec):
    return clean_string(c)


def rule_upper(c, dtype, spec):
    return F.upper(clean_string(c))


def rule_lower(c, dtype, spec):
    return F.lower(clean_string(c))


def rule_lpad_zero(c, dtype, spec):
    return F.lpad(clean_string(c, dtype), int(spec["length"]), "0")


def rule_flag(c, dtype, spec):
    if isinstance(dtype, BooleanType):
        return c
    u = F.upper(clean_string(c, dtype))
    return (F.when(u.isin(FLAG_TRUE_VALUES), F.lit(True))
             .when(u.isin(FLAG_FALSE_VALUES), F.lit(False))
             .otherwise(F.lit(None).cast("boolean")))


def rule_npi(c, dtype, spec):
    return clean_string(c, dtype)


def rule_ndc(c, dtype, spec):
    s = c.cast("string")
    dash = F.lit("-")

    from_hyphenated = F.concat(
        F.lpad(F.split_part(s, dash, F.lit(1)), 5, "0"),   
        F.lpad(F.split_part(s, dash, F.lit(2)), 4, "0"),   
        F.lpad(F.split_part(s, dash, F.lit(3)), 2, "0"),   )

    return (F.when(s.isNull() | (F.trim(s) == ""), F.lit(None).cast("string"))  
             .when(s.contains("-"), from_hyphenated)                            
             .otherwise(F.lpad(F.trim(s), 11, "0")))                            


def rule_decimal(c, dtype, spec):
    target = DecimalType(int(spec["precision"]), int(spec["scale"]))
    if isinstance(dtype, StringType):
        c = F.regexp_replace(F.trim(c), ",", "")   # " 1,234.50 " -> "1234.50"
    return c.try_cast(target)
    

def rule_date(c, dtype, spec):
    if isinstance(dtype, DateType):
        return c
    if isinstance(dtype, (TimestampType, TimestampNTZType)):
        return F.to_date(c)
    return F.to_date(F.try_to_timestamp(clean_string(c, dtype)))


def rule_timestamp(c, dtype, spec):
    if isinstance(dtype, (TimestampType, TimestampNTZType, DateType)):
        return c.cast("timestamp_ntz")
    return F.try_to_timestamp(clean_string(c, dtype)).cast("timestamp_ntz")


def rule_time(c, dtype, spec):
    if isinstance(dtype, (TimestampType, TimestampNTZType)):
        return F.date_format(c, "HH:mm:ss")
    return F.date_format(F.try_to_timestamp(clean_string(c, dtype)), "HH:mm:ss")


RULES = {
    "trim": rule_trim,
    "upper": rule_upper,
    "lower": rule_lower,
    "lpad_zero": rule_lpad_zero,
    "flag": rule_flag,
    "npi": rule_npi,
    "ndc": rule_ndc,
    "decimal": rule_decimal,
    "date": rule_date,
    "timestamp": rule_timestamp,
    "time": rule_time,
}




def load_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)

def list_tables(config: dict) -> list:
    return [k for k in config if not k.startswith("_")]


def build_staging_df(bronze_df: DataFrame, table_cfg: dict) -> DataFrame:
    df = bronze_df
    dtypes = {field.name: field.dataType for field in df.schema.fields}

    missing = sorted({c["source"] for c in table_cfg["columns"]} - set(dtypes))
    if missing:
        raise ValueError(f"Columns missing in bronze table {table_cfg['source_table']}: {missing}")

    select_cols = []
    for spec in table_cfg["columns"]:
        src, tgt, rule = spec["source"], spec["target"], spec["rule"]
        if rule not in RULES:
            raise ValueError(f"Unknown rule '{rule}' for column {tgt}")
        select_cols.append(RULES[rule](F.col(src), dtypes[src], spec).alias(tgt))

    select_cols.append(F.current_timestamp().alias("_stg_loaded_at"))
    return df.select(*select_cols)

def prepare_session(spark: SparkSession) -> None:
    """Strict date parsing: a layout that doesn't match gives NULL instead of an
    'inconsistent behavior across versions' error (e.g. '1/15/2024 10:30')."""
    try:
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    except Exception:
        pass


def run_staging_table(spark: SparkSession, config: dict, table_name: str,
                      catalog: str, bronze_schema: str, silver_schema: str) -> dict:
    if table_name not in config:
        raise ValueError(f"'{table_name}' not in config. Known tables: {list_tables(config)}")

    table_cfg = config[table_name]
    source = f"{catalog}.{bronze_schema}.{table_cfg['source_table']}"
    target = f"{catalog}.{silver_schema}.{table_name}"

    stg_df = build_staging_df(spark.read.table(source), table_cfg)

    (stg_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target))

    # Small data-quality summary for the run log
    written = spark.read.table(target)
    checks = [F.count(F.lit(1)).alias("rows")]
    for c in written.columns:
        if c.endswith("_is_valid"):
            checks.append(F.sum(F.when(F.col(c) == False, 1).otherwise(0)).alias(f"invalid_{c[:-9]}"))  # noqa: E712
    summary = written.agg(*checks).first().asDict()
    return {"table": target, "source": source, **summary}
