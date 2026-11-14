# Databricks notebook source
#----------------------- Configuring the path -------------------------------------------------

# Unity Catalog & Bronze schema.
CATALOG = "onco_pharma_analytics"
BRONZE_SCHEMA = f"{CATALOG}.bronze"

# ADLS Gen2 landing container containing source data.
LANDING_PATH = (
    "abfss://landing@adls01analytics.dfs.core.windows.net"
)

# A separate, persistent location for checkpoints and schemas.
METADATA_PATH = (
    "abfss://bronze-metadata@adls01analytics.dfs.core.windows.net/"
    "autoloader"
)

AUDIT_TABLE = f"{BRONZE_SCHEMA}.ingestion_audit"

# Source/table pairs.
SOURCE_TABLES = [
    ("hub", "hub_transactions"),
    ("internal_mdm", "field_force_details"),
    ("internal_mdm", "field_force_terr_assignment"),
    ("internal_mdm", "hcp_call_plan"),
    ("internal_mdm", "hcp_target_list"),
    ("iqvia","iqvia_ddd"),
    ("iqvia","iqvia_xponent"),
    ("iqvia_onekey","address_master"),
    ("iqvia_onekey","hco_onekey"),
    ("iqvia_onekey","hcp_hco_affiliation"),
    ("iqvia_onekey","hcp_onekey"),
    ("product_mdm","brand_details"),
    ("product_mdm","disease_details"),
    ("product_mdm","drug_indication_details"),
    ("product_mdm","generic_details"),
    ("product_mdm","market_details"),
    ("product_mdm","product_details"),
    ("product_mdm","sku_details"),
    ("sp","sp_transactions"),
    ("veeva_align","terr_algn"),
    ("veeva_align","zip_to_terr"),
    ("veeva_crm","call_activity"),
    ("veeva_crm","call_detail"),
    ("veeva_crm","email_activity"),
    ("veeva_crm","key_msg_master"),
    ("veeva_crm","med_eve_attendees"),
    ("veeva_crm","med_eve_master"),
    ("veeva_crm","samples_disbursed"),
]

# COMMAND ----------

#---------------------------- Creating audit table for ingestion audits ----------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
    source_name STRING,
    table_name STRING,
    source_file_path STRING,
    batch_id BIGINT,
    records_processed BIGINT,
    ingestion_timestamp TIMESTAMP,
    status STRING,
    error_message STRING
)
USING DELTA
""")

# COMMAND ----------

#---------------------- Autoloader Ingestion Function --------------------------------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, TimestampType
)

def ingest_to_bronze(source_name, table_name):

    source_path = f"{LANDING_PATH}/{source_name}/{table_name}"

    target_table = f"{BRONZE_SCHEMA}.raw_{table_name}"

    table_metadata_path = (
        f"{METADATA_PATH}/schemas/{source_name}/{table_name}"
    )

    checkpoint_path = (
        f"{METADATA_PATH}/checkpoints/{source_name}/{table_name}"
    )

    stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", table_metadata_path)
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaEvolutionMode","addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(source_path)
        .withColumn(
            "_source_file_path",
            F.col("_metadata.file_path")
        )
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn("_source_name", F.lit(source_name))
        .withColumn("_source_table", F.lit(table_name))
    )

    def write_batch(batch_df, batch_id):

        if batch_df.isEmpty():
            return

        record_count = batch_df.count()

        # Append source records to Bronze.
        (
            batch_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .option(
                "txnAppId",
                f"autoloader_{source_name}_{table_name}"
            )
            .option("txnVersion", batch_id)
            .saveAsTable(target_table)
        )

        # Write a separate audit record for the batch.
        audit_df = spark.createDataFrame(
            [(
                source_name,
                table_name,
                None,       # File-level details are not aggregated here.
                int(batch_id),
                int(record_count),
                None,
                "SUCCESS",
                None
            )],
            schema=StructType([
                StructField("source_name", StringType()),
                StructField("table_name", StringType()),
                StructField("source_file_path", StringType()),
                StructField("batch_id", LongType()),
                StructField("records_processed", LongType()),
                StructField("ingestion_timestamp", TimestampType()),
                StructField("status", StringType()),
                StructField("error_message", StringType())
            ])
        ).withColumn(
            "ingestion_timestamp", F.current_timestamp()
        )

        (
            audit_df.write
            .format("delta")
            .mode("append")
            .option(
                "txnAppId",
                f"autoloader_audit_{source_name}_{table_name}"
            )
            .option("txnVersion", batch_id)
            .saveAsTable(AUDIT_TABLE)
        )

    query = (
        stream_df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )

    return query

# COMMAND ----------


# -----------Execution of all 28 Bronze ingestions sequentially------------------------

for source_name, table_name in SOURCE_TABLES:

    print(f"STARTED: {source_name}/{table_name}")

    query = ingest_to_bronze(
        source_name,
        table_name
    )

    query.awaitTermination()

    print(f"SUCCESS: {source_name}/{table_name}")

print("All Bronze tables ingested successfully.")
