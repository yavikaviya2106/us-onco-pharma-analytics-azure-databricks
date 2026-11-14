-- Databricks notebook source
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.dim_sku_details (
    dim_sku_sk          STRING    NOT NULL,
    sku_id              STRING    NOT NULL,

    sku_code            STRING,
    description         STRING,

    ndc_code            STRING,

    warehouse_uom       STRING,
    strength            DECIMAL(10,2),
    strength_unit       STRING,
    pack_type           STRING,
    units_per_pack      DECIMAL(12,2),

    dosage_form         STRING,

    approval_status     STRING,
    approved_date       DATE,

    inventory_status    STRING,
    status              STRING,

    source_updated_at   TIMESTAMP,
    source_system       STRING,

    _row_hash           STRING,
    _dim_created_ts     TIMESTAMP,
    _dim_updated_ts     TIMESTAMP
)
USING DELTA
CLUSTER BY (sku_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed'  = 'true',
    'delta.enableDeletionVectors' = 'true'
);

-- COMMAND ----------

MERGE INTO onco_pharma_analytics.silver.dim_sku_details AS tgt
USING (
    WITH s AS (
        SELECT
            md5(cast(sku_id AS STRING)) AS dim_sku_sk,
            sku_id,

            sku_code,
            description,

            ndc_code,

            warehouse_uom,
            strength,
            strength_unit,
            pack_type,
            units_per_pack,

            dosage_form,

            approval_status,
            approved_date,

            inventory_status,
            status,

            source_updated_at,
            source_system,

            sha2(to_json(struct(
                    sku_code, description,
                    ndc_code,
                    warehouse_uom, strength, strength_unit, pack_type, units_per_pack,
                    dosage_form,
                    approval_status, approved_date,
                    inventory_status, status,
                    source_updated_at, source_system
                )), 256) AS _row_hash
        FROM onco_pharma_analytics.silver.stg_sku_details
    ),

    t AS (
        SELECT sku_id, _row_hash
        FROM onco_pharma_analytics.silver.dim_sku_details
    )

    SELECT
        coalesce(s.sku_id, t.sku_id)                             AS merge_key,
        CASE WHEN s.sku_id IS NULL THEN 'D' ELSE 'U' END         AS merge_action,
        s.*
    FROM s
    FULL OUTER JOIN t
      ON s.sku_id = t.sku_id
    WHERE t.sku_id IS NULL
       OR s.sku_id IS NULL
       OR s._row_hash IS DISTINCT FROM t._row_hash
) AS src

ON tgt.sku_id = src.merge_key

WHEN MATCHED AND src.merge_action = 'D' THEN DELETE

WHEN MATCHED AND src.merge_action = 'U'
             AND tgt._row_hash IS DISTINCT FROM src._row_hash
    THEN UPDATE SET
    tgt.sku_code          = src.sku_code,
    tgt.description       = src.description,
    tgt.ndc_code          = src.ndc_code,
    tgt.warehouse_uom     = src.warehouse_uom,
    tgt.strength          = src.strength,
    tgt.strength_unit     = src.strength_unit,
    tgt.pack_type         = src.pack_type,
    tgt.units_per_pack    = src.units_per_pack,
    tgt.dosage_form       = src.dosage_form,
    tgt.approval_status   = src.approval_status,
    tgt.approved_date     = src.approved_date,
    tgt.inventory_status  = src.inventory_status,
    tgt.status            = src.status,
    tgt.source_updated_at = src.source_updated_at,
    tgt.source_system     = src.source_system,
    tgt._row_hash         = src._row_hash,
    tgt._dim_updated_ts   = current_timestamp()

WHEN NOT MATCHED AND src.merge_action = 'U'
    THEN INSERT (
        dim_sku_sk, sku_id,
        sku_code, description,
        ndc_code,
        warehouse_uom, strength, strength_unit, pack_type, units_per_pack,
        dosage_form,
        approval_status, approved_date,
        inventory_status, status,
        source_updated_at, source_system,
        _row_hash, _dim_created_ts, _dim_updated_ts
    )
    VALUES (
        src.dim_sku_sk, src.sku_id,
        src.sku_code, src.description,
        src.ndc_code,
        src.warehouse_uom, src.strength, src.strength_unit, src.pack_type, src.units_per_pack,
        src.dosage_form,
        src.approval_status, src.approved_date,
        src.inventory_status, src.status,
        src.source_updated_at, src.source_system,
        src._row_hash, current_timestamp(), current_timestamp()
    )
;