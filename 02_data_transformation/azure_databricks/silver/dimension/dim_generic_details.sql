-- Databricks notebook source
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.dim_generic_details (
    dim_generic_sk      STRING    NOT NULL,
    generic_id          STRING    NOT NULL,
    generic_name        STRING,

    therapeutic_area    STRING,
    drug_class          STRING,
    mechanism_action    STRING,
    targets             STRING,

    source_updated_at   TIMESTAMP,
    source_system       STRING,

    _row_hash           STRING,
    _dim_created_ts     TIMESTAMP,
    _dim_updated_ts     TIMESTAMP
)
USING DELTA
CLUSTER BY (generic_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed'  = 'true',
    'delta.enableDeletionVectors' = 'true'
);

-- COMMAND ----------

MERGE INTO onco_pharma_analytics.silver.dim_generic_details AS tgt
USING (
    WITH s AS (
        SELECT
            md5(cast(generic_id AS STRING)) AS dim_generic_sk,
            generic_id,
            generic_name,

            therapeutic_area,
            drug_class,
            mechanism_action,
            targets,

            source_updated_at,
            source_system,

            sha2(to_json(struct(
                    generic_name,
                    therapeutic_area, drug_class, mechanism_action, targets,
                    source_updated_at, source_system
                )), 256) AS _row_hash
        FROM onco_pharma_analytics.silver.stg_generic_details
    ),

    t AS (
        SELECT generic_id, _row_hash
        FROM onco_pharma_analytics.silver.dim_generic_details
    )

    SELECT
        coalesce(s.generic_id, t.generic_id)                     AS merge_key,
        CASE WHEN s.generic_id IS NULL THEN 'D' ELSE 'U' END     AS merge_action,
        s.*
    FROM s
    FULL OUTER JOIN t
      ON s.generic_id = t.generic_id
    WHERE t.generic_id IS NULL
       OR s.generic_id IS NULL
       OR s._row_hash IS DISTINCT FROM t._row_hash
) AS src

ON tgt.generic_id = src.merge_key

WHEN MATCHED AND src.merge_action = 'D' THEN DELETE

WHEN MATCHED AND src.merge_action = 'U'
             AND tgt._row_hash IS DISTINCT FROM src._row_hash
    THEN UPDATE SET
    tgt.generic_name      = src.generic_name,
    tgt.therapeutic_area  = src.therapeutic_area,
    tgt.drug_class        = src.drug_class,
    tgt.mechanism_action  = src.mechanism_action,
    tgt.targets           = src.targets,
    tgt.source_updated_at = src.source_updated_at,
    tgt.source_system     = src.source_system,
    tgt._row_hash         = src._row_hash,
    tgt._dim_updated_ts   = current_timestamp()

WHEN NOT MATCHED AND src.merge_action = 'U'
    THEN INSERT (
        dim_generic_sk, generic_id, generic_name,
        therapeutic_area, drug_class, mechanism_action, targets,
        source_updated_at, source_system,
        _row_hash, _dim_created_ts, _dim_updated_ts
    )
    VALUES (
        src.dim_generic_sk, src.generic_id, src.generic_name,
        src.therapeutic_area, src.drug_class, src.mechanism_action, src.targets,
        src.source_updated_at, src.source_system,
        src._row_hash, current_timestamp(), current_timestamp()
    )
;