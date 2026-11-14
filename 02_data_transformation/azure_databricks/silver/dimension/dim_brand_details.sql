-- Databricks notebook source
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.dim_brand_details (
    dim_brand_sk        STRING    NOT NULL,
    brand_id            STRING    NOT NULL,
    brand_name          STRING,

    therapeutic_area    STRING,

    company_id          STRING,
    company_name        STRING,

    launch_date         DATE,

    source_updated_at   TIMESTAMP,
    source_system       STRING,

    _row_hash           STRING,
    _dim_created_ts     TIMESTAMP,
    _dim_updated_ts     TIMESTAMP
)
USING DELTA
CLUSTER BY (brand_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed'  = 'true',
    'delta.enableDeletionVectors' = 'true'
);

-- COMMAND ----------

MERGE INTO onco_pharma_analytics.silver.dim_brand_details AS tgt
USING (
    WITH s AS (
        SELECT
            md5(cast(brand_id AS STRING)) AS dim_brand_sk,
            brand_id,
            brand_name,

            therapeutic_area,

            company_id,
            company_name,

            launch_date,

            source_updated_at,
            source_system,

            sha2(to_json(struct(
                    brand_name,
                    therapeutic_area,
                    company_id, company_name,
                    launch_date,
                    source_updated_at, source_system
                )), 256) AS _row_hash
        FROM onco_pharma_analytics.silver.stg_brand_details
    ),

    t AS (
        SELECT brand_id, _row_hash
        FROM onco_pharma_analytics.silver.dim_brand_details
    )

    SELECT
        coalesce(s.brand_id, t.brand_id)                         AS merge_key,
        CASE WHEN s.brand_id IS NULL THEN 'D' ELSE 'U' END       AS merge_action,
        s.*
    FROM s
    FULL OUTER JOIN t
      ON s.brand_id = t.brand_id
    WHERE t.brand_id IS NULL
       OR s.brand_id IS NULL
       OR s._row_hash IS DISTINCT FROM t._row_hash
) AS src

ON tgt.brand_id = src.merge_key

WHEN MATCHED AND src.merge_action = 'D' THEN DELETE

WHEN MATCHED AND src.merge_action = 'U'
             AND tgt._row_hash IS DISTINCT FROM src._row_hash
    THEN UPDATE SET
    tgt.brand_name        = src.brand_name,
    tgt.therapeutic_area  = src.therapeutic_area,
    tgt.company_id        = src.company_id,
    tgt.company_name      = src.company_name,
    tgt.launch_date       = src.launch_date,
    tgt.source_updated_at = src.source_updated_at,
    tgt.source_system     = src.source_system,
    tgt._row_hash         = src._row_hash,
    tgt._dim_updated_ts   = current_timestamp()

WHEN NOT MATCHED AND src.merge_action = 'U'
    THEN INSERT (
        dim_brand_sk, brand_id, brand_name,
        therapeutic_area,
        company_id, company_name,
        launch_date,
        source_updated_at, source_system,
        _row_hash, _dim_created_ts, _dim_updated_ts
    )
    VALUES (
        src.dim_brand_sk, src.brand_id, src.brand_name,
        src.therapeutic_area,
        src.company_id, src.company_name,
        src.launch_date,
        src.source_updated_at, src.source_system,
        src._row_hash, current_timestamp(), current_timestamp()
    )
;