-- Databricks notebook source
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.dim_market_details (
    dim_market_sk       STRING    NOT NULL,
    market_id           STRING    NOT NULL,
    market_name         STRING,
    therapeutic_area    STRING,

    dim_generic_sk      STRING,
    generic_id          STRING,

    dim_brand_sk        STRING,
    brand_id            STRING,

    source_updated_at   TIMESTAMP,
    source_system       STRING,

    _row_hash           STRING,
    _dim_created_ts     TIMESTAMP,
    _dim_updated_ts     TIMESTAMP
)
USING DELTA
CLUSTER BY (market_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed'  = 'true',
    'delta.enableDeletionVectors' = 'true'
);

-- COMMAND ----------

MERGE INTO onco_pharma_analytics.silver.dim_market_details AS tgt
USING (
    WITH brand AS (
        SELECT dim_brand_sk, brand_id
        FROM onco_pharma_analytics.silver.dim_brand_details
    ),

    generic AS (
        SELECT dim_generic_sk, generic_id
        FROM onco_pharma_analytics.silver.dim_generic_details
    ),

    joined AS (
        SELECT
            md5(cast(market.market_id AS STRING)) AS dim_market_sk,
            market.market_id                      AS market_id,
            market.market_name                    AS market_name,
            market.therapeutic_area               AS therapeutic_area,

            generic.dim_generic_sk                AS dim_generic_sk,
            market.generic_id                     AS generic_id,

            brand.dim_brand_sk                    AS dim_brand_sk,
            market.brand_id                       AS brand_id,

            market.source_updated_at              AS source_updated_at,
            market.source_system                  AS source_system
        FROM onco_pharma_analytics.silver.stg_market_details AS market
        LEFT JOIN brand   ON market.brand_id   = brand.brand_id
        LEFT JOIN generic ON market.generic_id = generic.generic_id
    ),

    s AS (
        SELECT
            *,
            sha2(to_json(struct(
                    market_name, therapeutic_area,
                    dim_generic_sk, generic_id,
                    dim_brand_sk, brand_id,
                    source_updated_at, source_system
                )), 256) AS _row_hash
        FROM joined
    ),

    t AS (
        SELECT market_id, _row_hash
        FROM onco_pharma_analytics.silver.dim_market_details
    )

    SELECT
        coalesce(s.market_id, t.market_id)                       AS merge_key,
        CASE WHEN s.market_id IS NULL THEN 'D' ELSE 'U' END      AS merge_action,
        s.*
    FROM s
    FULL OUTER JOIN t
      ON s.market_id = t.market_id
    WHERE t.market_id IS NULL
       OR s.market_id IS NULL
       OR s._row_hash IS DISTINCT FROM t._row_hash
) AS src

ON tgt.market_id = src.merge_key

WHEN MATCHED AND src.merge_action = 'D' THEN DELETE

WHEN MATCHED AND src.merge_action = 'U'
             AND tgt._row_hash IS DISTINCT FROM src._row_hash
    THEN UPDATE SET
    tgt.market_name       = src.market_name,
    tgt.therapeutic_area  = src.therapeutic_area,
    tgt.dim_generic_sk    = src.dim_generic_sk,
    tgt.generic_id        = src.generic_id,
    tgt.dim_brand_sk      = src.dim_brand_sk,
    tgt.brand_id          = src.brand_id,
    tgt.source_updated_at = src.source_updated_at,
    tgt.source_system     = src.source_system,
    tgt._row_hash         = src._row_hash,
    tgt._dim_updated_ts   = current_timestamp()

WHEN NOT MATCHED AND src.merge_action = 'U'
    THEN INSERT (
        dim_market_sk, market_id, market_name, therapeutic_area,
        dim_generic_sk, generic_id,
        dim_brand_sk, brand_id,
        source_updated_at, source_system,
        _row_hash, _dim_created_ts, _dim_updated_ts
    )
    VALUES (
        src.dim_market_sk, src.market_id, src.market_name, src.therapeutic_area,
        src.dim_generic_sk, src.generic_id,
        src.dim_brand_sk, src.brand_id,
        src.source_updated_at, src.source_system,
        src._row_hash, current_timestamp(), current_timestamp()
    )
;