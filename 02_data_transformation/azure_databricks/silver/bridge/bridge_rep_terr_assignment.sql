-- Databricks notebook source
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.bridge_rep_terr_assignment (
    bridge_rep_terr_sk      STRING    NOT NULL,
    rep_id                  STRING,
    territory_id            STRING,
    assignment_slot         STRING,
    
    assignment_start_date   DATE,
    assignment_end_date     DATE,

    is_active               BOOLEAN,

    source_updated_at       TIMESTAMP,
    source_system           STRING,

    _row_hash               STRING,
    _dim_created_ts         TIMESTAMP,
    _dim_updated_ts         TIMESTAMP
)
USING DELTA
CLUSTER BY (rep_id, assignment_slot, assignment_start_date)
TBLPROPERTIES (
    'delta.enableChangeDataFeed'  = 'true',
    'delta.enableDeletionVectors' = 'true'
);

-- COMMAND ----------

MERGE INTO onco_pharma_analytics.silver.bridge_rep_terr_assignment AS tgt
USING (
    WITH s AS (
        SELECT
            md5(concat(
                coalesce(cast(rep_id                AS STRING), ''), '-',
                coalesce(cast(assignment_slot       AS STRING), ''), '-',
                coalesce(cast(assignment_start_date AS STRING), '')
            )) AS bridge_rep_terr_sk,
            rep_id,
            assignment_slot,
            assignment_start_date,

            territory_id,

            assignment_end_date,

            is_active,

            source_updated_at,
            source_system,

            sha2(to_json(struct(
                    territory_id,
                    assignment_end_date,
                    is_active,
                    source_updated_at, source_system
                )), 256) AS _row_hash
        FROM onco_pharma_analytics.silver.stg_field_force_terr_assignment
    ),

    t AS (
        SELECT bridge_rep_terr_sk, _row_hash
        FROM onco_pharma_analytics.silver.bridge_rep_terr_assignment
    )

    SELECT
        coalesce(s.bridge_rep_terr_sk, t.bridge_rep_terr_sk)             AS merge_key,
        CASE WHEN s.bridge_rep_terr_sk IS NULL THEN 'D' ELSE 'U' END     AS merge_action,
        s.*
    FROM s
    FULL OUTER JOIN t
      ON s.bridge_rep_terr_sk = t.bridge_rep_terr_sk
    WHERE t.bridge_rep_terr_sk IS NULL
       OR s.bridge_rep_terr_sk IS NULL
       OR s._row_hash IS DISTINCT FROM t._row_hash
) AS src

ON tgt.bridge_rep_terr_sk = src.merge_key

WHEN MATCHED AND src.merge_action = 'D' THEN DELETE

WHEN MATCHED AND src.merge_action = 'U'
             AND tgt._row_hash IS DISTINCT FROM src._row_hash
    THEN UPDATE SET
    tgt.territory_id        = src.territory_id,
    tgt.assignment_end_date = src.assignment_end_date,
    tgt.is_active           = src.is_active,
    tgt.source_updated_at   = src.source_updated_at,
    tgt.source_system       = src.source_system,
    tgt._row_hash           = src._row_hash,
    tgt._dim_updated_ts     = current_timestamp()

WHEN NOT MATCHED AND src.merge_action = 'U'
    THEN INSERT (
        bridge_rep_terr_sk,
        rep_id, assignment_slot, assignment_start_date,
        territory_id,
        assignment_end_date,
        is_active,
        source_updated_at, source_system,
        _row_hash, _dim_created_ts, _dim_updated_ts
    )
    VALUES (
        src.bridge_rep_terr_sk,
        src.rep_id, src.assignment_slot, src.assignment_start_date,
        src.territory_id,
        src.assignment_end_date,
        src.is_active,
        src.source_updated_at, src.source_system,
        src._row_hash, current_timestamp(), current_timestamp()
    )
;