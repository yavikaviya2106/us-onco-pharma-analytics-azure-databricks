-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW fct_hcp_tgt_list_vw AS

WITH target AS (
    SELECT * FROM onco_pharma_analytics.silver.stg_hcp_target_list

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY target_list_id, hcp_npi, effective_from
        ORDER BY source_updated_at DESC
    ) = 1
),

hcp AS (
    SELECT
        dim_hcp_sk,
        hcp_npi,
        valid_from,
        valid_to
    FROM onco_pharma_analytics.silver.dim_hcp_master
)

SELECT
    md5(CONCAT(
        CAST(target.target_list_id AS STRING),
        '-',
        CAST(target.hcp_npi AS STRING),
        '-',
        CAST(target.effective_from AS STRING)
    ))                           AS fct_hcp_target_sk,

    target.target_list_id           AS target_list_id,
    target.target_list_name         AS target_list_name,

    target.hcp_id                   AS veeva_hcp_id,
    target.hcp_npi                  AS hcp_npi,
    hcp.dim_hcp_sk                  AS dim_hcp_sk,

    target.target_flag              AS target_flag,
    target.target_priority          AS target_priority,
    target.target_segment           AS target_segment,
    target.target_rank              AS target_rank,
    target.target_reason            AS target_reason,
    target.target_score             AS target_score,

    target.target_status            AS target_status,
    target.effective_from           AS effective_from,
    target.effective_to             AS effective_to,

    target.source_updated_at        AS source_updated_at,

    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')   AS dwh_insert_ts,
    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')   AS dwh_update_ts

FROM target

LEFT JOIN hcp
    ON target.hcp_npi = hcp.hcp_npi
    AND target.effective_from >= hcp.valid_from
    AND (hcp.valid_to IS NULL
        OR target.effective_from < hcp.valid_to);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.fct_hcp_tgt_list
AS
SELECT * FROM fct_hcp_tgt_list_vw;

-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO onco_pharma_analytics.silver.fct_hcp_tgt_list AS t
USING (
    SELECT *
    FROM fct_hcp_tgt_list_vw
   
    WHERE source_updated_at >= (
        SELECT COALESCE(
                   MAX(source_updated_at) - INTERVAL 1 DAY,
                   TIMESTAMP '1900-01-01'
               )
        FROM onco_pharma_analytics.silver.fct_hcp_tgt_list
    )
) AS s
ON t.target_list_id = s.target_list_id
    AND t.hcp_npi = s.hcp_npi
    AND t.effective_from = s.effective_from


WHEN MATCHED THEN UPDATE SET
    t.fct_hcp_target_sk  = s.fct_hcp_target_sk,
    t.target_list_name   = s.target_list_name,
    t.veeva_hcp_id       = s.veeva_hcp_id,
    t.dim_hcp_sk         = s.dim_hcp_sk,
    t.target_flag        = s.target_flag,
    t.target_priority    = s.target_priority,
    t.target_segment     = s.target_segment,
    t.target_rank        = s.target_rank,
    t.target_reason      = s.target_reason,
    t.target_score       = s.target_score,
    t.target_status      = s.target_status,
    t.effective_to       = s.effective_to,
    t.source_updated_at  = s.source_updated_at,
    t.dwh_update_ts      = s.dwh_update_ts

WHEN NOT MATCHED THEN INSERT *;