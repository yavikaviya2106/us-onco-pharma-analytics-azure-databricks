-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW fct_hcp_call_plan_vw AS

WITH call_plan AS (
    SELECT *
    FROM onco_pharma_analytics.silver.stg_hcp_call_plan

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY call_plan_id
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
),

rep AS (
    SELECT
        dim_rep_sk,
        rep_id,
        valid_from,
        valid_to
    FROM onco_pharma_analytics.silver.dim_field_force_master
)

SELECT
    md5(CAST(call_plan.call_plan_id AS STRING)) AS fct_call_plan_sk,

    call_plan.call_plan_id                      AS call_plan_id,

    call_plan.cycle_id                          AS cycle_id,
    call_plan.cycle_name                        AS cycle_name,

    call_plan.rep_id                            AS rep_id,
    rep.dim_rep_sk                              AS dim_rep_sk,

    call_plan.hcp_id                            AS veeva_hcp_id,
    call_plan.hcp_npi                           AS hcp_npi,
    hcp.dim_hcp_sk                              AS dim_hcp_sk,

    call_plan.target_flag                       AS target_flag,
    call_plan.target_priority                   AS target_priority,

    call_plan.min_call_frequency                AS min_call_frequency,
    call_plan.planned_calls                     AS planned_calls,
    call_plan.max_call_frequency                AS max_call_frequency,

    call_plan.call_plan_start_date              AS call_plan_start_date,
    call_plan.call_plan_end_date                AS call_plan_end_date,

    call_plan.plan_created_date                 AS plan_created_date,
    call_plan.plan_approved_by                  AS plan_approved_by,

    call_plan.source_updated_at                 AS source_updated_at,

    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')               AS dwh_insert_ts,
    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')               AS dwh_update_ts

FROM call_plan

LEFT JOIN hcp
    ON call_plan.hcp_npi = hcp.hcp_npi
    AND call_plan.call_plan_start_date >= hcp.valid_from
    AND (hcp.valid_to IS NULL
        OR call_plan.call_plan_start_date < hcp.valid_to)

LEFT JOIN rep
    ON call_plan.rep_id = rep.rep_id
    AND call_plan.call_plan_start_date >= rep.valid_from
    AND (rep.valid_to IS NULL
        OR call_plan.call_plan_start_date < rep.valid_to);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.fct_hcp_call_plan
AS
SELECT * FROM fct_hcp_call_plan_vw;

-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO onco_pharma_analytics.silver.fct_hcp_call_plan AS t
USING (
    SELECT *
    FROM fct_hcp_call_plan_vw
  
    WHERE source_updated_at >= (
        SELECT COALESCE(
                   MAX(source_updated_at) - INTERVAL 1 DAY,
                   TIMESTAMP '1900-01-01'
               )
        FROM onco_pharma_analytics.silver.fct_hcp_call_plan
    )
) AS s
ON t.call_plan_id = s.call_plan_id

WHEN MATCHED THEN UPDATE SET
    t.fct_call_plan_sk      = s.fct_call_plan_sk,
    t.cycle_id              = s.cycle_id,
    t.cycle_name            = s.cycle_name,
    t.rep_id                = s.rep_id,
    t.dim_rep_sk            = s.dim_rep_sk,
    t.veeva_hcp_id          = s.veeva_hcp_id,
    t.hcp_npi               = s.hcp_npi,
    t.dim_hcp_sk            = s.dim_hcp_sk,
    t.target_flag           = s.target_flag,
    t.target_priority       = s.target_priority,
    t.min_call_frequency    = s.min_call_frequency,
    t.planned_calls         = s.planned_calls,
    t.max_call_frequency    = s.max_call_frequency,
    t.call_plan_start_date  = s.call_plan_start_date,
    t.call_plan_end_date    = s.call_plan_end_date,
    t.plan_created_date     = s.plan_created_date,
    t.plan_approved_by      = s.plan_approved_by,
    t.source_updated_at     = s.source_updated_at,
    t.dwh_update_ts         = s.dwh_update_ts

WHEN NOT MATCHED THEN INSERT *;