-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW fct_call_activity_vw AS

WITH call AS (
    SELECT * FROM onco_pharma_analytics.silver.stg_call_activity

    -- One row per key (latest version) so the MERGE never sees duplicate keys
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY call_id
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

    md5((CAST(call.call_id AS STRING)))         AS fct_call_act_sk,

    call.call_id                               AS call_id,
    call.call_date                             AS call_date,

    call.hcp_id                                AS veeva_hcp_id,
    call.hcp_npi                               AS hcp_npi,
    hcp.dim_hcp_sk                             AS dim_hcp_sk,

    call.rep_id                                AS rep_id,
    rep.dim_rep_sk                             AS dim_rep_sk,

    call.call_type                             AS call_type,
    call.call_status                           AS call_status,
    call.call_outcome                          AS call_outcome,
    call.call_duration_minutes                 AS call_duration_minutes,
    call.call_purpose                          AS call_purpose,
    call.call_channel                          AS call_channel,

    call.is_planned_call                       AS is_planned_call,
    call.is_target_hcp                         AS is_target_hcp,
    call.call_sequence                         AS call_sequence,
    call.key_message_presented                 AS key_message_presented,
    call.sample_drop_flag                      AS sample_drop_flag,

    call.follow_up_required                    AS follow_up_required,
    call.follow_up_date                        AS follow_up_date,

    call.location_type                         AS location_type,
    call.location_name                         AS location_name,

    call.parent_call_id                        AS parent_call_id,
    call.follow_up_action                      AS follow_up_action,
    call.next_best_action                      AS next_best_action,

    call.source_updated_at                     AS source_updated_at,

    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')              AS dwh_insert_ts,
    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')              AS dwh_update_ts


FROM call

LEFT JOIN hcp
    ON call.hcp_npi = hcp.hcp_npi
    AND call.call_date >= hcp.valid_from
    AND (hcp.valid_to IS NULL
        OR call.call_date < hcp.valid_to)

LEFT JOIN rep
    ON call.rep_id = rep.rep_id
    AND call.call_date >= rep.valid_from
    AND (rep.valid_to IS NULL
        OR call.call_date < rep.valid_to);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.fct_call_activity
AS
SELECT * FROM fct_call_activity_vw;

-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO onco_pharma_analytics.silver.fct_call_activity AS t
USING (
    SELECT *
    FROM fct_call_activity_vw
   
    WHERE source_updated_at >= (
        SELECT COALESCE(
                   MAX(source_updated_at) - INTERVAL 1 DAY,
                   TIMESTAMP '1900-01-01'
               )
        FROM onco_pharma_analytics.silver.fct_call_activity
    )
) AS s
ON t.call_id = s.call_id


WHEN MATCHED THEN UPDATE SET
    t.fct_call_act_sk        = s.fct_call_act_sk,
    t.call_date              = s.call_date,
    t.veeva_hcp_id           = s.veeva_hcp_id,
    t.hcp_npi                = s.hcp_npi,
    t.dim_hcp_sk             = s.dim_hcp_sk,
    t.rep_id                 = s.rep_id,
    t.dim_rep_sk             = s.dim_rep_sk,
    t.call_type              = s.call_type,
    t.call_status            = s.call_status,
    t.call_outcome           = s.call_outcome,
    t.call_duration_minutes  = s.call_duration_minutes,
    t.call_purpose           = s.call_purpose,
    t.call_channel           = s.call_channel,
    t.is_planned_call        = s.is_planned_call,
    t.is_target_hcp          = s.is_target_hcp,
    t.call_sequence          = s.call_sequence,
    t.key_message_presented  = s.key_message_presented,
    t.sample_drop_flag       = s.sample_drop_flag,
    t.follow_up_required     = s.follow_up_required,
    t.follow_up_date         = s.follow_up_date,
    t.location_type          = s.location_type,
    t.location_name          = s.location_name,
    t.parent_call_id         = s.parent_call_id,
    t.follow_up_action       = s.follow_up_action,
    t.next_best_action       = s.next_best_action,
    t.source_updated_at      = s.source_updated_at,
    t.dwh_update_ts          = s.dwh_update_ts

WHEN NOT MATCHED THEN INSERT *;