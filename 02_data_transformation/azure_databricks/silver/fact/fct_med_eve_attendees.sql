-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW fct_med_eve_attendees_vw AS

WITH eve_att AS (
    SELECT *
    FROM onco_pharma_analytics.silver.stg_med_eve_attendees
    WHERE event_attendee_id IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_attendee_id
        ORDER BY source_updated_at DESC
    ) = 1
),

dim_med_eve AS (
    SELECT
        dim_med_eve_sk,
        medical_event_id,
        event_start_date
    FROM onco_pharma_analytics.silver.dim_med_events
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
    md5(CAST(eve_att.event_attendee_id AS STRING)) AS fct_eve_att_sk,

    eve_att.event_attendee_id                   AS event_attendee_id,

    eve_att.medical_event_id                    AS medical_event_id,
    dim_med_eve.dim_med_eve_sk                  AS dim_med_eve_sk,

    eve_att.hcp_npi                             AS hcp_npi,
    hcp.dim_hcp_sk                              AS dim_hcp_sk,

    eve_att.attendee_role                       AS attendee_role,

    eve_att.invitation_date                     AS invitation_date,
    eve_att.invitation_status                   AS invitation_status,
    eve_att.registration_date                   AS registration_date,
    eve_att.registration_status                 AS registration_status,
    eve_att.attendance_status                   AS attendance_status,
    eve_att.attendance_mode                     AS attendance_mode,

    eve_att.check_in_time                       AS check_in_time,
    eve_att.check_out_time                      AS check_out_time,

    eve_att.no_show_flag                        AS no_show_flag,
    eve_att.cancellation_date                   AS cancellation_date,
    eve_att.eligibility_reason                  AS eligibility_reason,

    eve_att.source_updated_at                   AS source_updated_at,

    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')               AS dwh_insert_ts,
    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')               AS dwh_update_ts

FROM eve_att

LEFT JOIN dim_med_eve
    ON eve_att.medical_event_id = dim_med_eve.medical_event_id

LEFT JOIN hcp
    ON eve_att.hcp_npi = hcp.hcp_npi
    AND dim_med_eve.event_start_date >= hcp.valid_from
    AND (hcp.valid_to IS NULL
        OR dim_med_eve.event_start_date < hcp.valid_to)


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.fct_med_eve_attendees
AS
SELECT * FROM fct_med_eve_attendees_vw;

-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO onco_pharma_analytics.silver.fct_med_eve_attendees AS t
USING (
    SELECT *
    FROM fct_med_eve_attendees_vw
    WHERE source_updated_at >= (
        SELECT COALESCE(
                   MAX(source_updated_at) - INTERVAL 1 DAY,
                   TIMESTAMP '1900-01-01'
               )
        FROM onco_pharma_analytics.silver.fct_med_eve_attendees
    )
) AS s
ON t.event_attendee_id = s.event_attendee_id

WHEN MATCHED THEN UPDATE SET
    t.fct_eve_att_sk       = s.fct_eve_att_sk,
    t.medical_event_id     = s.medical_event_id,
    t.dim_med_eve_sk       = s.dim_med_eve_sk,
    t.hcp_npi              = s.hcp_npi,
    t.dim_hcp_sk           = s.dim_hcp_sk,
    t.attendee_role        = s.attendee_role,
    t.invitation_date      = s.invitation_date,
    t.invitation_status    = s.invitation_status,
    t.registration_date    = s.registration_date,
    t.registration_status  = s.registration_status,
    t.attendance_status    = s.attendance_status,
    t.attendance_mode      = s.attendance_mode,
    t.check_in_time        = s.check_in_time,
    t.check_out_time       = s.check_out_time,
    t.no_show_flag         = s.no_show_flag,
    t.cancellation_date    = s.cancellation_date,
    t.eligibility_reason   = s.eligibility_reason,
    t.source_updated_at    = s.source_updated_at,
    t.dwh_update_ts        = s.dwh_update_ts

WHEN NOT MATCHED THEN INSERT *;