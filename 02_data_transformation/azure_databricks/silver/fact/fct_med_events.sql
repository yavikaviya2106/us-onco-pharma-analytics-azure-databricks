-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW fct_med_events_vw AS

WITH med_eve AS (
    SELECT * FROM onco_pharma_analytics.silver.stg_medical_events

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY medical_event_id
        ORDER BY source_updated_at DESC
    ) = 1
),

dim_med_eve AS (
    SELECT
        dim_med_eve_sk,
        medical_event_id
    FROM onco_pharma_analytics.silver.dim_med_events
)

SELECT
    md5(CAST(med_eve.medical_event_id AS STRING)) AS fct_med_eve_sk,
    med_eve.medical_event_id                    AS medical_event_id,
    dim_med_eve.dim_med_eve_sk                  AS dim_med_eve_sk,

    med_eve.planned_attendee_count              AS planned_attendee_count,
    med_eve.actual_attendee_count               AS actual_attendee_count,
    med_eve.target_attendee_count               AS target_attendee_count,
    med_eve.registration_count                  AS registration_count,
    med_eve.event_capacity                      AS event_capacity,

    med_eve.event_budget                        AS event_budget,
    med_eve.actual_event_cost                   AS actual_event_cost,
    med_eve.currency_code                       AS currency_code,

    med_eve.source_updated_at                   AS source_updated_at,

    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')               AS dwh_insert_ts,
    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')               AS dwh_update_ts

FROM med_eve

LEFT JOIN dim_med_eve
    ON med_eve.medical_event_id = dim_med_eve.medical_event_id;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.fct_med_events
AS
SELECT * FROM fct_med_events_vw;

-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO onco_pharma_analytics.silver.fct_med_events AS t
USING (
    SELECT *
    FROM fct_med_events_vw
   
    WHERE source_updated_at >= (
        SELECT COALESCE(
                   MAX(source_updated_at) - INTERVAL 1 DAY,
                   TIMESTAMP '1900-01-01'
               )
        FROM onco_pharma_analytics.silver.fct_med_events
    )
) AS s
ON t.medical_event_id = s.medical_event_id


WHEN MATCHED THEN UPDATE SET
    t.fct_med_eve_sk          = s.fct_med_eve_sk,
    t.dim_med_eve_sk          = s.dim_med_eve_sk,
    t.planned_attendee_count  = s.planned_attendee_count,
    t.actual_attendee_count   = s.actual_attendee_count,
    t.target_attendee_count   = s.target_attendee_count,
    t.registration_count      = s.registration_count,
    t.event_capacity          = s.event_capacity,
    t.event_budget            = s.event_budget,
    t.actual_event_cost       = s.actual_event_cost,
    t.currency_code           = s.currency_code,
    t.source_updated_at       = s.source_updated_at,
    t.dwh_update_ts           = s.dwh_update_ts

WHEN NOT MATCHED THEN INSERT *;