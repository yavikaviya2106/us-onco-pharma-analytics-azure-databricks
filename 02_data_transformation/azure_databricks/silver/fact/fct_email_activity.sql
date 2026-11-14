-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW fct_email_activity_vw AS

WITH email AS (
    SELECT *
    FROM onco_pharma_analytics.silver.stg_email_activity

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY email_activity_id
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
),

prod AS (
    SELECT
        dim_prod_sk,
        product_id
    FROM onco_pharma_analytics.silver.dim_product_details
),

call AS (
    SELECT
        fct_call_act_sk,
        call_id,
        veeva_hcp_id
    FROM onco_pharma_analytics.silver.fct_call_activity
)

SELECT
    md5(CAST(email.email_activity_id AS STRING)) AS fct_email_act_sk,

    email.email_activity_id                 AS email_activity_id,
    email.email_date                        AS email_date,

    COALESCE(email.hcp_id,call.veeva_hcp_id)     AS veeva_hcp_id,
    email.hcp_npi                           AS hcp_npi,
    hcp.dim_hcp_sk                          AS dim_hcp_sk,

    email.rep_id                            AS rep_id,
    rep.dim_rep_sk                          AS dim_rep_sk,

    email.email_type                        AS email_type,
    email.email_direction                   AS email_direction,
    email.email_subject                     AS email_subject,
    email.email_status                      AS email_status,

    email.email_response_status             AS email_response_status,
    email.response_date                     AS response_date,
    email.email_priority                    AS email_priority,

    email.opened_flag                       AS opened_flag,
    email.open_date                         AS open_date,

    email.clicked_flag                      AS clicked_flag,
    email.click_date                        AS click_date,

    email.attachment_flag                   AS attachment_flag,
    email.response_required_flag            AS response_required_flag,

    email.product_id                        AS product_id,
    prod.dim_prod_sk                        AS dim_prod_sk,

    email.related_call_id                   AS related_call_id,
    call.fct_call_act_sk                    AS fct_call_act_sk,

    email.related_event_id                  AS related_event_id,

    email.campaign_id                       AS campaign_id,
    email.email_channel                     AS email_channel,

    email.created_date                      AS created_date,

    email.source_updated_at                 AS source_updated_at,

    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')           AS dwh_insert_ts,
    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')           AS dwh_update_ts

FROM email

LEFT JOIN hcp
    ON email.hcp_npi = hcp.hcp_npi
    AND email.email_date >= hcp.valid_from
    AND (hcp.valid_to IS NULL
        OR email.email_date < hcp.valid_to)

LEFT JOIN rep
    ON email.rep_id = rep.rep_id
    AND email.email_date >= rep.valid_from
    AND (rep.valid_to IS NULL
        OR email.email_date < rep.valid_to)

LEFT JOIN prod
    ON email.product_id = prod.product_id

LEFT JOIN call
    ON email.related_call_id = call.call_id;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.fct_email_activity
AS
SELECT * FROM fct_email_activity_vw;

-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO onco_pharma_analytics.silver.fct_email_activity AS t
USING (
    SELECT *
    FROM fct_email_activity_vw
    WHERE source_updated_at >= (
        SELECT COALESCE(
                   MAX(source_updated_at) - INTERVAL 1 DAY,
                   TIMESTAMP '1900-01-01'
               )
        FROM onco_pharma_analytics.silver.fct_email_activity
    )
) AS s
ON t.email_activity_id = s.email_activity_id

WHEN MATCHED THEN UPDATE SET
    t.fct_email_act_sk        = s.fct_email_act_sk,
    t.email_date              = s.email_date,
    t.veeva_hcp_id            = s.veeva_hcp_id,
    t.hcp_npi                 = s.hcp_npi,
    t.dim_hcp_sk              = s.dim_hcp_sk,
    t.rep_id                  = s.rep_id,
    t.dim_rep_sk              = s.dim_rep_sk,
    t.email_type              = s.email_type,
    t.email_direction         = s.email_direction,
    t.email_subject           = s.email_subject,
    t.email_status            = s.email_status,
    t.email_response_status   = s.email_response_status,
    t.response_date           = s.response_date,
    t.email_priority          = s.email_priority,
    t.opened_flag             = s.opened_flag,
    t.open_date               = s.open_date,
    t.clicked_flag            = s.clicked_flag,
    t.click_date              = s.click_date,
    t.attachment_flag         = s.attachment_flag,
    t.response_required_flag  = s.response_required_flag,
    t.product_id              = s.product_id,
    t.dim_prod_sk             = s.dim_prod_sk,
    t.related_call_id         = s.related_call_id,
    t.fct_call_act_sk         = s.fct_call_act_sk,
    t.related_event_id        = s.related_event_id,
    t.campaign_id             = s.campaign_id,
    t.email_channel           = s.email_channel,
    t.created_date            = s.created_date,
    t.source_updated_at       = s.source_updated_at,
    t.dwh_update_ts           = s.dwh_update_ts

WHEN NOT MATCHED THEN INSERT *;