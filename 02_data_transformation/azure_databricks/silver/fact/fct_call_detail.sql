-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW fct_call_detail_vw AS

WITH call_det AS (
    SELECT * FROM onco_pharma_analytics.silver.stg_call_detail

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY call_detail_id
        ORDER BY source_updated_at DESC
    ) = 1
),

call AS (
    SELECT
        fct_call_act_sk,
        call_id
     FROM onco_pharma_analytics.silver.fct_call_activity
),

prod AS (
    SELECT
        dim_prod_sk,
        product_id
    FROM onco_pharma_analytics.silver.dim_product_details
),

key_msg AS (
    SELECT
        dim_key_msg_sk,
        key_message_id
    FROM onco_pharma_analytics.silver.dim_key_msg_master
)

SELECT

    md5(CAST(call_det.call_detail_id AS STRING)) AS fct_call_dtl_sk,
    call_det.call_detail_id             AS call_detail_id,

    call_det.call_id                    AS call_id,
    call.fct_call_act_sk                AS fct_call_act_sk,

    call_det.product_id                 AS product_id,
    prod.dim_prod_sk                    AS dim_prod_sk,
    call_det.product_position           AS product_position,

    call_det.key_message_id             AS key_message_id,
    call_det.message_theme              AS message_theme,

    call_det.detail_type                AS detail_type,
    call_det.detail_priority            AS detail_priority,
    call_det.discussion_flag            AS discussion_flag,
    call_det.presentation_type          AS presentation_type,

    call_det.hcp_reaction               AS hcp_reaction,
    call_det.hcp_response               AS hcp_response,
    call_det.hcp_interest_level         AS hcp_interest_level,
    call_det.objection_flag             AS objection_flag,
    call_det.objection_type             AS objection_type,

    call_det.detail_notes               AS detail_notes,

    call_det.source_updated_at          AS source_updated_at,

    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')       AS dwh_insert_ts,
    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')       AS dwh_update_ts

FROM call_det

LEFT JOIN call
    ON call_det.call_id = call.call_id

LEFT JOIN prod
    ON call_det.product_id = prod.product_id

LEFT JOIN key_msg
    ON call_det.key_message_id = key_msg.key_message_id;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.fct_call_detail
AS
SELECT * FROM fct_call_detail_vw;

-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO onco_pharma_analytics.silver.fct_call_detail AS t
USING (
    SELECT *
    FROM fct_call_detail_vw
   
    WHERE source_updated_at >= (
        SELECT COALESCE(
                   MAX(source_updated_at) - INTERVAL 1 DAY,
                   TIMESTAMP '1900-01-01'
               )
        FROM onco_pharma_analytics.silver.fct_call_detail
    )
) AS s
ON t.call_detail_id = s.call_detail_id

WHEN MATCHED THEN UPDATE SET
    t.fct_call_dtl_sk     = s.fct_call_dtl_sk,
    t.call_id             = s.call_id,
    t.fct_call_act_sk     = s.fct_call_act_sk,
    t.product_id          = s.product_id,
    t.dim_prod_sk         = s.dim_prod_sk,
    t.product_position    = s.product_position,
    t.key_message_id      = s.key_message_id,
    t.message_theme       = s.message_theme,
    t.detail_type         = s.detail_type,
    t.detail_priority     = s.detail_priority,
    t.discussion_flag     = s.discussion_flag,
    t.presentation_type   = s.presentation_type,
    t.hcp_reaction        = s.hcp_reaction,
    t.hcp_response        = s.hcp_response,
    t.hcp_interest_level  = s.hcp_interest_level,
    t.objection_flag      = s.objection_flag,
    t.objection_type      = s.objection_type,
    t.detail_notes        = s.detail_notes,
    t.source_updated_at   = s.source_updated_at,
    t.dwh_update_ts       = s.dwh_update_ts

WHEN NOT MATCHED THEN INSERT *;