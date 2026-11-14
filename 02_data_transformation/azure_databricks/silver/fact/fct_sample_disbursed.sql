-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW fct_sample_disbursed_vw AS

WITH sample_dis AS (
    SELECT * FROM onco_pharma_analytics.silver.stg_sample_disbursed

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY sample_id
        ORDER BY source_updated_at DESC
    ) = 1
),

call AS (
    SELECT
        fct_call_act_sk,
        call_id
    FROM onco_pharma_analytics.silver.fct_call_activity
),

call_det AS (
    SELECT
        fct_call_dtl_sk,
        call_detail_id
    FROM onco_pharma_analytics.silver.fct_call_detail
),

prod AS (
    SELECT
        dim_prod_sk,
        product_id
    FROM onco_pharma_analytics.silver.dim_product_details
)

SELECT
    md5(CAST(sample_dis.sample_id AS STRING)) AS fct_sample_dis_sk,
    sample_dis.sample_id                                AS sample_id,

    sample_dis.call_id                                  AS call_id,
    call.fct_call_act_sk                            AS fct_call_act_sk,

    sample_dis.call_detail_id                           AS call_detail_id,
    call_det.fct_call_dtl_sk                        AS fct_call_dtl_sk,

    sample_dis.product_id                               AS product_id,
    prod.dim_prod_sk                                AS dim_prod_sk,

    sample_dis.sample_lot_number                        AS sample_lot_number,
    sample_dis.sample_quantity                          AS sample_quantity,
    sample_dis.sample_unit                              AS sample_unit,
    sample_dis.sample_type                              AS sample_type,
    sample_dis.sample_status                            AS sample_status,

    sample_dis.disbursement_date                        AS disbursement_date,
    sample_dis.expiration_date                          AS expiration_date,

    sample_dis.signature_captured_flag                  AS signature_captured_flag,
    sample_dis.acknowledgement_flag                     AS acknowledgement_flag,

    sample_dis.return_required_flag                     AS return_required_flag,
    sample_dis.return_date                              AS return_date,
    sample_dis.return_quantity                          AS return_quantity,
    sample_dis.return_reason                            AS return_reason,

    sample_dis.disbursement_reason                      AS disbursement_reason,

    sample_dis.sample_recipient_type                    AS sample_recipient_type,

    sample_dis.notes                                    AS notes,

    sample_dis.source_updated_at                        AS source_updated_at,

    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')                   AS dwh_insert_ts,
    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')                   AS dwh_update_ts

FROM sample_dis

LEFT JOIN call
    ON sample_dis.call_id = call.call_id

LEFT JOIN call_det
    ON sample_dis.call_detail_id = call_det.call_detail_id

LEFT JOIN prod
    ON sample_dis.product_id = prod.product_id;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.fct_sample_disbursed
AS
SELECT * FROM fct_sample_disbursed_vw;

-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO onco_pharma_analytics.silver.fct_sample_disbursed AS t
USING (
    SELECT *
    FROM fct_sample_disbursed_vw
    
    WHERE source_updated_at >= (
        SELECT COALESCE(
                   MAX(source_updated_at) - INTERVAL 1 DAY,
                   TIMESTAMP '1900-01-01'
               )
        FROM onco_pharma_analytics.silver.fct_sample_disbursed
    )
) AS s
ON t.sample_id = s.sample_id

WHEN MATCHED THEN UPDATE SET
    t.fct_sample_dis_sk        = s.fct_sample_dis_sk,
    t.call_id                  = s.call_id,
    t.fct_call_act_sk          = s.fct_call_act_sk,
    t.call_detail_id           = s.call_detail_id,
    t.fct_call_dtl_sk          = s.fct_call_dtl_sk,
    t.product_id               = s.product_id,
    t.dim_prod_sk              = s.dim_prod_sk,
    t.sample_lot_number        = s.sample_lot_number,
    t.sample_quantity          = s.sample_quantity,
    t.sample_unit              = s.sample_unit,
    t.sample_type              = s.sample_type,
    t.sample_status            = s.sample_status,
    t.disbursement_date        = s.disbursement_date,
    t.expiration_date          = s.expiration_date,
    t.signature_captured_flag  = s.signature_captured_flag,
    t.acknowledgement_flag     = s.acknowledgement_flag,
    t.return_required_flag     = s.return_required_flag,
    t.return_date              = s.return_date,
    t.return_quantity          = s.return_quantity,
    t.return_reason            = s.return_reason,
    t.disbursement_reason      = s.disbursement_reason,
    t.sample_recipient_type    = s.sample_recipient_type,
    t.notes                    = s.notes,
    t.source_updated_at        = s.source_updated_at,
    t.dwh_update_ts            = s.dwh_update_ts

WHEN NOT MATCHED THEN INSERT *;