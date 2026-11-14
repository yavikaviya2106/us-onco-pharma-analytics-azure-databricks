-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW fct_sp_transactions_vw AS

WITH sp_src AS (
    SELECT *
    FROM onco_pharma_analytics.silver.stg_sp_transactions

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY sp_transaction_id
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

prod AS (
    SELECT
        dim_prod_sk,
        product_id,
        ndc_code
    FROM onco_pharma_analytics.silver.dim_product_details
)

SELECT
    md5(CAST(sp_src.sp_transaction_id AS STRING)) AS fct_sp_txn_sk,

    sp_src.sp_transaction_id                    AS sp_transaction_id,
    sp_src.hub_case_id                          AS hub_case_id,
    sp_src.sp_case_id                           AS sp_case_id,

    sp_src.patient_id                           AS patient_id,

    sp_src.hcp_npi                              AS hcp_npi,
    hcp.dim_hcp_sk                              AS dim_hcp_sk,

    sp_src.ndc_code                             AS ndc_code,
    prod.product_id                             AS product_id,
    prod.dim_prod_sk                            AS dim_prod_sk,

    sp_src.transaction_date                     AS transaction_date,
    sp_src.transaction_type                     AS transaction_type,
    sp_src.transaction_status                   AS transaction_status,

    sp_src.rx_number                            AS rx_number,
    sp_src.fill_number                          AS fill_number,

    sp_src.quantity_prescribed                  AS quantity_prescribed,
    sp_src.quantity_dispensed                   AS quantity_dispensed,

    sp_src.unit_of_measure                      AS unit_of_measure,
    sp_src.package_quantity                     AS package_quantity,
    sp_src.package_count                        AS package_count,
    sp_src.units_per_day                        AS units_per_day,

    sp_src.days_supply                          AS days_supply,

    sp_src.shipment_date                        AS shipment_date,
    sp_src.delivery_date                        AS delivery_date,

    sp_src.payer_id                             AS payer_id,
    sp_src.copay_amount                         AS copay_amount,

    sp_src.source_updated_at                    AS source_updated_at,

    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')               AS dwh_insert_ts,
    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')               AS dwh_update_ts

FROM sp_src

LEFT JOIN hcp
    ON sp_src.hcp_npi = hcp.hcp_npi
    AND sp_src.transaction_date >= hcp.valid_from
    AND (hcp.valid_to IS NULL
        OR sp_src.transaction_date < hcp.valid_to)

LEFT JOIN prod
    ON sp_src.ndc_code = prod.ndc_code;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.fct_sp_transactions
AS
SELECT * FROM fct_sp_transactions_vw;

-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO onco_pharma_analytics.silver.fct_sp_transactions AS t
USING (
    SELECT *
    FROM fct_sp_transactions_vw
    
    WHERE source_updated_at >= (
        SELECT COALESCE(
                   MAX(source_updated_at) - INTERVAL 1 DAY,
                   TIMESTAMP '1900-01-01'
               )
        FROM onco_pharma_analytics.silver.fct_sp_transactions
    )
) AS s
ON t.sp_transaction_id = s.sp_transaction_id

WHEN MATCHED THEN UPDATE SET
    t.fct_sp_txn_sk        = s.fct_sp_txn_sk,
    t.hub_case_id          = s.hub_case_id,
    t.sp_case_id           = s.sp_case_id,
    t.patient_id           = s.patient_id,
    t.hcp_npi              = s.hcp_npi,
    t.dim_hcp_sk           = s.dim_hcp_sk,
    t.ndc_code             = s.ndc_code,
    t.product_id           = s.product_id,
    t.dim_prod_sk          = s.dim_prod_sk,
    t.transaction_date     = s.transaction_date,
    t.transaction_type     = s.transaction_type,
    t.transaction_status   = s.transaction_status,
    t.rx_number            = s.rx_number,
    t.fill_number          = s.fill_number,
    t.quantity_prescribed  = s.quantity_prescribed,
    t.quantity_dispensed   = s.quantity_dispensed,
    t.unit_of_measure      = s.unit_of_measure,
    t.package_quantity     = s.package_quantity,
    t.package_count        = s.package_count,
    t.units_per_day        = s.units_per_day,
    t.days_supply          = s.days_supply,
    t.shipment_date        = s.shipment_date,
    t.delivery_date        = s.delivery_date,
    t.payer_id             = s.payer_id,
    t.copay_amount         = s.copay_amount,
    t.source_updated_at    = s.source_updated_at,
    t.dwh_update_ts        = s.dwh_update_ts

WHEN NOT MATCHED THEN INSERT *;