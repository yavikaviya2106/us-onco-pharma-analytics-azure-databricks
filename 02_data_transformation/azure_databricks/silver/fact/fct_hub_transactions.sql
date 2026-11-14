-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW fct_hub_transactions_vw AS

WITH hub_src AS (
    SELECT *
    FROM onco_pharma_analytics.silver.stg_hub_transactions

    -- Case-only rows (no transaction) are not transactions: keep them out of the fact
    WHERE hub_pap_transaction_id IS NOT NULL

    -- One row per transaction (latest version) so the MERGE never sees duplicate keys
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY hub_pap_transaction_id
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
    md5(CAST(hub_src.hub_pap_transaction_id AS STRING)) AS fct_hub_txn_sk,

    hub_src.hub_pap_transaction_id              AS hub_pap_transaction_id,
    hub_src.hub_case_id                         AS hub_case_id,

    hub_src.patient_id                          AS patient_id,

    hub_src.hcp_npi                             AS hcp_npi,
    hcp.dim_hcp_sk                              AS dim_hcp_sk,

    hub_src.ndc_code                            AS ndc_code,
    prod.product_id                             AS product_id,
    prod.dim_prod_sk                            AS dim_prod_sk,

    hub_src.transaction_date                    AS transaction_date,
    hub_src.transaction_type                    AS transaction_type,
    hub_src.transaction_status                  AS transaction_status,

    hub_src.pap_program_id                      AS pap_program_id,

    hub_src.quantity_requested                  AS quantity_requested,
    hub_src.quantity_approved                   AS quantity_approved,
    hub_src.quantity_dispensed                  AS quantity_dispensed,
    hub_src.fill_number                         AS fill_number,

    hub_src.shipment_date                       AS shipment_date,
    hub_src.delivery_date                       AS delivery_date,

    hub_src.unit_of_measure                     AS unit_of_measure,
    hub_src.package_quantity                    AS package_quantity,
    hub_src.package_count                       AS package_count,
    hub_src.days_supply                         AS days_supply,
    hub_src.units_per_day                       AS units_per_day,

    hub_src.pap_reason                          AS pap_reason,
    hub_src.pap_outcome                         AS pap_outcome,

    hub_src.source_updated_at                   AS source_updated_at,

    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')               AS dwh_insert_ts,
    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')               AS dwh_update_ts

FROM hub_src

LEFT JOIN hcp
    ON hub_src.hcp_npi = hcp.hcp_npi
    AND hub_src.transaction_date >= hcp.valid_from
    AND (hcp.valid_to IS NULL
        OR hub_src.transaction_date < hcp.valid_to)

LEFT JOIN prod
    ON hub_src.ndc_code = prod.ndc_code


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.fct_hub_transactions
AS
SELECT * FROM fct_hub_transactions_vw;

-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO onco_pharma_analytics.silver.fct_hub_transactions AS t
USING (
    SELECT *
    FROM fct_hub_transactions_vw
    WHERE source_updated_at >= (
        SELECT COALESCE(
                   MAX(source_updated_at) - INTERVAL 1 DAY,
                   TIMESTAMP '1900-01-01'
               )
        FROM onco_pharma_analytics.silver.fct_hub_transactions
    )
) AS s
ON t.hub_pap_transaction_id = s.hub_pap_transaction_id

WHEN MATCHED THEN UPDATE SET
    t.fct_hub_txn_sk      = s.fct_hub_txn_sk,
    t.hub_case_id         = s.hub_case_id,
    t.patient_id          = s.patient_id,
    t.hcp_npi             = s.hcp_npi,
    t.dim_hcp_sk          = s.dim_hcp_sk,
    t.ndc_code            = s.ndc_code,
    t.product_id          = s.product_id,
    t.dim_prod_sk         = s.dim_prod_sk,
    t.transaction_date    = s.transaction_date,
    t.transaction_type    = s.transaction_type,
    t.transaction_status  = s.transaction_status,
    t.pap_program_id      = s.pap_program_id,
    t.quantity_requested  = s.quantity_requested,
    t.quantity_approved   = s.quantity_approved,
    t.quantity_dispensed  = s.quantity_dispensed,
    t.fill_number         = s.fill_number,
    t.shipment_date       = s.shipment_date,
    t.delivery_date       = s.delivery_date,
    t.unit_of_measure     = s.unit_of_measure,
    t.package_quantity    = s.package_quantity,
    t.package_count       = s.package_count,
    t.days_supply         = s.days_supply,
    t.units_per_day       = s.units_per_day,
    t.pap_reason          = s.pap_reason,
    t.pap_outcome         = s.pap_outcome,
    t.source_updated_at   = s.source_updated_at,
    t.dwh_update_ts       = s.dwh_update_ts

WHEN NOT MATCHED THEN INSERT *;