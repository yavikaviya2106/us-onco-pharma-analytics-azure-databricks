-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW fct_rx_sales_vw AS

WITH rx AS (
    SELECT *
    FROM onco_pharma_analytics.silver.stg_xponent_data

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY xponent_record_id
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
    md5(CAST(rx.xponent_record_id AS STRING)) AS fct_rx_sk,
    rx.xponent_record_id                    AS xponent_record_id,

    rx.iqvia_hcp_id                         AS iqvia_hcp_id,
    rx.hcp_npi                              AS hcp_npi,
    hcp.dim_hcp_sk                          AS dim_hcp_sk,

    rx.ndc_code                             AS ndc_code,
    prod.dim_prod_sk                        AS dim_prod_sk,
    prod.product_id                         AS product_id,

    rx.week_end_date                        AS week_end_date,

    rx.trx                                  AS trx,
    rx.nrx                                  AS nrx,
    rx.nbrx                                 AS nbrx,
    rx.refill_rx                            AS refill_rx,

    rx.trx_units                            AS trx_units,
    rx.nrx_units                            AS nrx_units,
    rx.nbrx_units                           AS nbrx_units,

    rx.market_trx                           AS market_trx,
    rx.market_nrx                           AS market_nrx,

    rx.source_updated_at                    AS source_updated_at,

    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')           AS dwh_insert_ts,
    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')           AS dwh_update_ts

FROM rx

LEFT JOIN hcp
    ON rx.hcp_npi = hcp.hcp_npi
    AND rx.week_end_date >= hcp.valid_from
    AND (hcp.valid_to IS NULL
        OR rx.week_end_date < hcp.valid_to)

LEFT JOIN prod
    ON rx.ndc_code = prod.ndc_code;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.fct_rx_sales
AS
SELECT * FROM fct_rx_sales_vw;

-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO onco_pharma_analytics.silver.fct_rx_sales AS t
USING (
    SELECT *
    FROM fct_rx_sales_vw
    -- is_incremental() filter: 1-day look-back from the latest loaded row
    WHERE source_updated_at >= (
        SELECT COALESCE(
                   MAX(source_updated_at) - INTERVAL 1 DAY,
                   TIMESTAMP '1900-01-01'
               )
        FROM onco_pharma_analytics.silver.fct_rx_sales
    )
) AS s
ON t.xponent_record_id = s.xponent_record_id

-- merge_exclude_columns = ['dwh_insert_ts']
WHEN MATCHED THEN UPDATE SET
    t.fct_rx_sk          = s.fct_rx_sk,
    t.iqvia_hcp_id       = s.iqvia_hcp_id,
    t.hcp_npi            = s.hcp_npi,
    t.dim_hcp_sk         = s.dim_hcp_sk,
    t.ndc_code           = s.ndc_code,
    t.dim_prod_sk        = s.dim_prod_sk,
    t.product_id         = s.product_id,
    t.week_end_date      = s.week_end_date,
    t.trx                = s.trx,
    t.nrx                = s.nrx,
    t.nbrx               = s.nbrx,
    t.refill_rx          = s.refill_rx,
    t.trx_units          = s.trx_units,
    t.nrx_units          = s.nrx_units,
    t.nbrx_units         = s.nbrx_units,
    t.market_trx         = s.market_trx,
    t.market_nrx         = s.market_nrx,
    t.source_updated_at  = s.source_updated_at,
    t.dwh_update_ts      = s.dwh_update_ts

WHEN NOT MATCHED THEN INSERT *;