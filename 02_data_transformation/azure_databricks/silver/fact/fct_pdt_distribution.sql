-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW fct_pdt_distribution_vw AS

WITH ddd AS (
    SELECT *
    FROM onco_pharma_analytics.silver.stg_drug_distribution_data

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ddd_record_id
        ORDER BY source_updated_at DESC
    ) = 1
),

prod AS (
    SELECT
        dim_prod_sk,
        product_id,
        ndc_code
    FROM onco_pharma_analytics.silver.dim_product_details
)

SELECT
    md5(CAST(ddd.ddd_record_id AS STRING))      AS fct_ddd_sk,
    ddd.ddd_record_id                           AS ddd_record_id,

    ddd.outlet_id                               AS outlet_id,
    ddd.account_name                            AS account_name,
    ddd.account_type                            AS account_type,
    ddd.zip_code                                AS zip_code,

    ddd.class_of_trade                          AS class_of_trade,
    ddd.channel                                 AS channel,
    ddd.sub_channel                             AS sub_channel,

    ddd.ndc_code                                AS ndc_code,
    prod.dim_prod_sk                            AS dim_prod_sk,
    prod.product_id                             AS product_id,

    ddd.week_end_date                           AS week_end_date,

    ddd.sales_units                             AS sales_units,
    ddd.standard_units                          AS standard_units,
    ddd.package_units                           AS package_units,
    ddd.returns_units                           AS returns_units,

    ddd.source_updated_at                       AS source_updated_at,

    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')               AS dwh_insert_ts,
    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')               AS dwh_update_ts

FROM ddd

LEFT JOIN prod
    ON ddd.ndc_code = prod.ndc_code;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.fct_pdt_distribution
AS
SELECT * FROM fct_pdt_distribution_vw;

-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO onco_pharma_analytics.silver.fct_pdt_distribution AS t
USING (
    SELECT *
    FROM fct_pdt_distribution_vw
    
    WHERE source_updated_at >= (
        SELECT COALESCE(
                   MAX(source_updated_at) - INTERVAL 1 DAY,
                   TIMESTAMP '1900-01-01'
               )
        FROM onco_pharma_analytics.silver.fct_pdt_distribution
    )
) AS s
ON t.ddd_record_id = s.ddd_record_id

WHEN MATCHED THEN UPDATE SET
    t.fct_ddd_sk         = s.fct_ddd_sk,
    t.outlet_id          = s.outlet_id,
    t.account_name       = s.account_name,
    t.account_type       = s.account_type,
    t.zip_code           = s.zip_code,
    t.class_of_trade     = s.class_of_trade,
    t.channel            = s.channel,
    t.sub_channel        = s.sub_channel,
    t.ndc_code           = s.ndc_code,
    t.dim_prod_sk        = s.dim_prod_sk,
    t.product_id         = s.product_id,
    t.week_end_date      = s.week_end_date,
    t.sales_units        = s.sales_units,
    t.standard_units     = s.standard_units,
    t.package_units      = s.package_units,
    t.returns_units      = s.returns_units,
    t.source_updated_at  = s.source_updated_at,
    t.dwh_update_ts      = s.dwh_update_ts

WHEN NOT MATCHED THEN INSERT *;