-- Databricks notebook source
CREATE OR REPLACE TABLE onco_pharma_analytics.silver.dim_product_ltd_bucket
USING DELTA
AS

WITH reporting_limit AS (
    SELECT
        onco_pharma_analytics.common.get_max_reporting_date()
            AS max_reporting_date
),

weeks AS (
    SELECT DISTINCT
        week_name,
        week_start_date,
        week_end_date
    FROM onco_pharma_analytics.silver.dim_date
    WHERE date <= 
    (
        SELECT max_reporting_date
        FROM reporting_limit
    )
),

pdt AS (
    SELECT DISTINCT
        product_id,
        launch_date
    FROM onco_pharma_analytics.silver.dim_product_details
    WHERE launch_date IS NOT NULL
      AND upper(status) = 'ACTIVE'
)

SELECT
    pdt.product_id,

    'LTD' AS bucket_type,
    'LTD' AS bucket_name,

    pdt.launch_date AS bucket_start_date,

    LEAST(
        weeks.week_end_date,
        (SELECT max_reporting_date FROM reporting_limit)
    ) AS bucket_end_date,

    LEAST(
        weeks.week_end_date,
        (SELECT max_reporting_date FROM reporting_limit)
    ) AS till_date

FROM pdt

JOIN weeks
    ON weeks.week_end_date >= pdt.launch_date

WHERE pdt.launch_date <= (
    SELECT max_reporting_date
    FROM reporting_limit
);