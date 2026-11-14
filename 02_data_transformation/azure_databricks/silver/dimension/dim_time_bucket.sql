-- Databricks notebook source
CREATE OR REPLACE TABLE onco_pharma_analytics.silver.dim_time_bucket
USING DELTA
AS

WITH reporting_limit AS (
    SELECT
        onco_pharma_analytics.common.get_max_reporting_date()
            AS max_reporting_date
),

dates AS (
    SELECT *
    FROM onco_pharma_analytics.silver.dim_date
    WHERE date <= (
        SELECT max_reporting_date
        FROM reporting_limit
    )
),

-- Calendar buckets: one row per distinct period
week AS (
    SELECT DISTINCT
        'WEEK' AS bucket_type,
        week_name AS bucket_name,
        week_start_date AS bucket_start_date,
        week_end_date AS bucket_end_date,
        CAST(NULL AS DATE) AS till_date
    FROM dates
),

month AS (
    SELECT DISTINCT
        'MONTH',
        month_name,
        month_start_date,
        month_end_date,
        CAST(NULL AS DATE)
    FROM dates
),

quarter AS (
    SELECT DISTINCT
        'QUARTER',
        quarter_name,
        quarter_start_date,
        quarter_end_date,
        CAST(NULL AS DATE)
    FROM dates
),

year AS (
    SELECT DISTINCT
        'YEAR',
        fiscal_year_name,
        fiscal_year_start_date,
        fiscal_year_end_date,
        CAST(NULL AS DATE)
    FROM dates
),


r4w AS (
    SELECT DISTINCT
        'R4W',
        concat('R4W-', week_name),
        date_sub(week_start_date, 21),
        week_end_date,
        week_end_date
    FROM dates
),

r13w AS (
    SELECT DISTINCT
        'R13W',
        concat('R13W-', week_name),
        date_sub(week_start_date, 84),
        week_end_date,
        week_end_date
    FROM dates
),

r26w AS (
    SELECT DISTINCT
        'R26W',
        concat('R26W-', week_name),
        date_sub(week_start_date, 175),
        week_end_date,
        week_end_date
    FROM dates
),

r52w AS (
    SELECT DISTINCT
        'R52W',
        concat('R52W-', week_name),
        date_sub(week_start_date, 357),
        week_end_date,
        week_end_date
    FROM dates
),


wtd AS (
    SELECT
        'WTD',
        concat('WTD-', date_format(date, 'yyyy-MM-dd')),
        week_start_date,
        date,
        date
    FROM dates
),

mtd AS (
    SELECT
        'MTD',
        concat('MTD-', date_format(date, 'yyyy-MM-dd')),
        month_start_date,
        date,
        date
    FROM dates
),

qtd AS (
    SELECT
        'QTD',
        concat('QTD-', date_format(date, 'yyyy-MM-dd')),
        quarter_start_date,
        date,
        date
    FROM dates
),

ytd AS (
    SELECT
        'YTD',
        concat('YTD-', date_format(date, 'yyyy-MM-dd')),
        fiscal_year_start_date,
        date,
        date
    FROM dates
),

-- Combine all 12 bucket types
buckets AS (
    SELECT * FROM week
    UNION ALL SELECT * FROM month
    UNION ALL SELECT * FROM quarter
    UNION ALL SELECT * FROM year
    UNION ALL SELECT * FROM r4w
    UNION ALL SELECT * FROM r13w
    UNION ALL SELECT * FROM r26w
    UNION ALL SELECT * FROM r52w
    UNION ALL SELECT * FROM wtd
    UNION ALL SELECT * FROM mtd
    UNION ALL SELECT * FROM qtd
    UNION ALL SELECT * FROM ytd
)

SELECT
    b.bucket_type,
    b.bucket_name,
    b.bucket_start_date,

    LEAST(
        b.bucket_end_date,
        (SELECT max_reporting_date FROM reporting_limit)
    ) AS bucket_end_date,

    b.till_date

FROM buckets b

WHERE b.bucket_start_date <= (
    SELECT max_reporting_date
    FROM reporting_limit
);