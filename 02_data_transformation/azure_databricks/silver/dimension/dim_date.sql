-- Databricks notebook source
-- Date Range    : 2021-01-01 to 2030-12-31
-- Business Week : Saturday to Friday
-- Fiscal Year   : January to December

CREATE OR REPLACE TABLE onco_pharma_analytics.silver.dim_date
USING DELTA
AS

WITH date_spine AS (
    SELECT
        explode(
            sequence(
                DATE '2021-01-01',
                DATE '2030-12-31',
                INTERVAL 1 DAY
            )
        ) AS date_day
),

date_base AS (
    SELECT
        date_day,

        -- Saturday as the start of the business week
        date_sub(
            date_day,
            pmod(dayofweek(date_day), 7)
        ) AS week_start_date

    FROM date_spine
),

date_ranges AS (
    SELECT
        date_day,

        -- Week
        week_start_date,
        date_add(week_start_date, 6) AS week_end_date,

        -- Month 
        trunc(date_day, 'MONTH') AS month_start_date,
        last_day(date_day) AS month_end_date,

        -- Quarter 
        CAST(date_trunc('QUARTER', date_day) AS DATE)
            AS quarter_start_date,

        date_sub(
            add_months(
                CAST(date_trunc('QUARTER', date_day) AS DATE),
                3
            ),
            1
        ) AS quarter_end_date,

        -- Year 
        trunc(date_day, 'YEAR') AS year_start_date,
        make_date(year(date_day), 12, 31) AS year_end_date

    FROM date_base
),

date_final AS (
    SELECT
        date_day AS date,

        dayofweek(date_day) - 1 AS day_of_week,
        date_format(date_day, 'E') AS day_name,
        day(date_day) AS day_of_month,
        dayofyear(date_day) AS day_of_year,

    
        weekofyear(week_end_date) AS week_number,
        year(
            date_add(
                week_end_date,
                4 - pmod(dayofweek(week_end_date) + 5, 7)
            )
        ) AS week_year,
        concat(
            'WK',
            lpad(
                CAST(weekofyear(week_end_date) AS STRING),
                2,
                '0'
            ),
            '-',
            CAST(
                year(
                    date_add(
                        week_end_date,
                        4 - pmod(dayofweek(week_end_date) + 5, 7)
                    )
                ) AS STRING
            )
        ) AS week_name,
        week_start_date,
        week_end_date,
       
        month(date_day) AS month_number,
        date_format(date_day, 'MMM-yyyy') AS month_name,
        month_start_date,
        month_end_date,

        
        quarter(date_day) AS quarter_number,
        concat(
            'Q',
            CAST(quarter(date_day) AS STRING),
            '-',
            CAST(year(date_day) AS STRING)
        ) AS quarter_name,
        quarter_start_date,
        quarter_end_date,

    
        year(date_day) AS fiscal_year,
        concat(
            'FY-',
            CAST(year(date_day) AS STRING)
        ) AS fiscal_year_name,
        year_start_date AS fiscal_year_start_date,
        year_end_date AS fiscal_year_end_date,


        CASE
            WHEN month(date_day) = 1
             AND day(date_day) = 1 THEN TRUE

            WHEN month(date_day) = 7
             AND day(date_day) = 4 THEN TRUE

            WHEN month(date_day) = 11
             AND day(date_day) = 11 THEN TRUE

            WHEN month(date_day) = 12
             AND day(date_day) = 25 THEN TRUE

            ELSE FALSE
        END AS holiday_flag,

      
        CASE
            WHEN month(date_day) = 1
             AND day(date_day) = 1
                THEN 'New Years Day'

            WHEN month(date_day) = 7
             AND day(date_day) = 4
                THEN 'Independence Day'

            WHEN month(date_day) = 11
             AND day(date_day) = 11
                THEN 'Veterans Day'

            WHEN month(date_day) = 12
             AND day(date_day) = 25
                THEN 'Christmas Day'

            ELSE NULL
        END AS holiday_name

    FROM date_ranges
)

SELECT * FROM date_final;