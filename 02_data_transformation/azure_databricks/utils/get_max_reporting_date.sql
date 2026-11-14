-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS onco_pharma_analytics.common;

-- COMMAND ----------

-- Reusable SQL function
CREATE OR REPLACE FUNCTION
    onco_pharma_analytics.common.get_max_reporting_date()
RETURNS DATE
LANGUAGE SQL
READS SQL DATA
RETURN
    (
        SELECT MAX(max_date)
        FROM (
            SELECT MAX(week_end_date) AS max_date
            FROM onco_pharma_analytics.silver.stg_xponent_data

            UNION ALL

            SELECT MAX(week_end_date) AS max_date
            FROM onco_pharma_analytics.silver.stg_drug_distribution_data
        ) AS source_dates
    );