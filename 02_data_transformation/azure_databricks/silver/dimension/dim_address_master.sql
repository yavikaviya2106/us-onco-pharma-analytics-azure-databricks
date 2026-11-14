-- Databricks notebook source
-- STEP 1: CREATE DIMENSION TABLE 
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.dim_address_master
USING DELTA
AS
SELECT
    CAST(NULL AS STRING)        AS dim_addr_sk,
    address_id,
    address_line_1,
    address_line_2,
    city,
    county_code,
    county_name,
    state_code,
    state_name,
    zip_code,
    country_code,
    country                      AS country_name,
    latitude,
    longitude,
    metro_flag,
    urban_flag,
    rural_flag,
    address_verified_flag,
    address_validation_status,
    address_effective_date,
    address_end_date,
    last_verified_date,
    address_source,

    CAST(NULL AS STRING)        AS row_hash,
    CAST(NULL AS TIMESTAMP)     AS valid_from,
    CAST(NULL AS TIMESTAMP)     AS snapshot_valid_from,
    CAST(NULL AS TIMESTAMP)     AS valid_to,
    CAST(NULL AS BOOLEAN)       AS is_current

FROM onco_pharma_analytics.silver.stg_address_master
WHERE 1 = 0;

-- COMMAND ----------

-- STEP 2: PREPARE STAGING DATA FOR SCD2
CREATE OR REPLACE TEMP VIEW stg_address_scd 
AS SELECT
    s.*,
    -- Generate hash for change detection
    SHA2(
        TO_JSON(
            NAMED_STRUCT(
                'address_line_1',            s.address_line_1,
                'address_line_2',            s.address_line_2,
                'city',                      s.city,
                'county_code',               s.county_code,
                'county_name',               s.county_name,
                'state_code',                s.state_code,
                'state_name',                s.state_name,
                'zip_code',                  s.zip_code,
                'country_code',              s.country_code,
                'country',                   s.country,
                'latitude',                  s.latitude,
                'longitude',                 s.longitude,
                'metro_flag',                s.metro_flag,
                'urban_flag',                s.urban_flag,
                'rural_flag',                s.rural_flag,
                'address_verified_flag',     s.address_verified_flag,
                'address_validation_status', s.address_validation_status,
                'address_effective_date',    s.address_effective_date,
                'address_end_date',          s.address_end_date,
                'last_verified_date',        s.last_verified_date,
                'address_source',            s.address_source
            ),
            MAP('ignoreNullFields', 'false')
        ),
        256
    ) AS row_hash,

    -- Identify whether this business key has ever existed
    NOT EXISTS (
        SELECT 1
        FROM onco_pharma_analytics.silver.dim_address_master dim
        WHERE dim.address_id = s.address_id
    ) AS is_new_address,

    -- Single timestamp for this load
    CURRENT_TIMESTAMP() AS load_ts

FROM (
    SELECT
        address_id,
        address_line_1,
        address_line_2,
        city,
        county_code,
        county_name,
        state_code,
        state_name,
        zip_code,
        country_code,
        country,
        latitude,
        longitude,
        metro_flag,
        urban_flag,
        rural_flag,
        address_verified_flag,
        address_validation_status,
        address_effective_date,
        address_end_date,
        last_verified_date,
        address_source

    FROM onco_pharma_analytics.silver.stg_address_master

    -- Keep the latest record per business key
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY address_id
        ORDER BY last_verified_date DESC NULLS LAST
    ) = 1

) s;

-- COMMAND ----------

-- STEP 3: SCD2 MERGE
-- CLOSE OLD VERSION + INSERT NEW VERSION
MERGE INTO onco_pharma_analytics.silver.dim_address_master AS tgt
USING (

    -- PART 1: All incoming records
    -- Used to match existing current records or insert new addresses
    SELECT
        s.address_id AS merge_key,
        s.*
    FROM stg_address_scd s

    UNION ALL

    -- PART 2: Changed records only
    -- NULL merge_key forces insertion of a new historical version
    SELECT
        NULL AS merge_key,
        s.*
    FROM stg_address_scd s
    JOIN onco_pharma_analytics.silver.dim_address_master d
        ON d.address_id = s.address_id
       AND d.is_current = TRUE
    WHERE NOT (d.row_hash <=> s.row_hash)

) AS src


ON tgt.address_id = src.merge_key
AND tgt.is_current = TRUE


-- CASE 1: Existing address has changed
-- Expire the old version
WHEN MATCHED
AND NOT (tgt.row_hash <=> src.row_hash)
THEN UPDATE SET
    tgt.valid_to   = src.load_ts,
    tgt.is_current = FALSE


-- CASE 2: New address or changed address
-- Insert a new version
WHEN NOT MATCHED
THEN INSERT (
    dim_addr_sk, address_id, 
    address_line_1, address_line_2,
    city, county_code, county_name, state_code, state_name,
    zip_code, country_code, country_name,
    latitude, longitude,
    metro_flag, urban_flag, rural_flag,
    address_verified_flag, address_validation_status, address_effective_date,
    address_end_date, last_verified_date, address_source,
    row_hash,
    valid_from, snapshot_valid_from,
    valid_to, is_current
)

VALUES (
    -- Surrogate key: business key + version start timestamp
    SHA2(
        TO_JSON(
            NAMED_STRUCT(
                'address_id', src.address_id,
                'valid_from',
                    CASE
                        WHEN src.is_new_address
                        THEN CAST('1900-01-01' AS TIMESTAMP)
                        ELSE src.load_ts
                    END
            ),
            MAP('ignoreNullFields', 'false')
        ),
        256
    ),

    src.address_id, src.address_line_1, src.address_line_2,
    src.city, src.county_code, src.county_name,
    src.state_code, src.state_name, src.zip_code,
    src.country_code, src.country, 
    src.latitude, src.longitude,
    src.metro_flag, src.urban_flag, src.rural_flag,
    src.address_verified_flag, src.address_validation_status,
    src.address_effective_date, src.address_end_date,
    src.last_verified_date, src.address_source,
    src.row_hash,

    -- First version vs subsequent versions
    CASE
        WHEN src.is_new_address
        THEN CAST('1900-01-01' AS TIMESTAMP)
        ELSE src.load_ts
    END,

    -- Actual Databricks load timestamp
    src.load_ts,

    NULL,
    TRUE

);



-- STEP 4: OPTIONAL - EXPIRE MISSING ADDRESSES
-- ENABLE ONLY FOR CONFIRMED FULL SOURCE EXTRACTS

-- WHEN NOT MATCHED BY SOURCE AND tgt.is_current = TRUE
-- THEN UPDATE SET
--     tgt.valid_to   = CURRENT_TIMESTAMP(),
--     tgt.is_current = FALSE
;