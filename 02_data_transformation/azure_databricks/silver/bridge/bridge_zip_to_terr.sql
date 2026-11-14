-- Databricks notebook source
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.bridge_zip_to_terr
USING DELTA
AS
SELECT
    CAST(NULL AS STRING)    AS bridge_zip_terr_sk,
    zip_code,
    city_name,
    state_code,
    territory_id,
    CAST(NULL AS STRING)    AS row_hash,
    CAST(NULL AS DATE)      AS valid_from,
    CAST(NULL AS TIMESTAMP) AS snapshot_valid_from,
    CAST(NULL AS DATE)      AS valid_to,
    CAST(NULL AS BOOLEAN)   AS is_current
FROM onco_pharma_analytics.silver.stg_zip_to_terr
WHERE 1 = 0;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW stg_zip_terr_scd AS
SELECT
    s.*,

    -- Check whether this ZIP code has ever existed
    NOT EXISTS (
        SELECT 1
        FROM onco_pharma_analytics.silver.bridge_zip_to_terr dim
        WHERE dim.zip_code = s.zip_code
    ) AS is_new_key,

    -- Load timestamp
    CURRENT_TIMESTAMP() AS load_ts

FROM (
    SELECT
        zip_code,
        city_name,
        state_code,
        territory_id,

        -- Hash for change detection
        SHA2(
            TO_JSON(
                NAMED_STRUCT(
                    'city_name',    city_name,
                    'state_code',   state_code,
                    'territory_id', territory_id
                ),
                MAP('ignoreNullFields', 'false')
            ),
            256
        ) AS row_hash

    FROM onco_pharma_analytics.silver.stg_zip_to_terr
) s;

-- COMMAND ----------

MERGE INTO onco_pharma_analytics.silver.bridge_zip_to_terr AS tgt
USING (

    SELECT
        s.zip_code AS merge_key,
        s.*
    FROM stg_zip_terr_scd s

    UNION ALL

    SELECT
        CAST(NULL AS STRING) AS merge_key,
        s.*
    FROM stg_zip_terr_scd s

    JOIN onco_pharma_analytics.silver.bridge_zip_to_terr dim
        ON dim.zip_code = s.zip_code
       AND dim.is_current = TRUE

    WHERE NOT (dim.row_hash <=> s.row_hash)

) AS src


ON tgt.zip_code = src.merge_key
AND tgt.is_current = TRUE


WHEN MATCHED
AND NOT (tgt.row_hash <=> src.row_hash)
THEN UPDATE SET
    tgt.valid_to    = CAST(src.load_ts AS DATE),
    tgt.is_current  = FALSE


WHEN NOT MATCHED
THEN INSERT (
    bridge_zip_terr_sk,
    zip_code,
    city_name,
    state_code,
    territory_id,
    row_hash,
    valid_from,
    snapshot_valid_from,
    valid_to,
    is_current
)

VALUES (
    -- Surrogate key: business key + version load timestamp
    SHA2(
        TO_JSON(
            NAMED_STRUCT(
                'zip_code', src.zip_code,
                'snapshot_valid_from', src.load_ts
            ),
            MAP('ignoreNullFields', 'false')
        ),
        256
    ),

    src.zip_code,
    src.city_name,
    src.state_code,
    src.territory_id,
    src.row_hash,

    CASE
        WHEN src.is_new_key
        THEN DATE '1900-01-01'
        ELSE CAST(src.load_ts AS DATE)
    END,

    -- Actual Databricks load timestamp
    src.load_ts,

    NULL,
    TRUE
);


-- OPTIONAL - EXPIRE MISSING ZIP CODES
-- ENABLE ONLY FOR CONFIRMED FULL SOURCE EXTRACTS

-- WHEN NOT MATCHED BY SOURCE AND tgt.is_current = TRUE
-- THEN UPDATE SET
--     tgt.valid_to   = CURRENT_DATE(),
--     tgt.is_current = FALSE