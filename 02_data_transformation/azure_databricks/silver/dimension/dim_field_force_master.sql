-- Databricks notebook source
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.dim_field_force_master
USING DELTA
AS
SELECT
    CAST(NULL AS STRING)    AS dim_rep_sk,
    rep_id,
    rep_name,
    rep_role,
    hire_date,
    effective_start_date,
    effective_end_date,
    rep_status,

    CAST(NULL AS STRING)    AS row_hash,
    CAST(NULL AS TIMESTAMP) AS valid_from,
    CAST(NULL AS TIMESTAMP) AS snapshot_valid_from,
    CAST(NULL AS TIMESTAMP) AS valid_to,
    CAST(NULL AS BOOLEAN)   AS is_current

FROM onco_pharma_analytics.silver.stg_field_force_details
WHERE 1 = 0;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW stg_rep_scd AS
SELECT
    s.*,

    -- Check whether this rep has ever existed
    NOT EXISTS (
        SELECT 1
        FROM onco_pharma_analytics.silver.dim_field_force_master dim
        WHERE dim.rep_id = s.rep_id
    ) AS is_new_key,

    -- One load timestamp for this batch
    CURRENT_TIMESTAMP() AS load_ts

FROM (

    SELECT
        rep_id,
        rep_name,
        rep_role,
        hire_date,
        effective_start_date,
        effective_end_date,
        rep_status,

        -- Hash for change detection
        SHA2(
            TO_JSON(
                NAMED_STRUCT(
                    'rep_name',             rep_name,
                    'rep_role',             rep_role,
                    'hire_date',            hire_date,
                    'effective_start_date', effective_start_date,
                    'effective_end_date',   effective_end_date,
                    'rep_status',           rep_status
                ),
                MAP('ignoreNullFields', 'false')
            ),
            256
        ) AS row_hash

    FROM (

        SELECT
            rep_id,
            rep_name,
            rep_role,
            hire_date,
            effective_start_date,
            effective_end_date,
            rep_status
        FROM onco_pharma_analytics.silver.stg_field_force_details

        -- Keep the latest record per rep
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY rep_id
            ORDER BY effective_start_date DESC NULLS LAST
        ) = 1

    ) latest

) s;

-- COMMAND ----------

MERGE INTO onco_pharma_analytics.silver.dim_field_force_master AS tgt
USING (
    SELECT
        s.rep_id AS merge_key,
        s.*
    FROM stg_rep_scd s

    UNION ALL

    SELECT
        CAST(NULL AS STRING) AS merge_key,
        s.*
    FROM stg_rep_scd s
    JOIN onco_pharma_analytics.silver.dim_field_force_master dim
        ON dim.rep_id = s.rep_id
       AND dim.is_current = TRUE

    WHERE NOT (dim.row_hash <=> s.row_hash)

) AS src


ON tgt.rep_id = src.merge_key
AND tgt.is_current = TRUE



WHEN MATCHED
AND NOT (tgt.row_hash <=> src.row_hash)
THEN UPDATE SET
    tgt.valid_to    = src.load_ts,
    tgt.is_current  = FALSE



WHEN NOT MATCHED
THEN INSERT (

    dim_rep_sk,
    rep_id,
    rep_name,
    rep_role,
    hire_date,
    effective_start_date,
    effective_end_date,
    rep_status,
    row_hash,
    valid_from,
    snapshot_valid_from,
    valid_to,
    is_current

)

VALUES (

    -- Surrogate key: business key + version timestamp
    SHA2(
        TO_JSON(
            NAMED_STRUCT(
                'rep_id', src.rep_id,
                'valid_from',
                    CASE
                        WHEN src.is_new_key
                        THEN CAST('1900-01-01' AS TIMESTAMP)
                        ELSE src.load_ts
                    END
            ),
            MAP('ignoreNullFields', 'false')
        ),
        256
    ),

    src.rep_id,
    src.rep_name,
    src.rep_role,
    src.hire_date,
    src.effective_start_date,
    src.effective_end_date,
    src.rep_status,
    src.row_hash,

    CASE
        WHEN src.is_new_key
        THEN CAST('1900-01-01' AS TIMESTAMP)
        ELSE src.load_ts
    END,

    src.load_ts,

    NULL,
    TRUE

);



-- STEP 4: OPTIONAL - EXPIRE MISSING REPS
-- ENABLE ONLY FOR CONFIRMED FULL SOURCE EXTRACTS

-- WHEN NOT MATCHED BY SOURCE AND tgt.is_current = TRUE
-- THEN UPDATE SET
--     tgt.valid_to   = CURRENT_TIMESTAMP(),
--     tgt.is_current = FALSE