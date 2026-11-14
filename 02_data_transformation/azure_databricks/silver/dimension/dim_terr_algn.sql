-- Databricks notebook source
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.dim_terr_algn
USING DELTA
AS
SELECT
    CAST(NULL AS STRING) AS dim_terr_sk,

    territory_id,
    territory_name,
    district_id,
    district_name,
    region_id,
    region_name,
    area_id,
    area_name,
    nation_id,
    nation_name,

    CAST(NULL AS STRING) AS row_hash,
    CAST(NULL AS TIMESTAMP) AS valid_from,
    CAST(NULL AS TIMESTAMP) AS snapshot_valid_from,
    CAST(NULL AS TIMESTAMP) AS valid_to,
    CAST(NULL AS BOOLEAN) AS is_current

FROM onco_pharma_analytics.silver.stg_terr_algn
WHERE 1 = 0;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW stg_terr_scd AS
SELECT
    s.*,

    NOT EXISTS (
        SELECT 1
        FROM onco_pharma_analytics.silver.dim_terr_algn dim
        WHERE dim.territory_id = s.territory_id
    ) AS is_new_key,

    CURRENT_TIMESTAMP() AS load_ts

FROM (

    SELECT
        territory_id, territory_name,
        district_id, district_name,
        region_id, region_name,
        area_id, area_name,
        nation_id, nation_name,

        SHA2(
            TO_JSON(
                NAMED_STRUCT(
                    'territory_name', territory_name,
                    'district_id', district_id,
                    'district_name', district_name,
                    'region_id', region_id,
                    'region_name', region_name,
                    'area_id', area_id,
                    'area_name', area_name,
                    'nation_id', nation_id,
                    'nation_name', nation_name
                ),
                MAP('ignoreNullFields', 'false')
            ),
            256
        ) AS row_hash

    FROM onco_pharma_analytics.silver.stg_terr_algn

) s;

-- COMMAND ----------

MERGE INTO onco_pharma_analytics.silver.dim_terr_algn AS tgt
USING (


    SELECT
        s.territory_id AS merge_key,
        s.*
    FROM stg_terr_scd s

    UNION ALL

    SELECT
        CAST(NULL AS STRING) AS merge_key,
        s.*
    FROM stg_terr_scd s
    INNER JOIN onco_pharma_analytics.silver.dim_terr_algn t
        ON  t.territory_id = s.territory_id
        AND t.is_current = TRUE
    WHERE NOT (t.row_hash <=> s.row_hash)

) AS src

ON  tgt.territory_id = src.merge_key
AND tgt.is_current = TRUE


WHEN MATCHED
    AND NOT (tgt.row_hash <=> src.row_hash)
THEN UPDATE SET
    tgt.valid_to = src.load_ts,
    tgt.is_current = FALSE


WHEN NOT MATCHED
THEN INSERT (
    dim_terr_sk,
    territory_id, territory_name,
    district_id, district_name,
    region_id, region_name,
    area_id, area_name,
    nation_id, nation_name,

    row_hash,
    valid_from,
    snapshot_valid_from,
    valid_to,
    is_current
)

VALUES (

    SHA2(
        TO_JSON(
            NAMED_STRUCT(
                'territory_id', src.territory_id,
                'valid_from',
                    CASE
                        WHEN src.is_new_key
                            THEN CAST('1900-01-01' AS TIMESTAMP)
                        ELSE src.load_ts
                    END
            )
        ),
        256
    ),

    src.territory_id, src.territory_name,
    src.district_id, src.district_name,
    src.region_id, src.region_name,
    src.area_id, src.area_name,
    src.nation_id, src.nation_name,
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



-- OPTIONAL - Expire missing territories
-- Only if SOURCE is a confirmed FULL extract every run.

-- WHEN NOT MATCHED BY SOURCE AND tgt.is_current = TRUE
-- THEN UPDATE SET
--     tgt.valid_to   = CURRENT_TIMESTAMP(),
--     tgt.is_current = FALSE