-- Databricks notebook source
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.dim_hco_master
USING DELTA
AS
SELECT
    CAST(NULL AS STRING) AS dim_hco_sk,
    onekey_hco_id,

    hco_npi,
    organization_legal_name AS hco_name,
    doing_business_as_name AS hco_business_name,
    organization_short_name AS hco_short_name,

    organization_status AS hco_status,
    organization_type AS hco_type,

    class_of_trade,
    class_of_trade_level_1,
    class_of_trade_level_2,
    class_of_trade_level_3,

    facility_type,
    specialty_type,

    hospital_flag,
    medical_group_flag,
    clinic_flag,
    nursing_home_flag,
    pharmacy_flag,
    gpo_flag,
    idn_flag,
    aco_flag,
    ipa_flag,

    corporate_parent_onekey_hco_id,
    corporate_parent_name,

    idn_onekey_hco_id,
    idn_name,

    primary_address_id,
    CAST(NULL AS STRING) AS dim_addr_sk,

    secondary_address_id,
    tertiary_address_id,

    outpatient_flag,
    inpatient_flag,
    academic_medical_center_flag,
    teaching_hospital_flag,

    effective_start_date,
    effective_end_date,

    CAST(NULL AS STRING) AS row_hash,
    CAST(NULL AS TIMESTAMP) AS valid_from,
    CAST(NULL AS TIMESTAMP) AS snapshot_valid_from,
    CAST(NULL AS TIMESTAMP) AS valid_to,
    CAST(NULL AS BOOLEAN) AS is_current

FROM onco_pharma_analytics.silver.stg_hco_onekey
WHERE 1 = 0;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW stg_hco_scd AS
SELECT
    s.*,
    a.dim_addr_sk,

    NOT EXISTS (
        SELECT 1
        FROM onco_pharma_analytics.silver.dim_hco_master dim
        WHERE dim.onekey_hco_id = s.onekey_hco_id
    ) AS is_new_key,

    CURRENT_TIMESTAMP() AS load_ts

FROM (

    SELECT
        latest.*,

        SHA2(
            TO_JSON(
                NAMED_STRUCT(
                    'hco_npi', hco_npi,
                    'organization_legal_name', organization_legal_name,
                    'doing_business_as_name', doing_business_as_name,
                    'organization_short_name', organization_short_name,
                    'organization_status', organization_status,
                    'organization_type', organization_type,
                    'class_of_trade', class_of_trade,
                    'class_of_trade_level_1', class_of_trade_level_1,
                    'class_of_trade_level_2', class_of_trade_level_2,
                    'class_of_trade_level_3', class_of_trade_level_3,
                    'facility_type', facility_type,
                    'specialty_type', specialty_type,
                    'hospital_flag', hospital_flag,
                    'medical_group_flag', medical_group_flag,
                    'clinic_flag', clinic_flag,
                    'nursing_home_flag', nursing_home_flag,
                    'pharmacy_flag', pharmacy_flag,
                    'gpo_flag', gpo_flag,
                    'idn_flag', idn_flag,
                    'aco_flag', aco_flag,
                    'ipa_flag', ipa_flag,
                    'corporate_parent_onekey_hco_id', corporate_parent_onekey_hco_id,
                    'corporate_parent_name', corporate_parent_name,
                    'idn_onekey_hco_id', idn_onekey_hco_id,
                    'idn_name', idn_name,
                    'primary_address_id', primary_address_id,
                    'secondary_address_id', secondary_address_id,
                    'tertiary_address_id', tertiary_address_id,
                    'outpatient_flag', outpatient_flag,
                    'inpatient_flag', inpatient_flag,
                    'academic_medical_center_flag', academic_medical_center_flag,
                    'teaching_hospital_flag', teaching_hospital_flag,
                    'effective_start_date', effective_start_date,
                    'effective_end_date', effective_end_date
                ),
                MAP('ignoreNullFields', 'false')
            ),
            256
        ) AS row_hash

    FROM (

        SELECT
            onekey_hco_id,
            hco_npi,
            organization_legal_name,
            doing_business_as_name,
            organization_short_name,
            organization_status,
            organization_type,

            class_of_trade,
            class_of_trade_level_1,
            class_of_trade_level_2,
            class_of_trade_level_3,

            facility_type,
            specialty_type,

            hospital_flag,
            medical_group_flag,
            clinic_flag,
            nursing_home_flag,
            pharmacy_flag,
            gpo_flag,
            idn_flag,
            aco_flag,
            ipa_flag,

            corporate_parent_onekey_hco_id,
            corporate_parent_name,

            idn_onekey_hco_id,
            idn_name,

            primary_address_id,
            secondary_address_id,
            tertiary_address_id,

            outpatient_flag,
            inpatient_flag,
            academic_medical_center_flag,
            teaching_hospital_flag,

            effective_start_date,
            effective_end_date

        FROM onco_pharma_analytics.silver.stg_hco_onekey

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY onekey_hco_id
            ORDER BY effective_start_date DESC NULLS LAST
        ) = 1

    ) latest

) s

LEFT JOIN onco_pharma_analytics.silver.dim_address_master a
    ON  a.address_id = s.primary_address_id
    AND a.is_current = TRUE;

-- COMMAND ----------

MERGE INTO onco_pharma_analytics.silver.dim_hco_master AS tgt
USING (

    SELECT
        s.onekey_hco_id AS merge_key,
        s.*
    FROM stg_hco_scd s

    UNION ALL

    SELECT
        CAST(NULL AS STRING) AS merge_key,
        s.*
    FROM stg_hco_scd s

    INNER JOIN onco_pharma_analytics.silver.dim_hco_master t
        ON  t.onekey_hco_id = s.onekey_hco_id
        AND t.is_current = TRUE

    WHERE NOT (t.row_hash <=> s.row_hash)

) AS src

ON  tgt.onekey_hco_id = src.merge_key
AND tgt.is_current = TRUE



WHEN MATCHED
    AND NOT (tgt.row_hash <=> src.row_hash)
THEN UPDATE SET
    tgt.valid_to = src.load_ts,
    tgt.is_current = FALSE


WHEN NOT MATCHED
THEN INSERT (
    dim_hco_sk,
    onekey_hco_id,

    hco_npi,
    hco_name,
    hco_business_name,
    hco_short_name,

    hco_status,
    hco_type,

    class_of_trade,
    class_of_trade_level_1,
    class_of_trade_level_2,
    class_of_trade_level_3,

    facility_type,
    specialty_type,

    hospital_flag,
    medical_group_flag,
    clinic_flag,
    nursing_home_flag,
    pharmacy_flag,
    gpo_flag,
    idn_flag,
    aco_flag,
    ipa_flag,

    corporate_parent_onekey_hco_id,
    corporate_parent_name,

    idn_onekey_hco_id,
    idn_name,

    primary_address_id,
    dim_addr_sk,

    secondary_address_id,
    tertiary_address_id,

    outpatient_flag,
    inpatient_flag,
    academic_medical_center_flag,
    teaching_hospital_flag,

    effective_start_date,
    effective_end_date,

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
                'onekey_hco_id', src.onekey_hco_id,
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

    src.onekey_hco_id,

    src.hco_npi,
    src.organization_legal_name,
    src.doing_business_as_name,
    src.organization_short_name,

    src.organization_status,
    src.organization_type,

    src.class_of_trade,
    src.class_of_trade_level_1,
    src.class_of_trade_level_2,
    src.class_of_trade_level_3,

    src.facility_type,
    src.specialty_type,

    src.hospital_flag,
    src.medical_group_flag,
    src.clinic_flag,
    src.nursing_home_flag,
    src.pharmacy_flag,
    src.gpo_flag,
    src.idn_flag,
    src.aco_flag,
    src.ipa_flag,

    src.corporate_parent_onekey_hco_id,
    src.corporate_parent_name,

    src.idn_onekey_hco_id,
    src.idn_name,

    src.primary_address_id,
    src.dim_addr_sk,

    src.secondary_address_id,
    src.tertiary_address_id,

    src.outpatient_flag,
    src.inpatient_flag,
    src.academic_medical_center_flag,
    src.teaching_hospital_flag,

    src.effective_start_date,
    src.effective_end_date,

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



-- OPTIONAL - EXPIRE MISSING ADDRESSES
-- ENABLE ONLY FOR CONFIRMED FULL SOURCE EXTRACTS

-- WHEN NOT MATCHED BY SOURCE AND tgt.is_current = TRUE
-- THEN UPDATE SET
--     tgt.valid_to   = CURRENT_TIMESTAMP(),
--     tgt.is_current = FALSE