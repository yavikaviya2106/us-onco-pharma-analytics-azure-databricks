-- Databricks notebook source
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.dim_hcp_master
USING DELTA
AS
SELECT
    CAST(NULL AS STRING) AS dim_hcp_sk,
    onekey_hcp_id,

    npi AS hcp_npi,
    full_name,
    gender,

    professional_status,
    profession_type,
    credentials,

    primary_specialty,
    primary_specialty_code,
    secondary_specialty,
    sub_specialty,

    prescriber_flag,
    prescribing_status,

    pdrp_flag,
    patient_facing_flag,
    hospital_based_flag,
    academic_flag,
    key_decision_maker_flag,
    key_opinion_leader_flag,
    sample_eligibility_flag,
    controlled_sample_eligibility,
    noncontrolled_sample_eligibility,

    primary_employer_hco_id,
    CAST(NULL AS STRING) AS dim_hco_sk,

    primary_affiliation_id,
    CAST(NULL AS STRING) AS bridge_hcp_hco_sk,
    affiliation_count,

    communication_preference,
    channel_preference,
    language_preference,

    CAST(NULL AS STRING) AS row_hash,
    CAST(NULL AS TIMESTAMP) AS valid_from,
    CAST(NULL AS TIMESTAMP) AS snapshot_valid_from,
    CAST(NULL AS TIMESTAMP) AS valid_to,
    CAST(NULL AS BOOLEAN) AS is_current

FROM onco_pharma_analytics.silver.stg_hcp_onekey
WHERE 1 = 0;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW stg_hcp_scd AS
SELECT
    h.*,

    hco.dim_hco_sk,

    aff.bridge_hcp_hco_sk,

    NOT EXISTS (
        SELECT 1
        FROM onco_pharma_analytics.silver.dim_hcp_master dim
        WHERE dim.onekey_hcp_id = h.onekey_hcp_id
    ) AS is_new_key

FROM (

    SELECT
        source_data.*,

        SHA2(
            TO_JSON(
                NAMED_STRUCT(
                    'npi', npi,
                    'full_name', full_name,
                    'gender', gender,
                    'professional_status', professional_status,
                    'profession_type', profession_type,
                    'credentials', credentials,
                    'primary_specialty', primary_specialty,
                    'primary_specialty_code', primary_specialty_code,
                    'secondary_specialty', secondary_specialty,
                    'sub_specialty', sub_specialty,
                    'prescriber_flag', prescriber_flag,
                    'prescribing_status', prescribing_status,
                    'pdrp_flag', pdrp_flag,
                    'patient_facing_flag', patient_facing_flag,
                    'hospital_based_flag', hospital_based_flag,
                    'academic_flag', academic_flag,
                    'key_decision_maker_flag', key_decision_maker_flag,
                    'key_opinion_leader_flag', key_opinion_leader_flag,
                    'sample_eligibility_flag', sample_eligibility_flag,
                    'controlled_sample_eligibility', controlled_sample_eligibility,
                    'noncontrolled_sample_eligibility', noncontrolled_sample_eligibility,
                    'primary_employer_hco_id', primary_employer_hco_id,
                    'primary_affiliation_id', primary_affiliation_id,
                    'affiliation_count', affiliation_count,
                    'communication_preference', communication_preference,
                    'channel_preference', channel_preference,
                    'language_preference', language_preference
                ),
                MAP('ignoreNullFields', 'false')
            ),
            256
        ) AS row_hash,

        CURRENT_TIMESTAMP() AS load_ts

    FROM onco_pharma_analytics.silver.stg_hcp_onekey source_data

) h


LEFT JOIN onco_pharma_analytics.silver.dim_hco_master hco
    ON  hco.onekey_hco_id = h.primary_employer_hco_id
    AND hco.is_current = TRUE

LEFT JOIN onco_pharma_analytics.silver.bridge_hcp_hco_affln aff
    ON  aff.affiliation_id = h.primary_affiliation_id
    AND h.load_ts >= aff.effective_start_date
    AND (
        aff.effective_end_date IS NULL
        OR h.load_ts < aff.effective_end_date
    );

-- COMMAND ----------

MERGE INTO onco_pharma_analytics.silver.dim_hcp_master AS tgt
USING (

    SELECT
        s.onekey_hcp_id AS merge_key,
        s.*
    FROM stg_hcp_scd s

    UNION ALL

    SELECT
        CAST(NULL AS STRING) AS merge_key,
        s.*
    FROM stg_hcp_scd s

    INNER JOIN onco_pharma_analytics.silver.dim_hcp_master t
        ON  t.onekey_hcp_id = s.onekey_hcp_id
        AND t.is_current = TRUE

    WHERE NOT (t.row_hash <=> s.row_hash)

) AS src

ON  tgt.onekey_hcp_id = src.merge_key
AND tgt.is_current = TRUE



WHEN MATCHED
    AND NOT (tgt.row_hash <=> src.row_hash)
THEN UPDATE SET
    tgt.valid_to = src.load_ts,
    tgt.is_current = FALSE


WHEN NOT MATCHED
THEN INSERT (

    dim_hcp_sk,
    onekey_hcp_id,

    hcp_npi,
    full_name,
    gender,

    professional_status,
    profession_type,
    credentials,

    primary_specialty,
    primary_specialty_code,
    secondary_specialty,
    sub_specialty,

    prescriber_flag,
    prescribing_status,

    pdrp_flag,
    patient_facing_flag,
    hospital_based_flag,
    academic_flag,
    key_decision_maker_flag,
    key_opinion_leader_flag,
    sample_eligibility_flag,
    controlled_sample_eligibility,
    noncontrolled_sample_eligibility,

    primary_employer_hco_id,
    dim_hco_sk,

    primary_affiliation_id,
    bridge_hcp_hco_sk,
    affiliation_count,

    communication_preference,
    channel_preference,
    language_preference,

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
                'onekey_hcp_id', src.onekey_hcp_id,
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

    src.onekey_hcp_id,

    src.npi,
    src.full_name,
    src.gender,

    src.professional_status,
    src.profession_type,
    src.credentials,

    src.primary_specialty,
    src.primary_specialty_code,
    src.secondary_specialty,
    src.sub_specialty,

    src.prescriber_flag,
    src.prescribing_status,

    src.pdrp_flag,
    src.patient_facing_flag,
    src.hospital_based_flag,
    src.academic_flag,
    src.key_decision_maker_flag,
    src.key_opinion_leader_flag,
    src.sample_eligibility_flag,
    src.controlled_sample_eligibility,
    src.noncontrolled_sample_eligibility,

    src.primary_employer_hco_id,
    src.dim_hco_sk,

    src.primary_affiliation_id,
    src.bridge_hcp_hco_sk,
    src.affiliation_count,

    src.communication_preference,
    src.channel_preference,
    src.language_preference,

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



-- OPTIONAL - Expire missing HCPs
-- Only if SOURCE is a confirmed FULL extract every run.

-- WHEN NOT MATCHED BY SOURCE AND tgt.is_current = TRUE
-- THEN UPDATE SET
--     tgt.valid_to   = CURRENT_TIMESTAMP(),
--     tgt.is_current = FALSE