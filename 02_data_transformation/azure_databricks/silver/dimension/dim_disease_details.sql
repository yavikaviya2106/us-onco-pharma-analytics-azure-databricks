-- Databricks notebook source
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.dim_disease_details (
    dim_disease_sk          STRING    NOT NULL,
    disease_id              STRING    NOT NULL,
    disease_name            STRING,
    therapeutic_area        STRING,

    cancer_type             STRING,
    biomarker_required      BOOLEAN,
    biomarker_name          STRING,
    diagnosis_specialties   STRING,
    prescribing_specialty   STRING,

    source_updated_at       TIMESTAMP,
    source_system           STRING,

    _row_hash               STRING,
    _dim_created_ts         TIMESTAMP,
    _dim_updated_ts         TIMESTAMP
)
USING DELTA
CLUSTER BY (disease_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed'  = 'true',
    'delta.enableDeletionVectors' = 'true'
);

-- COMMAND ----------

MERGE INTO onco_pharma_analytics.silver.dim_disease_details AS tgt
USING (
    WITH s AS (
        SELECT
            md5(cast(disease_id AS STRING)) AS dim_disease_sk,
            disease_id,
            disease_name,
            therapeutic_area,

            cancer_type,
            biomarker_required,
            biomarker_name,
            diagnosis_specialties,
            prescribing_specialty,

            source_updated_at,
            source_system,

            sha2(to_json(struct(
                    disease_name, therapeutic_area,
                    cancer_type, biomarker_required, biomarker_name,
                    diagnosis_specialties, prescribing_specialty,
                    source_updated_at, source_system
                )), 256) AS _row_hash
        FROM onco_pharma_analytics.silver.stg_disease_details
    ),

    t AS (
        SELECT disease_id, _row_hash
        FROM onco_pharma_analytics.silver.dim_disease_details
    )

    SELECT
        coalesce(s.disease_id, t.disease_id)                     AS merge_key,
        CASE WHEN s.disease_id IS NULL THEN 'D' ELSE 'U' END     AS merge_action,
        s.*
    FROM s
    FULL OUTER JOIN t
      ON s.disease_id = t.disease_id
    WHERE t.disease_id IS NULL
       OR s.disease_id IS NULL
       OR s._row_hash IS DISTINCT FROM t._row_hash
) AS src

ON tgt.disease_id = src.merge_key

WHEN MATCHED AND src.merge_action = 'D' THEN DELETE

WHEN MATCHED AND src.merge_action = 'U'
             AND tgt._row_hash IS DISTINCT FROM src._row_hash
    THEN UPDATE SET
    tgt.disease_name          = src.disease_name,
    tgt.therapeutic_area      = src.therapeutic_area,
    tgt.cancer_type           = src.cancer_type,
    tgt.biomarker_required    = src.biomarker_required,
    tgt.biomarker_name        = src.biomarker_name,
    tgt.diagnosis_specialties = src.diagnosis_specialties,
    tgt.prescribing_specialty = src.prescribing_specialty,
    tgt.source_updated_at     = src.source_updated_at,
    tgt.source_system         = src.source_system,
    tgt._row_hash             = src._row_hash,
    tgt._dim_updated_ts       = current_timestamp()

WHEN NOT MATCHED AND src.merge_action = 'U'
    THEN INSERT (
        dim_disease_sk, disease_id, disease_name, therapeutic_area,
        cancer_type, biomarker_required, biomarker_name,
        diagnosis_specialties, prescribing_specialty,
        source_updated_at, source_system,
        _row_hash, _dim_created_ts, _dim_updated_ts
    )
    VALUES (
        src.dim_disease_sk, src.disease_id, src.disease_name, src.therapeutic_area,
        src.cancer_type, src.biomarker_required, src.biomarker_name,
        src.diagnosis_specialties, src.prescribing_specialty,
        src.source_updated_at, src.source_system,
        src._row_hash, current_timestamp(), current_timestamp()
    )
;