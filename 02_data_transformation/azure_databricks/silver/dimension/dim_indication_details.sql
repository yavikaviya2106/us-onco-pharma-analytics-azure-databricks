-- Databricks notebook source
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.dim_indication_details (
    dim_indication_sk         STRING    NOT NULL,
    indication_id             STRING    NOT NULL,

    dim_generic_sk            STRING,
    generic_id                STRING,
    generic_name              STRING,

    dim_brand_sk              STRING,
    brand_id                  STRING,

    dim_disease_sk            STRING,
    disease_id                STRING,
    disease_name              STRING,
    therapeutic_area          STRING,

    type_of_therapy           STRING,
    line_of_therapy           STRING,
    route_of_administration   STRING,
    standard_dose             DECIMAL(10,2),
    dose_unit                 STRING,
    dosage_form               STRING,
    units_per_day             DECIMAL(10,2),
    frequency                 STRING,
    treatment_duration        STRING,
    schedule_type             STRING,
    food_requirement          STRING,
    guideline_reference       STRING,

    source_updated_at         TIMESTAMP,
    source_system             STRING,

    _row_hash                 STRING,
    _dim_created_ts           TIMESTAMP,
    _dim_updated_ts           TIMESTAMP
)
USING DELTA
CLUSTER BY (indication_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed'  = 'true',
    'delta.enableDeletionVectors' = 'true'
);

-- COMMAND ----------

MERGE INTO onco_pharma_analytics.silver.dim_indication_details AS tgt
USING (
    WITH generic AS (
        SELECT dim_generic_sk, generic_id, generic_name
        FROM onco_pharma_analytics.silver.dim_generic_details
    ),

    brand AS (
        SELECT dim_brand_sk, brand_id
        FROM onco_pharma_analytics.silver.dim_brand_details
    ),

    disease AS (
        SELECT dim_disease_sk, disease_id, disease_name, therapeutic_area
        FROM onco_pharma_analytics.silver.dim_disease_details
    ),

    joined AS (
        SELECT
            md5(cast(indication.indication_id AS STRING))                   AS dim_indication_sk,
            indication.indication_id                                        AS indication_id,

            generic.dim_generic_sk                                          AS dim_generic_sk,
            indication.generic_id                                           AS generic_id,
            coalesce(indication.generic_name, generic.generic_name)         AS generic_name,

            brand.dim_brand_sk                                              AS dim_brand_sk,
            indication.brand_id                                             AS brand_id,

            disease.dim_disease_sk                                          AS dim_disease_sk,
            indication.disease_id                                           AS disease_id,
            coalesce(indication.disease_name, disease.disease_name)         AS disease_name,
            coalesce(indication.therapeutic_area, disease.therapeutic_area) AS therapeutic_area,

            indication.type_of_therapy                                      AS type_of_therapy,
            indication.line_of_therapy                                      AS line_of_therapy,
            indication.route_of_administration                              AS route_of_administration,
            indication.standard_dose                                        AS standard_dose,
            indication.dose_unit                                            AS dose_unit,
            indication.dosage_form                                          AS dosage_form,
            indication.units_per_day                                        AS units_per_day,
            indication.frequency                                            AS frequency,
            indication.treatment_duration                                   AS treatment_duration,
            indication.schedule_type                                        AS schedule_type,
            indication.food_requirement                                     AS food_requirement,
            indication.guideline_reference                                  AS guideline_reference,

            indication.source_updated_at                                    AS source_updated_at,
            indication.source_system                                        AS source_system
        FROM onco_pharma_analytics.silver.stg_indication_details AS indication
        LEFT JOIN generic ON indication.generic_id = generic.generic_id
        LEFT JOIN brand   ON indication.brand_id   = brand.brand_id
        LEFT JOIN disease ON indication.disease_id = disease.disease_id
    ),

    s AS (
        SELECT
            *,
            sha2(to_json(struct(
                    dim_generic_sk, generic_id, generic_name,
                    dim_brand_sk, brand_id,
                    dim_disease_sk, disease_id, disease_name, therapeutic_area,
                    type_of_therapy, line_of_therapy, route_of_administration,
                    standard_dose, dose_unit, dosage_form, units_per_day,
                    frequency, treatment_duration, schedule_type,
                    food_requirement, guideline_reference,
                    source_updated_at, source_system
                )), 256) AS _row_hash
        FROM joined
    ),

    t AS (
        SELECT indication_id, _row_hash
        FROM onco_pharma_analytics.silver.dim_indication_details
    )

    SELECT
        coalesce(s.indication_id, t.indication_id)                   AS merge_key,
        CASE WHEN s.indication_id IS NULL THEN 'D' ELSE 'U' END      AS merge_action,
        s.*
    FROM s
    FULL OUTER JOIN t
      ON s.indication_id = t.indication_id
    WHERE t.indication_id IS NULL
       OR s.indication_id IS NULL
       OR s._row_hash IS DISTINCT FROM t._row_hash
) AS src

ON tgt.indication_id = src.merge_key

WHEN MATCHED AND src.merge_action = 'D' THEN DELETE

WHEN MATCHED AND src.merge_action = 'U'
             AND tgt._row_hash IS DISTINCT FROM src._row_hash
    THEN UPDATE SET
    tgt.dim_generic_sk          = src.dim_generic_sk,
    tgt.generic_id              = src.generic_id,
    tgt.generic_name            = src.generic_name,
    tgt.dim_brand_sk            = src.dim_brand_sk,
    tgt.brand_id                = src.brand_id,
    tgt.dim_disease_sk          = src.dim_disease_sk,
    tgt.disease_id              = src.disease_id,
    tgt.disease_name            = src.disease_name,
    tgt.therapeutic_area        = src.therapeutic_area,
    tgt.type_of_therapy         = src.type_of_therapy,
    tgt.line_of_therapy         = src.line_of_therapy,
    tgt.route_of_administration = src.route_of_administration,
    tgt.standard_dose           = src.standard_dose,
    tgt.dose_unit               = src.dose_unit,
    tgt.dosage_form             = src.dosage_form,
    tgt.units_per_day           = src.units_per_day,
    tgt.frequency               = src.frequency,
    tgt.treatment_duration      = src.treatment_duration,
    tgt.schedule_type           = src.schedule_type,
    tgt.food_requirement        = src.food_requirement,
    tgt.guideline_reference     = src.guideline_reference,
    tgt.source_updated_at       = src.source_updated_at,
    tgt.source_system           = src.source_system,
    tgt._row_hash               = src._row_hash,
    tgt._dim_updated_ts         = current_timestamp()

WHEN NOT MATCHED AND src.merge_action = 'U'
    THEN INSERT (
        dim_indication_sk, indication_id,
        dim_generic_sk, generic_id, generic_name,
        dim_brand_sk, brand_id,
        dim_disease_sk, disease_id, disease_name, therapeutic_area,
        type_of_therapy, line_of_therapy, route_of_administration,
        standard_dose, dose_unit, dosage_form, units_per_day,
        frequency, treatment_duration, schedule_type,
        food_requirement, guideline_reference,
        source_updated_at, source_system,
        _row_hash, _dim_created_ts, _dim_updated_ts
    )
    VALUES (
        src.dim_indication_sk, src.indication_id,
        src.dim_generic_sk, src.generic_id, src.generic_name,
        src.dim_brand_sk, src.brand_id,
        src.dim_disease_sk, src.disease_id, src.disease_name, src.therapeutic_area,
        src.type_of_therapy, src.line_of_therapy, src.route_of_administration,
        src.standard_dose, src.dose_unit, src.dosage_form, src.units_per_day,
        src.frequency, src.treatment_duration, src.schedule_type,
        src.food_requirement, src.guideline_reference,
        src.source_updated_at, src.source_system,
        src._row_hash, current_timestamp(), current_timestamp()
    )
;