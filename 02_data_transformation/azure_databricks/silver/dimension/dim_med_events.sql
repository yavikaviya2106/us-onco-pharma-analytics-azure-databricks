-- Databricks notebook source
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.dim_med_events (
    dim_med_eve_sk              STRING    NOT NULL,
    medical_event_id            STRING    NOT NULL,

    event_name                  STRING,
    event_type                  STRING,
    event_category              STRING,
    event_status                STRING,
    event_description           STRING,
    event_format                STRING,

    event_start_date            DATE,
    event_end_date              DATE,
    event_start_time            STRING,
    event_end_time              STRING,
    timezone                    STRING,

    venue_name                  STRING,
    venue_address               STRING,
    city                        STRING,
    state                       STRING,
    country                     STRING,

    virtual_platform            STRING,
    virtual_event_url           STRING,

    therapeutic_area            STRING,
    disease_id                  STRING,
    dim_disease_sk              STRING,

    primary_product_id          STRING,
    dim_prod_sk                 STRING,

    indication_id               STRING,
    dim_indication_sk           STRING,

    sponsor_name                STRING,
    organizing_department       STRING,
    event_owner_id              STRING,
    event_coordinator           STRING,
    agency_name                 STRING,

    primary_speaker_npi         STRING,

    compliance_required_flag    STRING,
    compliance_status           STRING,
    approval_status             STRING,
    approval_date               DATE,
    approval_reference_id       STRING,

    source_updated_at           TIMESTAMP,
    source_system               STRING,

    _row_hash                   STRING,
    _dim_created_ts             TIMESTAMP,
    _dim_updated_ts             TIMESTAMP
)
USING DELTA
CLUSTER BY (medical_event_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed'  = 'true',
    'delta.enableDeletionVectors' = 'true'
);

-- COMMAND ----------

MERGE INTO onco_pharma_analytics.silver.dim_med_events AS tgt
USING (
    WITH disease AS (
        SELECT dim_disease_sk, disease_id
        FROM onco_pharma_analytics.silver.dim_disease_details
    ),

    indication AS (
        SELECT dim_indication_sk, indication_id
        FROM onco_pharma_analytics.silver.dim_indication_details
    ),

    prod AS (
        SELECT dim_prod_sk, product_id
        FROM onco_pharma_analytics.silver.dim_product_details
    ),

    joined AS (
        SELECT
            md5(cast(events.medical_event_id AS STRING)) AS dim_med_eve_sk,
            events.medical_event_id             AS medical_event_id,

            events.event_name                   AS event_name,
            events.event_type                   AS event_type,
            events.event_category               AS event_category,
            events.event_status                 AS event_status,
            events.event_description            AS event_description,
            events.event_format                 AS event_format,

            events.event_start_date             AS event_start_date,
            events.event_end_date               AS event_end_date,
            events.event_start_time             AS event_start_time,
            events.event_end_time               AS event_end_time,
            events.timezone                     AS timezone,

            events.venue_name                   AS venue_name,
            events.venue_address                AS venue_address,
            events.city                         AS city,
            events.state                        AS state,
            events.country                      AS country,

            events.virtual_platform             AS virtual_platform,
            events.virtual_event_url            AS virtual_event_url,

            events.therapeutic_area             AS therapeutic_area,
            events.disease_id                   AS disease_id,
            disease.dim_disease_sk              AS dim_disease_sk,

            events.primary_product_id           AS primary_product_id,
            prod.dim_prod_sk                    AS dim_prod_sk,

            events.indication_id                AS indication_id,
            indication.dim_indication_sk        AS dim_indication_sk,

            events.sponsor_name                 AS sponsor_name,
            events.organizing_department        AS organizing_department,
            events.event_owner_id               AS event_owner_id,
            events.event_coordinator            AS event_coordinator,
            events.agency_name                  AS agency_name,

            events.primary_speaker_npi          AS primary_speaker_npi,

            events.compliance_required_flag     AS compliance_required_flag,
            events.compliance_status            AS compliance_status,
            events.approval_status              AS approval_status,
            events.approval_date                AS approval_date,
            events.approval_reference_id        AS approval_reference_id,

            events.source_updated_at            AS source_updated_at,
            events.source_system                AS source_system
        FROM onco_pharma_analytics.silver.stg_medical_events AS events
        LEFT JOIN disease    ON events.disease_id         = disease.disease_id
        LEFT JOIN indication ON events.indication_id      = indication.indication_id
        LEFT JOIN prod       ON events.primary_product_id = prod.product_id
    ),

    s AS (
        SELECT
            *,
            sha2(to_json(struct(
                    event_name, event_type, event_category, event_status,
                    event_description, event_format,
                    event_start_date, event_end_date, event_start_time, event_end_time, timezone,
                    venue_name, venue_address, city, state, country,
                    virtual_platform, virtual_event_url,
                    therapeutic_area, disease_id, dim_disease_sk,
                    primary_product_id, dim_prod_sk,
                    indication_id, dim_indication_sk,
                    sponsor_name, organizing_department, event_owner_id,
                    event_coordinator, agency_name,
                    primary_speaker_npi,
                    compliance_required_flag, compliance_status,
                    approval_status, approval_date, approval_reference_id,
                    source_updated_at, source_system
                )), 256) AS _row_hash
        FROM joined
    ),

    t AS (
        SELECT medical_event_id, _row_hash
        FROM onco_pharma_analytics.silver.dim_med_events
    )

    SELECT
        s.medical_event_id                                       AS merge_key,
        'U'                                                      AS merge_action,
        s.*
    FROM s
    LEFT JOIN t
      ON s.medical_event_id = t.medical_event_id
    WHERE t.medical_event_id IS NULL
       OR s._row_hash IS DISTINCT FROM t._row_hash
) AS src

ON tgt.medical_event_id = src.merge_key

WHEN MATCHED AND src.merge_action = 'U'
             AND tgt._row_hash IS DISTINCT FROM src._row_hash
    THEN UPDATE SET
    tgt.event_name               = src.event_name,
    tgt.event_type               = src.event_type,
    tgt.event_category           = src.event_category,
    tgt.event_status             = src.event_status,
    tgt.event_description        = src.event_description,
    tgt.event_format             = src.event_format,
    tgt.event_start_date         = src.event_start_date,
    tgt.event_end_date           = src.event_end_date,
    tgt.event_start_time         = src.event_start_time,
    tgt.event_end_time           = src.event_end_time,
    tgt.timezone                 = src.timezone,
    tgt.venue_name               = src.venue_name,
    tgt.venue_address            = src.venue_address,
    tgt.city                     = src.city,
    tgt.state                    = src.state,
    tgt.country                  = src.country,
    tgt.virtual_platform         = src.virtual_platform,
    tgt.virtual_event_url        = src.virtual_event_url,
    tgt.therapeutic_area         = src.therapeutic_area,
    tgt.disease_id               = src.disease_id,
    tgt.dim_disease_sk           = src.dim_disease_sk,
    tgt.primary_product_id       = src.primary_product_id,
    tgt.dim_prod_sk              = src.dim_prod_sk,
    tgt.indication_id            = src.indication_id,
    tgt.dim_indication_sk        = src.dim_indication_sk,
    tgt.sponsor_name             = src.sponsor_name,
    tgt.organizing_department    = src.organizing_department,
    tgt.event_owner_id           = src.event_owner_id,
    tgt.event_coordinator        = src.event_coordinator,
    tgt.agency_name              = src.agency_name,
    tgt.primary_speaker_npi      = src.primary_speaker_npi,
    tgt.compliance_required_flag = src.compliance_required_flag,
    tgt.compliance_status        = src.compliance_status,
    tgt.approval_status          = src.approval_status,
    tgt.approval_date            = src.approval_date,
    tgt.approval_reference_id    = src.approval_reference_id,
    tgt.source_updated_at        = src.source_updated_at,
    tgt.source_system            = src.source_system,
    tgt._row_hash                = src._row_hash,
    tgt._dim_updated_ts          = current_timestamp()

WHEN NOT MATCHED AND src.merge_action = 'U'
    THEN INSERT (
        dim_med_eve_sk, medical_event_id,
        event_name, event_type, event_category, event_status,
        event_description, event_format,
        event_start_date, event_end_date, event_start_time, event_end_time, timezone,
        venue_name, venue_address, city, state, country,
        virtual_platform, virtual_event_url,
        therapeutic_area, disease_id, dim_disease_sk,
        primary_product_id, dim_prod_sk,
        indication_id, dim_indication_sk,
        sponsor_name, organizing_department, event_owner_id,
        event_coordinator, agency_name,
        primary_speaker_npi,
        compliance_required_flag, compliance_status,
        approval_status, approval_date, approval_reference_id,
        source_updated_at, source_system,
        _row_hash, _dim_created_ts, _dim_updated_ts
    )
    VALUES (
        src.dim_med_eve_sk, src.medical_event_id,
        src.event_name, src.event_type, src.event_category, src.event_status,
        src.event_description, src.event_format,
        src.event_start_date, src.event_end_date, src.event_start_time, src.event_end_time, src.timezone,
        src.venue_name, src.venue_address, src.city, src.state, src.country,
        src.virtual_platform, src.virtual_event_url,
        src.therapeutic_area, src.disease_id, src.dim_disease_sk,
        src.primary_product_id, src.dim_prod_sk,
        src.indication_id, src.dim_indication_sk,
        src.sponsor_name, src.organizing_department, src.event_owner_id,
        src.event_coordinator, src.agency_name,
        src.primary_speaker_npi,
        src.compliance_required_flag, src.compliance_status,
        src.approval_status, src.approval_date, src.approval_reference_id,
        src.source_updated_at, src.source_system,
        src._row_hash, current_timestamp(), current_timestamp()
    )
;