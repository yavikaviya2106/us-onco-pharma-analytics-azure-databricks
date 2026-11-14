-- Databricks notebook source
-- Target table
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.bridge_hcp_hco_affln (
    bridge_hcp_hco_sk                  STRING    NOT NULL,
    affiliation_id                     STRING    NOT NULL,

    onekey_hcp_id                      STRING,
    onekey_hco_id                      STRING,
    address_id                         STRING,

    primary_affiliation_flag           BOOLEAN,
    best_affiliation_flag              BOOLEAN,
    effective_start_date               DATE,
    effective_end_date                 DATE,
    status                             STRING,

    relationship_type                  STRING,
    affiliation_type                   STRING,
    affiliation_sub_type               STRING,
    affiliation_role                   STRING,
    affiliation_rank                   INT,
    employment_flag                    BOOLEAN,
    employment_status                  STRING,

    department_at_hco                  STRING,
    specialty_at_hco                   STRING,

    practice_location_flag             BOOLEAN,
    hospital_affiliation_flag          BOOLEAN,
    medical_group_affiliation_flag     BOOLEAN,
    idn_affiliation_flag               BOOLEAN,
    gpo_affiliation_flag               BOOLEAN,
    corporate_parent_affiliation_flag  BOOLEAN,
    out_of_state_flag                  BOOLEAN,
    source_type                        STRING,
    verification_date                  DATE,

    --source audit columns
    source_updated_at                   TIMESTAMP,
    source_system                       STRING,

    -- audit / change-detection columns
    _row_hash                          STRING,
    _dim_created_ts                    TIMESTAMP,
    _dim_updated_ts                    TIMESTAMP
)
USING DELTA
CLUSTER BY (affiliation_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed'  = 'true',
    'delta.enableDeletionVectors' = 'true'   -- updates/deletes mark old rows instead of rewriting whole files
);

-- COMMAND ----------

-- 2. Type 1 MERGE: insert new, overwrite changed, delete removed
MERGE INTO onco_pharma_analytics.silver.bridge_hcp_hco_affln AS tgt
USING (
    WITH s AS (
        SELECT
            md5(cast(affiliation_id AS STRING)) AS bridge_hcp_hco_sk,
            affiliation_id,

            onekey_hcp_id,
            onekey_hco_id,
            address_id,

            primary_affiliation_flag,
            best_affiliation_flag,
            effective_start_date,
            effective_end_date,
            status,

            relationship_type,
            affiliation_type,
            affiliation_sub_type,
            affiliation_role,
            affiliation_rank,
            employment_flag,
            employment_status,

            department_at_hco,
            specialty_at_hco,

            practice_location_flag,
            hospital_affiliation_flag,
            medical_group_affiliation_flag,
            idn_affiliation_flag,
            gpo_affiliation_flag,
            corporate_parent_affiliation_flag,
            out_of_state_flag,
            source_type,
            verification_date,

            source_updated_at,
            source_system,

            -- to_json keeps column names, so NULLs in different columns never collide
            sha2(to_json(struct(
                    onekey_hcp_id, onekey_hco_id, address_id,
                    primary_affiliation_flag, best_affiliation_flag,
                    effective_start_date, effective_end_date, status,
                    relationship_type, affiliation_type, affiliation_sub_type,
                    affiliation_role, affiliation_rank, employment_flag, employment_status,
                    department_at_hco, specialty_at_hco,
                    practice_location_flag, hospital_affiliation_flag,
                    medical_group_affiliation_flag, idn_affiliation_flag,
                    gpo_affiliation_flag, corporate_parent_affiliation_flag,
                    out_of_state_flag, source_type, verification_date,
                    source_updated_at,source_system
                )), 256) AS _row_hash
        FROM onco_pharma_analytics.silver.stg_hcp_hco_affiliation          
    ),

    t AS (
        SELECT affiliation_id, _row_hash
        FROM onco_pharma_analytics.silver.bridge_hcp_hco_affln
    )

    SELECT
        coalesce(s.affiliation_id, t.affiliation_id)             AS merge_key,
        CASE WHEN s.affiliation_id IS NULL THEN 'D' ELSE 'U' END   AS merge_action,
        s.*
    FROM s 
    FULL OUTER JOIN t t
      ON s.affiliation_id = t.affiliation_id
    WHERE t.affiliation_id IS NULL                          -- new in staging
       OR s.affiliation_id IS NULL                          -- removed from staging
       OR s._row_hash IS DISTINCT FROM t._row_hash         -- changed (NULL-safe)
) AS src

ON tgt.affiliation_id = src.merge_key

-- gone from staging: deleted (Type 1)
WHEN MATCHED AND src.merge_action = 'D' THEN DELETE

-- changed rows: overwrite in place (Type 1)
WHEN MATCHED AND src.merge_action = 'U'
             AND tgt._row_hash IS DISTINCT FROM src._row_hash 
    THEN UPDATE SET
    tgt.onekey_hcp_id                     = src.onekey_hcp_id,
    tgt.onekey_hco_id                     = src.onekey_hco_id,
    tgt.address_id                        = src.address_id,
    tgt.primary_affiliation_flag          = src.primary_affiliation_flag,
    tgt.best_affiliation_flag             = src.best_affiliation_flag,
    tgt.effective_start_date              = src.effective_start_date,
    tgt.effective_end_date                = src.effective_end_date,
    tgt.status                            = src.status,
    tgt.relationship_type                 = src.relationship_type,
    tgt.affiliation_type                  = src.affiliation_type,
    tgt.affiliation_sub_type              = src.affiliation_sub_type,
    tgt.affiliation_role                  = src.affiliation_role,
    tgt.affiliation_rank                  = src.affiliation_rank,
    tgt.employment_flag                   = src.employment_flag,
    tgt.employment_status                 = src.employment_status,
    tgt.department_at_hco                 = src.department_at_hco,
    tgt.specialty_at_hco                  = src.specialty_at_hco,
    tgt.practice_location_flag            = src.practice_location_flag,
    tgt.hospital_affiliation_flag         = src.hospital_affiliation_flag,
    tgt.medical_group_affiliation_flag    = src.medical_group_affiliation_flag,
    tgt.idn_affiliation_flag              = src.idn_affiliation_flag,
    tgt.gpo_affiliation_flag              = src.gpo_affiliation_flag,
    tgt.corporate_parent_affiliation_flag = src.corporate_parent_affiliation_flag,
    tgt.out_of_state_flag                 = src.out_of_state_flag,
    tgt.source_type                       = src.source_type,
    tgt.verification_date                 = src.verification_date,
    tgt.source_updated_at                 = src.source_updated_at,
    tgt.source_system                     = src.source_system,
    tgt._row_hash                         = src._row_hash,
    tgt._dim_updated_ts                   = current_timestamp()
    

-- new affiliations: insert
WHEN NOT MATCHED AND src.merge_action = 'U' 
    THEN INSERT (
        bridge_hcp_hco_sk, affiliation_id,
        onekey_hcp_id, onekey_hco_id, address_id,
        primary_affiliation_flag, best_affiliation_flag,
        effective_start_date, effective_end_date, status,
        relationship_type, affiliation_type, affiliation_sub_type,
        affiliation_role, affiliation_rank, employment_flag, employment_status,
        department_at_hco, specialty_at_hco,
        practice_location_flag, hospital_affiliation_flag,
        medical_group_affiliation_flag, idn_affiliation_flag,
        gpo_affiliation_flag, corporate_parent_affiliation_flag,
        out_of_state_flag, source_type, verification_date,
        source_updated_at, source_system,
        _row_hash, _dim_created_ts, _dim_updated_ts
    ) 
    VALUES (
        src.bridge_hcp_hco_sk, src.affiliation_id,
        src.onekey_hcp_id, src.onekey_hco_id, src.address_id,
        src.primary_affiliation_flag, src.best_affiliation_flag,
        src.effective_start_date, src.effective_end_date, src.status,
        src.relationship_type, src.affiliation_type, src.affiliation_sub_type,
        src.affiliation_role, src.affiliation_rank, src.employment_flag, src.employment_status,
        src.department_at_hco, src.specialty_at_hco,
        src.practice_location_flag, src.hospital_affiliation_flag,
        src.medical_group_affiliation_flag, src.idn_affiliation_flag,
        src.gpo_affiliation_flag, src.corporate_parent_affiliation_flag,
        src.out_of_state_flag, src.source_type, src.verification_date,
        src.source_updated_at, src.source_system, src._row_hash, 
        current_timestamp(), current_timestamp()
    )
;

