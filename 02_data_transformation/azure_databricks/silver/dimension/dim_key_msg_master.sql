-- Databricks notebook source
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.dim_key_msg_master (
    dim_key_msg_sk          STRING    NOT NULL,
    key_message_id          STRING    NOT NULL,

    dim_prod_sk             STRING,
    product_id              STRING,

    message_title           STRING,
    message_text            STRING,
    message_type            STRING,

    therapeutic_area        STRING,
    indication              STRING,
    target_specialty        STRING,
    priority                STRING,
    sequence                INT,

    content_version         STRING,
    objection_handling      STRING,
    source_reference        STRING,

    approval_status         STRING,
    approved_date           DATE,

    effective_start_date    DATE,
    effective_end_date      DATE,

    source_updated_at       TIMESTAMP,
    source_system           STRING,

    _row_hash               STRING,
    _dim_created_ts         TIMESTAMP,
    _dim_updated_ts         TIMESTAMP
)
USING DELTA
CLUSTER BY (key_message_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed'  = 'true',
    'delta.enableDeletionVectors' = 'true'
);

-- COMMAND ----------

MERGE INTO onco_pharma_analytics.silver.dim_key_msg_master AS tgt
USING (
    WITH product AS (
        SELECT dim_prod_sk, product_id
        FROM onco_pharma_analytics.silver.dim_product_details
    ),

    joined AS (
        SELECT
            md5(cast(key_msg.key_message_id AS STRING)) AS dim_key_msg_sk,
            key_msg.key_message_id          AS key_message_id,

            product.dim_prod_sk             AS dim_prod_sk,
            key_msg.product_id              AS product_id,

            key_msg.message_title           AS message_title,
            key_msg.message_text            AS message_text,
            key_msg.message_type            AS message_type,

            key_msg.therapeutic_area        AS therapeutic_area,
            key_msg.indication              AS indication,
            key_msg.target_specialty        AS target_specialty,
            key_msg.priority                AS priority,
            key_msg.sequence                AS sequence,

            key_msg.content_version         AS content_version,
            key_msg.objection_handling      AS objection_handling,
            key_msg.source_reference        AS source_reference,

            key_msg.approval_status         AS approval_status,
            key_msg.approved_date           AS approved_date,

            key_msg.effective_start_date    AS effective_start_date,
            key_msg.effective_end_date      AS effective_end_date,

            key_msg.source_updated_at       AS source_updated_at,
            key_msg.source_system           AS source_system
        FROM onco_pharma_analytics.silver.stg_key_messages AS key_msg
        LEFT JOIN product ON key_msg.product_id = product.product_id
    ),

    s AS (
        SELECT
            *,
            sha2(to_json(struct(
                    dim_prod_sk, product_id,
                    message_title, message_text, message_type,
                    therapeutic_area, indication, target_specialty, priority, sequence,
                    content_version, objection_handling, source_reference,
                    approval_status, approved_date,
                    effective_start_date, effective_end_date,
                    source_updated_at, source_system
                )), 256) AS _row_hash
        FROM joined
    ),

    t AS (
        SELECT key_message_id, _row_hash
        FROM onco_pharma_analytics.silver.dim_key_msg_master
    )

    SELECT
        s.key_message_id                                         AS merge_key,
        'U'                                                      AS merge_action,
        s.*
    FROM s
    LEFT JOIN t
      ON s.key_message_id = t.key_message_id
    WHERE t.key_message_id IS NULL
       OR s._row_hash IS DISTINCT FROM t._row_hash
) AS src

ON tgt.key_message_id = src.merge_key

WHEN MATCHED AND src.merge_action = 'U'
             AND tgt._row_hash IS DISTINCT FROM src._row_hash
    THEN UPDATE SET
    tgt.dim_prod_sk          = src.dim_prod_sk,
    tgt.product_id           = src.product_id,
    tgt.message_title        = src.message_title,
    tgt.message_text         = src.message_text,
    tgt.message_type         = src.message_type,
    tgt.therapeutic_area     = src.therapeutic_area,
    tgt.indication           = src.indication,
    tgt.target_specialty     = src.target_specialty,
    tgt.priority             = src.priority,
    tgt.sequence             = src.sequence,
    tgt.content_version      = src.content_version,
    tgt.objection_handling   = src.objection_handling,
    tgt.source_reference     = src.source_reference,
    tgt.approval_status      = src.approval_status,
    tgt.approved_date        = src.approved_date,
    tgt.effective_start_date = src.effective_start_date,
    tgt.effective_end_date   = src.effective_end_date,
    tgt.source_updated_at    = src.source_updated_at,
    tgt.source_system        = src.source_system,
    tgt._row_hash            = src._row_hash,
    tgt._dim_updated_ts      = current_timestamp()

WHEN NOT MATCHED AND src.merge_action = 'U'
    THEN INSERT (
        dim_key_msg_sk, key_message_id,
        dim_prod_sk, product_id,
        message_title, message_text, message_type,
        therapeutic_area, indication, target_specialty, priority, sequence,
        content_version, objection_handling, source_reference,
        approval_status, approved_date,
        effective_start_date, effective_end_date,
        source_updated_at, source_system,
        _row_hash, _dim_created_ts, _dim_updated_ts
    )
    VALUES (
        src.dim_key_msg_sk, src.key_message_id,
        src.dim_prod_sk, src.product_id,
        src.message_title, src.message_text, src.message_type,
        src.therapeutic_area, src.indication, src.target_specialty, src.priority, src.sequence,
        src.content_version, src.objection_handling, src.source_reference,
        src.approval_status, src.approved_date,
        src.effective_start_date, src.effective_end_date,
        src.source_updated_at, src.source_system,
        src._row_hash, current_timestamp(), current_timestamp()
    )
;