-- Databricks notebook source
CREATE TABLE IF NOT EXISTS onco_pharma_analytics.silver.dim_product_details (
    dim_prod_sk               STRING    NOT NULL,
    product_id                STRING    NOT NULL,

    dim_sku_sk                STRING,

    dim_brand_sk              STRING,
    brand_id                  STRING,

    dim_generic_sk            STRING,
    generic_id                STRING,

    product_name              STRING,
    ndc_code                  STRING,

    strength                  DECIMAL(10,2),
    strength_unit             STRING,

    dosage_form               STRING,
    route_of_administration   STRING,

    pack_type                 STRING,
    pack_size                 INT,

    launch_date               DATE,
    status                    STRING,

    source_updated_at         TIMESTAMP,
    source_system             STRING,

    _row_hash                 STRING,
    _dim_created_ts           TIMESTAMP,
    _dim_updated_ts           TIMESTAMP
)
USING DELTA
CLUSTER BY (product_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed'  = 'true',
    'delta.enableDeletionVectors' = 'true'
);

-- COMMAND ----------

MERGE INTO onco_pharma_analytics.silver.dim_product_details AS tgt
USING (
    WITH sku AS (
        SELECT dim_sku_sk, ndc_code, strength, strength_unit, pack_type, dosage_form
        FROM onco_pharma_analytics.silver.dim_sku_details
        WHERE ndc_code IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ndc_code ORDER BY source_updated_at DESC NULLS LAST) = 1
    ),

    generic AS (
        SELECT dim_generic_sk, generic_id
        FROM onco_pharma_analytics.silver.dim_generic_details
    ),

    brand AS (
        SELECT dim_brand_sk, brand_id
        FROM onco_pharma_analytics.silver.dim_brand_details
    ),

    joined AS (
        SELECT
            md5(cast(product.product_id AS STRING))             AS dim_prod_sk,
            product.product_id                                  AS product_id,

            sku.dim_sku_sk                                      AS dim_sku_sk,

            brand.dim_brand_sk                                  AS dim_brand_sk,
            product.brand_id                                    AS brand_id,

            generic.dim_generic_sk                              AS dim_generic_sk,
            product.generic_id                                  AS generic_id,

            product.product_name                                AS product_name,
            sku.ndc_code                                        AS ndc_code,

            coalesce(product.strength, sku.strength)            AS strength,
            coalesce(product.strength_unit, sku.strength_unit)  AS strength_unit,

            coalesce(product.dosage_form, sku.dosage_form)      AS dosage_form,

            product.route_of_administration                     AS route_of_administration,

            coalesce(product.pack_type, sku.pack_type)          AS pack_type,

            product.pack_size                                   AS pack_size,

            product.launch_date                                 AS launch_date,
            product.status                                      AS status,

            product.source_updated_at                           AS source_updated_at,
            product.source_system                               AS source_system
        FROM onco_pharma_analytics.silver.stg_product_details AS product
        LEFT JOIN brand   ON product.brand_id   = brand.brand_id
        LEFT JOIN generic ON product.generic_id = generic.generic_id
        LEFT JOIN sku     ON product.ndc_code   = sku.ndc_code
        WHERE product.source_updated_at >= (
            SELECT coalesce(max(source_updated_at) - INTERVAL 1 DAY,
                            TIMESTAMP '1900-01-01 00:00:00')
            FROM onco_pharma_analytics.silver.dim_product_details
        )
    ),

    s AS (
        SELECT
            *,
            sha2(to_json(struct(
                    dim_sku_sk,
                    dim_brand_sk, brand_id,
                    dim_generic_sk, generic_id,
                    product_name, ndc_code,
                    strength, strength_unit,
                    dosage_form,
                    route_of_administration,
                    pack_type,
                    pack_size,
                    launch_date, status,
                    source_updated_at, source_system
                )), 256) AS _row_hash
        FROM joined
    ),

    t AS (
        SELECT product_id, _row_hash
        FROM onco_pharma_analytics.silver.dim_product_details
    )

    SELECT
        s.product_id                                             AS merge_key,
        'U'                                                      AS merge_action,
        s.*
    FROM s
    LEFT JOIN t
      ON s.product_id = t.product_id
    WHERE t.product_id IS NULL
       OR s._row_hash IS DISTINCT FROM t._row_hash
) AS src

ON tgt.product_id = src.merge_key

WHEN MATCHED AND src.merge_action = 'U'
             AND tgt._row_hash IS DISTINCT FROM src._row_hash
    THEN UPDATE SET
    tgt.dim_sku_sk              = src.dim_sku_sk,
    tgt.dim_brand_sk            = src.dim_brand_sk,
    tgt.brand_id                = src.brand_id,
    tgt.dim_generic_sk          = src.dim_generic_sk,
    tgt.generic_id              = src.generic_id,
    tgt.product_name            = src.product_name,
    tgt.ndc_code                = src.ndc_code,
    tgt.strength                = src.strength,
    tgt.strength_unit           = src.strength_unit,
    tgt.dosage_form             = src.dosage_form,
    tgt.route_of_administration = src.route_of_administration,
    tgt.pack_type               = src.pack_type,
    tgt.pack_size               = src.pack_size,
    tgt.launch_date             = src.launch_date,
    tgt.status                  = src.status,
    tgt.source_updated_at       = src.source_updated_at,
    tgt.source_system           = src.source_system,
    tgt._row_hash               = src._row_hash,
    tgt._dim_updated_ts         = current_timestamp()

WHEN NOT MATCHED AND src.merge_action = 'U'
    THEN INSERT (
        dim_prod_sk, product_id,
        dim_sku_sk,
        dim_brand_sk, brand_id,
        dim_generic_sk, generic_id,
        product_name, ndc_code,
        strength, strength_unit,
        dosage_form,
        route_of_administration,
        pack_type,
        pack_size,
        launch_date, status,
        source_updated_at, source_system,
        _row_hash, _dim_created_ts, _dim_updated_ts
    )
    VALUES (
        src.dim_prod_sk, src.product_id,
        src.dim_sku_sk,
        src.dim_brand_sk, src.brand_id,
        src.dim_generic_sk, src.generic_id,
        src.product_name, src.ndc_code,
        src.strength, src.strength_unit,
        src.dosage_form,
        src.route_of_administration,
        src.pack_type,
        src.pack_size,
        src.launch_date, src.status,
        src.source_updated_at, src.source_system,
        src._row_hash, current_timestamp(), current_timestamp()
    )
;