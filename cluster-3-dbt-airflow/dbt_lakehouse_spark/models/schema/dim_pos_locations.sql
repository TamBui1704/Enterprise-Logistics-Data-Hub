{{ config(
    materialized='table',
    file_format='delta',
    location_root='s3a://logistics-lakehouse/gold/schema'
) }}

SELECT
    POS_CODE,
    POS_NAME,
    PROVINCE_CODE,
    PROVINCE_NAME,
    REGION,
    POS_LEVEL,
    CREATED_AT,
    ingested_at
FROM delta.`s3a://logistics-lakehouse/silver/value_dim_pos_locations`

