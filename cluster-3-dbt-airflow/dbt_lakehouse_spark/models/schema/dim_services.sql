{{ config(
    materialized='table',
    file_format='delta',
    location_root='s3a://logistics-lakehouse/gold/schema'
) }}

SELECT
    SERVICE_ID,
    SERVICE_CODE,
    SERVICE_NAME,
    IS_INTERNATIONAL,
    CREATED_AT
FROM delta.`s3a://logistics-lakehouse/silver/value_dim_services`
