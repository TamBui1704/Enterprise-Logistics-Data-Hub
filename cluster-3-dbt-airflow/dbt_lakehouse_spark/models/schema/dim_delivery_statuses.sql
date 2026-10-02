{{ config(
    materialized='table',
    file_format='delta',
    location_root='s3a://logistics-lakehouse/gold/schema'
) }}

SELECT
    STATUS_ID,
    STATUS_CODE,
    STATUS_NAME,
    STATUS_GROUP
FROM delta.`s3a://logistics-lakehouse/silver/value_dim_delivery_statuses`
