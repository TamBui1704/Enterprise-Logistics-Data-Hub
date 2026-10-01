{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='SERVICE_ID'
) }}

SELECT
    SERVICE_ID,
    SERVICE_CODE,
    SERVICE_NAME,
    IS_INTERNATIONAL,
    CREATED_AT
FROM s3('http://minio:9000/logistics-lakehouse/silver/value_dim_services/*.parquet', 'minioadmin', 'minioadminpassword')
