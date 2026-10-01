{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='POS_CODE'
) }}

SELECT
    POS_CODE,
    POS_NAME,
    PROVINCE_CODE,
    PROVINCE_NAME,
    REGION,
    POS_LEVEL,
    CREATED_AT
FROM s3('http://minio:9000/logistics-lakehouse/silver/value_dim_pos_locations/*.parquet', 'minioadmin', 'minioadminpassword')
