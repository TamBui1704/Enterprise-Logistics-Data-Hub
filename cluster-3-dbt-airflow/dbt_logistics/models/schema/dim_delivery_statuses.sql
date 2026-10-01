{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='STATUS_ID'
) }}

SELECT
    STATUS_ID,
    STATUS_CODE,
    STATUS_NAME,
    STATUS_GROUP
FROM s3('http://minio:9000/logistics-lakehouse/silver/value_dim_delivery_statuses/*.parquet', 'minioadmin', 'minioadminpassword')
