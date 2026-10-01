{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='CUSTOMER_ID'
) }}

SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CUSTOMER_TYPE,
    TAX_CODE,
    PROVINCE,
    REGION,
    CREATED_AT
FROM s3('http://minio:9000/logistics-lakehouse/silver/value_dim_customers/*.parquet', 'minioadmin', 'minioadminpassword')
