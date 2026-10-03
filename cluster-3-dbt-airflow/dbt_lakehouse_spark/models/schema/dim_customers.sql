/*
  Spark dbt Model: Customer Dimension Table in Gold Layer S3
  Source: Silver S3 value_dim_customers
*/

SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CUSTOMER_TYPE,
    TAX_CODE,
    PROVINCE,
    REGION,
    CREATED_AT,
    ingested_at
FROM delta.`s3a://logistics-lakehouse/silver/value_dim_customers`

