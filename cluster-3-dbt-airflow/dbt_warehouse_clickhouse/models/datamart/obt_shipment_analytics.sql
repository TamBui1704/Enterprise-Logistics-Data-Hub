{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(BOOKING_DATE, BOOKING_ID)'
) }}

/*
  ClickHouse dbt Model: OLAP OBT Table
  Reads the clean Gold OBT dataset from S3 Gold Layer (created by dbt-spark in Delta format)
  and materializes into ClickHouse for BI & OLAP.
*/

SELECT
    BOOKING_ID,
    ITEM_CODE,
    BOOKING_DATE,
    WEIGHT_GRAM,
    MAIN_FEE,
    SUR_FEE,
    DISCOUNT_AMOUNT,
    TOTAL_REVENUE,
    COST_AMOUNT,
    PROFIT_AMOUNT,
    CREATED_AT,
    UPDATED_AT,
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CUSTOMER_TYPE,
    CUSTOMER_PROVINCE,
    CUSTOMER_REGION,
    SERVICE_ID,
    SERVICE_CODE,
    SERVICE_NAME,
    IS_INTERNATIONAL,
    SENDING_POS_CODE,
    SENDING_POS_NAME,
    SENDING_PROVINCE,
    SENDING_REGION,
    RECEIVING_POS_CODE,
    RECEIVING_POS_NAME,
    RECEIVING_PROVINCE,
    RECEIVING_REGION,
    STATUS_ID,
    STATUS_CODE,
    STATUS_NAME,
    STATUS_GROUP,
    ingested_at,
    now() AS olap_loaded_at
FROM deltaLake(
    'http://minio:9000/logistics-lakehouse/gold/datamart/obt_shipment_analytics/',
    'minioadmin',
    'minioadmin'
)
