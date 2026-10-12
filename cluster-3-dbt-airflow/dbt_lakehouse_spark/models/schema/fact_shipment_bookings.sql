{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='BOOKING_ID',
    partition_by=['BOOKING_DATE']
) }}

/*
  Spark dbt Fact Model: Shipment Bookings in Gold Layer S3
  Source: Silver S3 Delta Layer (value_shipment_bookings)
*/

SELECT
    BOOKING_ID,
    ITEM_CODE,
    CAST(BOOKING_DATE AS DATE) AS BOOKING_DATE,
    CUSTOMER_ID,
    SERVICE_ID,
    SENDING_POS_CODE,
    RECEIVING_POS_CODE,
    CAST(WEIGHT_GRAM AS LONG) AS WEIGHT_GRAM,
    WEIGHT_TIER_ID,
    ROUTING_TYPE_ID,
    CAST(MAIN_FEE AS DOUBLE) AS MAIN_FEE,
    CAST(SUR_FEE AS DOUBLE) AS SUR_FEE,
    CAST(DISCOUNT_AMOUNT AS DOUBLE) AS DISCOUNT_AMOUNT,
    CAST(TOTAL_REVENUE AS DOUBLE) AS TOTAL_REVENUE,
    CAST(COST_AMOUNT AS DOUBLE) AS COST_AMOUNT,
    STATUS_ID,
    CREATED_AT,
    UPDATED_AT,
    ingested_at
FROM delta.`s3a://logistics-lakehouse/silver/value_shipment_bookings`

{% if is_incremental() %}
  WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), CAST('1900-01-01 00:00:00' AS TIMESTAMP)) FROM {{ this }})
{% endif %}

