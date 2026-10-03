{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='BOOKING_ID',
    partition_by=['BOOKING_DATE']
) }}

/*
  Spark dbt Model: Gold Layer Clean OBT Table (One Big Table)
  Pre-joins fact_shipment_bookings with dim_customers, dim_services,
  dim_pos_locations and dim_delivery_statuses from Gold S3 Schema Layer.
  Saved incrementally as Delta Lake at s3a://logistics-lakehouse/gold/datamart/obt_shipment_analytics.
*/

SELECT
    f.BOOKING_ID,
    f.ITEM_CODE,
    CAST(f.BOOKING_DATE AS DATE) AS BOOKING_DATE,
    f.WEIGHT_GRAM,
    f.MAIN_FEE,
    f.SUR_FEE,
    f.DISCOUNT_AMOUNT,
    f.TOTAL_REVENUE,
    f.COST_AMOUNT,
    (f.TOTAL_REVENUE - f.COST_AMOUNT) AS PROFIT_AMOUNT,
    f.CREATED_AT,
    f.UPDATED_AT,

    -- Customer Details
    c.CUSTOMER_ID,
    c.CUSTOMER_NAME,
    c.CUSTOMER_TYPE,
    c.PROVINCE AS CUSTOMER_PROVINCE,
    c.REGION AS CUSTOMER_REGION,

    -- Service Details
    s.SERVICE_ID,
    s.SERVICE_CODE,
    s.SERVICE_NAME,
    s.IS_INTERNATIONAL,

    -- Sending POS Details
    sp.POS_CODE AS SENDING_POS_CODE,
    sp.POS_NAME AS SENDING_POS_NAME,
    sp.PROVINCE_NAME AS SENDING_PROVINCE,
    sp.REGION AS SENDING_REGION,

    -- Receiving POS Details
    rp.POS_CODE AS RECEIVING_POS_CODE,
    rp.POS_NAME AS RECEIVING_POS_NAME,
    rp.PROVINCE_NAME AS RECEIVING_PROVINCE,
    rp.REGION AS RECEIVING_REGION,

    -- Delivery Status Details
    st.STATUS_ID,
    st.STATUS_CODE,
    st.STATUS_NAME,
    st.STATUS_GROUP,

    f.ingested_at

FROM {{ ref('fact_shipment_bookings') }} f
LEFT JOIN {{ ref('dim_customers') }} c ON f.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN {{ ref('dim_services') }} s ON f.SERVICE_ID = s.SERVICE_ID
LEFT JOIN {{ ref('dim_pos_locations') }} sp ON f.SENDING_POS_CODE = sp.POS_CODE
LEFT JOIN {{ ref('dim_pos_locations') }} rp ON f.RECEIVING_POS_CODE = rp.POS_CODE
LEFT JOIN {{ ref('dim_delivery_statuses') }} st ON f.STATUS_ID = st.STATUS_ID

{% if is_incremental() %}
  {% set max_obt_ingested_at = "(SELECT COALESCE(MAX(ingested_at), CAST('1900-01-01 00:00:00' AS TIMESTAMP)) FROM " ~ this ~ ")" %}

  WHERE f.ingested_at > {{ max_obt_ingested_at }}
     OR f.CUSTOMER_ID IN (
         SELECT CUSTOMER_ID FROM {{ ref('dim_customers') }} WHERE ingested_at > {{ max_obt_ingested_at }}
     )
     OR f.SERVICE_ID IN (
         SELECT SERVICE_ID FROM {{ ref('dim_services') }} WHERE ingested_at > {{ max_obt_ingested_at }}
     )
     OR f.SENDING_POS_CODE IN (
         SELECT POS_CODE FROM {{ ref('dim_pos_locations') }} WHERE ingested_at > {{ max_obt_ingested_at }}
     )
     OR f.RECEIVING_POS_CODE IN (
         SELECT POS_CODE FROM {{ ref('dim_pos_locations') }} WHERE ingested_at > {{ max_obt_ingested_at }}
     )
     OR f.STATUS_ID IN (
         SELECT STATUS_ID FROM {{ ref('dim_delivery_statuses') }} WHERE ingested_at > {{ max_obt_ingested_at }}
     )
{% endif %}


