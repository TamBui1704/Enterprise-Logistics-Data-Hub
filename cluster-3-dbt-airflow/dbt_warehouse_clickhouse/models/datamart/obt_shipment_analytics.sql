{{ config(
    materialized='view'
) }}

/*
  ClickHouse dbt Model: Wrapper View for OBT Shipment Analytics
  This view transparently applies the 'FINAL' keyword to the base incremental table.
  Data Analysts can query this view without worrying about deduplication, 
  ensuring 100% accurate aggregations out-of-the-box.
*/

SELECT *
FROM {{ ref('obt_shipment_analytics_base') }}
FINAL
