{{
config(
materialized='incermental',
unique_key='customer_id',
incremental_strategy='merge'
)
}}
SELECT
customer_id,
name,
from AIRBNB.RAW.customers

{% if is_incremental() %}
  -- Only select new or updated rows since the last run
  WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
 
