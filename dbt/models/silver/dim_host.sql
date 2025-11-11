{{ 
    config(
    materialized='table',
    alias='dim_host',
    unique_key='host_id') 
}}

select distinct
  host_id,
  host_name,
  host_since::date as host_since,
  host_is_superhost,
  host_neighbourhood,
  scraped_date      -- keep for timestamp snapshots
from {{ ref('airbnb_cleaned') }}